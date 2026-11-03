//! TeamSpeak side: keeps the connection alive, forwards chat commands to the bot,
//! sends audio and replies.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use tsclientlib::events::Event;
use tsclientlib::prelude::*;
use tsclientlib::{
    AudioEvent, ChannelId, ClientId, ClientType, Connection, DisconnectOptions, Identity, MessageTarget,
    StreamItem,
};
use tsproto_packets::packets::{AudioData, CodecType, OutAudio};
use tsproto_types::crypto::EccKeyPrivP256;

use crate::config::Config;
use crate::fleet::Fleet;
use crate::player::AudioOut;

#[derive(Debug, Clone, Copy)]
pub enum Reply {
    /// The channel the bot is in. TeamSpeak has no way to post to any other channel.
    Channel,
    Client(ClientId),
}

/// A command from someone in another channel, held until the bot has moved there.
struct Waiting {
    handle: u16,
    target: ChannelId,
    reply: Reply,
    uid: String,
    name: String,
    text: String,
}

pub enum ConnCmd {
    Text(Reply, String),
    Description(String),
}

pub enum BotEvent {
    Command { reply: Reply, uid: String, name: String, text: String },
    /// Number of people in the bot's channel who can hear it.
    Listeners(usize),
}

/// Dev tool: joins `channel` as an ordinary client, sends `commands` to the main bot in
/// private, 3 s apart, and prints what it hears until `secs` have passed.
pub async fn test_client(cfg: &Config, channel: u64, nick: &str, secs: u64, commands: Vec<String>) -> Result<()> {
    let mut opts = Connection::build(cfg.ts.address.clone())
        .name(nick.to_string())
        .identity(Identity::create())
        .channel_id(ChannelId(channel));
    if !cfg.ts.password.is_empty() {
        opts = opts.password(cfg.ts.password.clone());
    }
    let mut con = opts.connect()?;
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    let mut next = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut commands = commands.into_iter();
    let mut connected = false;
    loop {
        enum Step {
            Item(Option<Result<StreamItem, tsclientlib::Error>>),
            Send,
            Done,
        }
        let step = {
            let mut events = con.events();
            tokio::select! {
                e = events.next() => Step::Item(e),
                _ = tokio::time::sleep_until(next), if connected => Step::Send,
                _ = tokio::time::sleep_until(end) => Step::Done,
            }
        };
        match step {
            Step::Item(None) => bail!("connection closed"),
            Step::Item(Some(item)) => {
                if let StreamItem::BookEvents(events) = item? {
                    if !connected {
                        con.get_state()?.server.set_subscribed(true).send(&mut con)?;
                    }
                    connected = true;
                    for e in events {
                        if let Event::Message { target, invoker, message } = e {
                            println!("[{target:?}] {}: {message}", invoker.name);
                        }
                    }
                }
            }
            Step::Send => {
                next += Duration::from_secs(3);
                let Some(text) = commands.next() else { continue };
                let state = con.get_state()?;
                let bot = state.clients.values().find(|c| c.name == cfg.ts.nickname).map(|c| c.id);
                let bot = bot.context("main bot not found")?;
                println!("> {text}");
                state.send_message(MessageTarget::Client(bot), &text).send(&mut con)?;
            }
            Step::Done => break,
        }
    }
    con.disconnect(DisconnectOptions::new())?;
    let drain = con.events().for_each(|_| async {});
    let _ = tokio::time::timeout(Duration::from_secs(2), drain).await;
    Ok(())
}

/// What a connection is for: the main bot (index 0) or a temporary one (index 2 and up).
pub struct Role {
    pub index: usize,
    pub fleet: Fleet,
    pub channel: ChannelId,
    pub channel_password: Option<String>,
    /// Who asked for a temporary bot, to report failures to.
    pub requester: Option<ClientId>,
    /// Set by the bot while it has something playing or queued.
    pub busy: Arc<AtomicBool>,
}

impl Role {
    fn is_main(&self) -> bool { self.index == 0 }
}

/// Commands that add music, and so justify starting a temporary bot.
fn adds_music(cmd: &str) -> bool {
    matches!(cmd, "p" | "play" | "点歌" | "播放" | "pn" | "插播" | "b" | "bili" | "b站" | "pl" | "playlist" | "歌单" | "播放歌单")
}

pub async fn run(
    cfg: Arc<Config>,
    role: Role,
    mut audio_rx: mpsc::Receiver<AudioOut>,
    mut cmd_rx: mpsc::Receiver<ConnCmd>,
    bot_tx: mpsc::Sender<BotEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut backoff = Duration::from_secs(2);
    let mut arrived = false;
    let mut attempts = 0;
    loop {
        let started = tokio::time::Instant::now();
        match session(&cfg, &role, &mut arrived, &mut audio_rx, &mut cmd_rx, &bot_tx, &mut shutdown).await {
            Ok(()) => return Ok(()),
            Err(e) if !role.is_main() => {
                // Connecting can hit the server's flood limit for a moment: wait it out.
                if !arrived && attempts < 3 {
                    attempts += 1;
                    warn!("bot {}: {e:#}, retrying", role.index);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(20)) => continue,
                        _ = shutdown.changed() => return Ok(()),
                    }
                }
                // After that temporary bots don't reconnect: nothing there is worth keeping.
                if let (false, Some(who)) = (arrived, role.requester) {
                    role.fleet.notify(who, format!("[color=#c0392b]没能叫来新的点歌机：{e}[/color]")).await;
                }
                return Err(e);
            }
            Err(e) => warn!("TeamSpeak connection lost: {e:#}"),
        }
        let _ = bot_tx.send(BotEvent::Listeners(0)).await;
        if started.elapsed() > Duration::from_secs(60) {
            backoff = Duration::from_secs(2);
        }
        info!("reconnecting in {}s", backoff.as_secs());
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => return Ok(()),
        }
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

enum Act {
    Event(Option<Result<StreamItem, tsclientlib::Error>>),
    Audio(Option<AudioOut>),
    Cmd(Option<ConnCmd>),
    Tick,
    Quit,
}

#[allow(clippy::too_many_arguments)]
async fn session(
    cfg: &Config,
    role: &Role,
    arrived: &mut bool,
    audio_rx: &mut mpsc::Receiver<AudioOut>,
    cmd_rx: &mut mpsc::Receiver<ConnCmd>,
    bot_tx: &mpsc::Sender<BotEvent>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let identity = load_identity(cfg, role.index)?;
    let name = if role.is_main() { cfg.ts.nickname.clone() } else { format!("{} {}", cfg.ts.nickname, role.index) };
    let mut opts = Connection::build(cfg.ts.address.clone())
        .name(name)
        .identity(identity)
        .log_commands(false);
    if !cfg.ts.password.is_empty() {
        opts = opts.password(cfg.ts.password.clone());
    }
    if role.channel.0 > 0 {
        opts = opts.channel_id(role.channel);
        if let Some(pw) = role.channel_password.as_ref().filter(|p| !p.is_empty()) {
            opts = opts.channel_password(pw.clone());
        }
    }
    let mut con = opts.connect()?;
    let mut connected = false;
    let mut listeners = usize::MAX;
    let mut voice_active = false;
    // Channel moves: where the bot lives, where it is now, who asked for a pending move.
    let mut home = role.channel;
    let mut here: Option<ChannelId> = None;
    let mut pending_moves: HashMap<u16, Reply> = HashMap::new();
    let mut waiting: Vec<Waiting> = Vec::new();
    let mut alone_since: Option<Instant> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(15));
    let mut leaving = false;

    loop {
        if leaving {
            info!("disconnecting");
            con.disconnect(DisconnectOptions::new().message("下次见"))?;
            let drain = con.events().for_each(|_| async {});
            let _ = tokio::time::timeout(Duration::from_secs(3), drain).await;
            return Ok(());
        }
        let act = {
            let mut events = con.events();
            tokio::select! {
                biased;
                a = audio_rx.recv() => Act::Audio(a),
                e = events.next() => Act::Event(e),
                c = cmd_rx.recv() => Act::Cmd(c),
                _ = tick.tick() => Act::Tick,
                _ = shutdown.changed() => Act::Quit,
            }
        };
        match act {
            Act::Audio(Some(AudioOut::Opus(data))) => {
                if connected {
                    let packet = OutAudio::new(&AudioData::C2S { id: 0, codec: CodecType::OpusMusic, data: &data });
                    con.send_audio(packet)?;
                    voice_active = true;
                }
            }
            Act::Audio(Some(AudioOut::End)) => {
                if connected && voice_active {
                    let packet = OutAudio::new(&AudioData::C2S { id: 0, codec: CodecType::OpusMusic, data: &[] });
                    con.send_audio(packet)?;
                }
                voice_active = false;
            }
            Act::Audio(None) | Act::Cmd(None) | Act::Quit => leaving = true,
            Act::Cmd(Some(ConnCmd::Text(reply, text))) => {
                if connected {
                    send_text(&mut con, reply, &text)?;
                }
            }
            Act::Cmd(Some(ConnCmd::Description(text))) => {
                if connected {
                    let state = con.get_state()?;
                    if let Some(me) = state.clients.get(&state.own_client) {
                        me.edit().set_description(&text).send(&mut con)?;
                    }
                }
            }
            Act::Tick if !role.is_main() => {
                // A temporary bot leaves once nobody is there or nothing is playing.
                if !connected {
                    continue;
                }
                if count_clients(cfg, &con, false) > 0 && role.busy.load(Ordering::Relaxed) {
                    alone_since = None;
                } else {
                    let since = *alone_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= Duration::from_secs(cfg.bot.temp_idle_secs) {
                        send_text(&mut con, Reply::Channel, "这里没事了，我先走了。需要时再叫我")?;
                        leaving = true;
                    }
                }
            }
            Act::Tick => {
                // Summoned elsewhere and everyone left: go back to the music channel.
                let Some(cur) = here.filter(|c| connected && *c != home) else {
                    alone_since = None;
                    continue;
                };
                if count_clients(cfg, &con, false) > 0 || role.fleet.temp_in(home) {
                    alone_since = None;
                } else if cfg.ts.return_home_secs > 0 {
                    let since = *alone_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= Duration::from_secs(cfg.ts.return_home_secs) {
                        info!("alone in channel {} for a while, going home", cur.0);
                        alone_since = None;
                        move_to(&mut con, home, None)?;
                    }
                }
            }
            Act::Event(None) => bail!("connection closed"),
            Act::Event(Some(Err(e))) => return Err(e.into()),
            Act::Event(Some(Ok(item))) => match item {
                StreamItem::BookEvents(events) => {
                    if !connected {
                        connected = true;
                        let state = con.get_state()?;
                        info!("bot {}: connected to {} as client {}", role.index, state.server.name, state.own_client.0);
                        if role.is_main() {
                            save_identity(cfg, &con);
                            // Without this the server hides who is in other channels, and the
                            // bot can't tell where a command came from.
                            con.get_state()?.server.set_subscribed(true).send(&mut con)?;
                        }
                    }
                    let (own, own_channel) = {
                        let state = con.get_state()?;
                        let ch = state.clients.get(&state.own_client).map(|c| c.channel);
                        (state.own_client, ch)
                    };
                    if let Some(ch) = own_channel {
                        if home.0 == 0 {
                            home = ch;
                        }
                        if !role.is_main() && here.is_none() {
                            if ch != role.channel {
                                // Password or full: the server dropped it in the default channel.
                                if let Some(who) = role.requester {
                                    role.fleet.notify(who, "[color=#c0392b]新的点歌机进不去你的频道（有密码请用 !来 密码）[/color]".into()).await;
                                }
                                leaving = true;
                                continue;
                            }
                            announce_arrival(&mut con, false, true)?;
                        } else if here.is_some_and(|h| h != ch) {
                            announce_arrival(&mut con, ch == home, !role.is_main())?;
                            alone_since = None;
                        }
                        *arrived = true;
                        here = Some(ch);
                        if !role.is_main() {
                            role.fleet.set_channel(role.index, ch);
                        }
                        // Arrived: answer those commands in this channel.
                        let (ready, rest) = std::mem::take(&mut waiting).into_iter().partition(|w| w.target == ch);
                        waiting = rest;
                        for w in ready {
                            let ev = BotEvent::Command { reply: Reply::Channel, uid: w.uid, name: w.name, text: w.text };
                            bot_tx.send(ev).await.map_err(|_| anyhow!("bot stopped"))?;
                        }
                    }
                    for e in events {
                        if let Event::Message { target, invoker, message } = e {
                            if invoker.id == own {
                                continue;
                            }
                            // Every bot sees server chat; only the main one acts on it.
                            if matches!(target, MessageTarget::Server) && !role.is_main() {
                                continue;
                            }
                            // Server chat is answered privately: the bot group may not post there.
                            let reply = match target {
                                MessageTarget::Channel => Reply::Channel,
                                MessageTarget::Client(_) | MessageTarget::Server => Reply::Client(invoker.id),
                                _ => continue,
                            };
                            let text = message.trim().to_string();
                            if !(text.starts_with('!') || text.starts_with('！')) {
                                continue;
                            }
                            let body = text.trim_start_matches(['!', '！']).trim();
                            let (cmd, arg) = body.split_once(char::is_whitespace).unwrap_or((body, ""));
                            let cmd = cmd.to_lowercase();
                            match cmd.as_str() {
                                "来" | "过来" | "come" | "summon" => {
                                    let target = con.get_state()?.clients.get(&invoker.id).map(|c| c.channel);
                                    match target {
                                        Some(t) if Some(t) == here => send_text(&mut con, reply, "我就在你这个频道呀")?,
                                        Some(t) if role.is_main() && role.fleet.temp_in(t) => {
                                            send_text(&mut con, reply, "你的频道已经有点歌机了")?;
                                        }
                                        Some(t) if count_clients(cfg, &con, false) == 0 => {
                                            let pw = Some(arg.trim()).filter(|p| !p.is_empty());
                                            if let Some(h) = move_to(&mut con, t, pw)? {
                                                pending_moves.insert(h, reply);
                                            }
                                        }
                                        Some(t) if role.is_main() => {
                                            let pw = Some(arg.trim().to_string()).filter(|p| !p.is_empty());
                                            if !role.fleet.spawn(t, pw, invoker.id, None) {
                                                send_text(&mut con, reply, &format!("点歌机都在别的频道忙着（最多 {} 个）", cfg.bot.max_bots))?;
                                            }
                                        }
                                        Some(_) => send_text(&mut con, reply, "这边还有人在听，走不开")?,
                                        None => {}
                                    }
                                }
                                "回去" | "回家" | "home" | "离开" | "leave" if !role.is_main() => {
                                    send_text(&mut con, Reply::Channel, "好的，我先走了")?;
                                    leaving = true;
                                }
                                "回去" | "回家" | "home" | "离开" | "leave" => {
                                    if here == Some(home) {
                                        send_text(&mut con, reply, "我已经在音乐频道了")?;
                                    } else if let Some(h) = move_to(&mut con, home, None)? {
                                        pending_moves.insert(h, reply);
                                    }
                                }
                                _ => {
                                    let uid = invoker.uid.map(|u| u.as_ref().to_string()).unwrap_or_else(|| invoker.name.clone());
                                    // Channel replies only reach the bot's own channel, so a bot has to
                                    // be with the asker. Use the temporary bot there, else move if that
                                    // takes the music from nobody, else start one for new music.
                                    let asker = con.get_state()?.clients.get(&invoker.id).map(|c| c.channel);
                                    let elsewhere = asker.filter(|t| {
                                        role.is_main() && !matches!(reply, Reply::Channel) && Some(*t) != here
                                    });
                                    if let Some(t) = elsewhere {
                                        if role.fleet.temp_in(t) {
                                            let ev = BotEvent::Command { reply: Reply::Channel, uid, name: invoker.name, text };
                                            role.fleet.forward(t, ev).await;
                                            continue;
                                        }
                                        if can_talk_in(&con, t) {
                                            if count_clients(cfg, &con, false) == 0 {
                                                let handle = match waiting.iter().find(|w| w.target == t) {
                                                    Some(w) => Some(w.handle),
                                                    None => move_to(&mut con, t, None)?,
                                                };
                                                if let Some(handle) = handle {
                                                    waiting.push(Waiting { handle, target: t, reply, uid, name: invoker.name, text });
                                                    continue;
                                                }
                                            } else if adds_music(&cmd) {
                                                if role.fleet.has_room() {
                                                    let ev = BotEvent::Command { reply: Reply::Channel, uid, name: invoker.name, text };
                                                    role.fleet.spawn(t, None, invoker.id, Some(ev));
                                                    continue;
                                                }
                                                send_text(&mut con, reply, "点歌机都在别的频道忙着，这首先排进音乐频道的队列")?;
                                            }
                                        }
                                    }
                                    let ev = BotEvent::Command { reply, uid, name: invoker.name, text };
                                    bot_tx.send(ev).await.map_err(|_| anyhow!("bot stopped"))?;
                                }
                            }
                        }
                    }
                    let n = count_clients(cfg, &con, true);
                    if n != listeners {
                        listeners = n;
                        bot_tx.send(BotEvent::Listeners(n)).await.map_err(|_| anyhow!("bot stopped"))?;
                    }
                    // Commands that started this bot, now that it knows who's listening.
                    if !role.is_main() && here.is_some() {
                        for ev in role.fleet.take_early(role.index) {
                            bot_tx.send(ev).await.map_err(|_| anyhow!("bot stopped"))?;
                        }
                    }
                }
                StreamItem::IdentityLevelIncreasing(level) => info!("raising identity level to {level}"),
                StreamItem::IdentityLevelIncreased if role.is_main() => save_identity(cfg, &con),
                StreamItem::DisconnectedTemporarily(reason) => {
                    warn!("temporarily disconnected: {reason:?}");
                    connected = false;
                    listeners = 0;
                    bot_tx.send(BotEvent::Listeners(0)).await.ok();
                }
                StreamItem::MessageResult(handle, Err(e)) => {
                    // Couldn't move (password, full...): run the command from here after all.
                    let (failed, rest) = std::mem::take(&mut waiting).into_iter().partition(|w| w.handle == handle.0);
                    waiting = rest;
                    for w in failed {
                        let ev = BotEvent::Command { reply: w.reply, uid: w.uid, name: w.name, text: w.text };
                        bot_tx.send(ev).await.map_err(|_| anyhow!("bot stopped"))?;
                    }
                    if let Some(reply) = pending_moves.remove(&handle.0) {
                        let why = match e.missing_permission {
                            Some(p) => format!("缺少权限 {p:?}"),
                            None => e.error.to_string(),
                        };
                        send_text(&mut con, reply, &format!("[color=#c0392b]过不去：{why}（有密码的频道请用 !来 密码）[/color]"))?;
                    }
                }
                StreamItem::MessageResult(handle, Ok(())) => {
                    pending_moves.remove(&handle.0);
                }
                StreamItem::AudioChange(AudioEvent::CanSendAudio(false)) => {
                    warn!("server says the bot cannot talk here (talk power / muted)");
                }
                _ => {}
            },
        }
    }
}

/// People in the bot's channel, not counting other bots; with `hearing`, only those whose
/// speakers are on.
fn count_clients(cfg: &Config, con: &Connection, hearing: bool) -> usize {
    let Ok(state) = con.get_state() else { return 0 };
    let Some(me) = state.clients.get(&state.own_client) else { return 0 };
    state
        .clients
        .values()
        .filter(|c| {
            c.id != me.id
                && c.channel == me.channel
                && c.client_type == ClientType::Normal
                && !c.name.starts_with(&cfg.ts.nickname)
                && (!hearing || (!c.output_muted && c.output_hardware_enabled))
        })
        .count()
}

/// Whether the bot may talk in `channel` (the AFK channel needs talk power, for one).
fn can_talk_in(con: &Connection, channel: ChannelId) -> bool {
    let Ok(state) = con.get_state() else { return false };
    let Some(me) = state.clients.get(&state.own_client) else { return false };
    let needed = state.channels.get(&channel).and_then(|c| c.needed_talk_power).unwrap_or(0);
    me.talk_power >= needed
}

/// Asks the server to move the bot; returns the handle whose result reports failures.
fn move_to(con: &mut Connection, channel: ChannelId, password: Option<&str>) -> Result<Option<u16>> {
    let part = {
        let state = con.get_state()?;
        let Some(me) = state.clients.get(&state.own_client) else { return Ok(None) };
        me.client_move(channel)
    };
    let part = match password {
        Some(pw) => part.set_password(pw),
        None => part,
    };
    Ok(Some(part.send_with_result(con)?.0))
}

fn announce_arrival(con: &mut Connection, home: bool, temporary: bool) -> Result<()> {
    let mut msg = if temporary {
        "🎵 点歌机来啦！发送 [b]!点歌 歌名[/b] 点歌，[b]!帮助[/b] 查看全部命令，[b]!离开[/b] 让我走（这里空闲一会儿我也会自己走）".to_string()
    } else if home {
        "🎵 点歌机回来了".to_string()
    } else {
        "🎵 点歌机来啦！发送 [b]!点歌 歌名[/b] 点歌，[b]!帮助[/b] 查看全部命令，[b]!回去[/b] 让我回音乐频道".to_string()
    };
    if !con.can_send_audio() {
        msg += "\n[color=#c0392b]这个频道需要发言权限，点歌机在这里放不出声音[/color]";
    }
    send_text(con, Reply::Channel, &msg)
}

/// TeamSpeak caps a text message at 8 KiB; keep well below and split on lines.
fn send_text(con: &mut Connection, reply: Reply, text: &str) -> Result<()> {
    let target = match reply {
        Reply::Channel => MessageTarget::Channel,
        Reply::Client(id) => MessageTarget::Client(id),
    };
    let mut chunk = String::new();
    for line in text.lines() {
        if !chunk.is_empty() && chunk.len() + line.len() > 900 {
            con.get_state()?.send_message(target, &chunk).send(con)?;
            chunk.clear();
        }
        if !chunk.is_empty() {
            chunk.push('\n');
        }
        chunk.push_str(line);
    }
    if !chunk.is_empty() {
        con.get_state()?.send_message(target, &chunk).send(con)?;
    }
    Ok(())
}

/// Temporary bots get identities of their own: the server limits clones of one identity.
pub fn load_identity(cfg: &Config, index: usize) -> Result<Identity> {
    let path = cfg.identity_file(index);
    if let Ok(text) = std::fs::read_to_string(&path) {
        return serde_json::from_str(&text).with_context(|| format!("bad identity file {}", path.display()));
    }
    let id = if cfg.ts.identity_key.is_empty() || index != 0 {
        info!("creating a new TeamSpeak identity");
        Identity::create()
    } else {
        let key = EccKeyPrivP256::import_str(&cfg.ts.identity_key).map_err(|e| anyhow!("identity_key: {e}"))?;
        Identity::new(key, cfg.ts.identity_offset)
    };
    crate::write_private(&path, &serde_json::to_string(&id)?)?;
    Ok(id)
}

fn save_identity(cfg: &Config, con: &Connection) {
    if let Some(id) = con.get_options().get_identity() {
        if let Ok(json) = serde_json::to_string(id) {
            let _ = crate::write_private(&cfg.identity_file(0), &json);
        }
    }
}
