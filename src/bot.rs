//! Queue, commands and playback control.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use rand::seq::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::Config;
use crate::netease::{PlaylistInfo, Song};
use crate::player::{self, AudioOut, Cache, Input, PlayParams};
use crate::resolve::{Resolver, Source, Stream, Track};
use crate::ts::{BotEvent, ConnCmd, Reply};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Mode {
    Normal,
    RepeatOne,
    RepeatAll,
    Shuffle,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Normal => "顺序播放",
            Mode::RepeatOne => "单曲循环",
            Mode::RepeatAll => "列表循环",
            Mode::Shuffle => "随机播放",
        }
    }
}

/// Queue and settings kept across restarts (state.json), so an update or crash
/// doesn't throw away what people queued.
#[derive(Serialize, Deserialize)]
struct Saved {
    queue: VecDeque<Track>,
    current: Option<Track>,
    offset_ms: u64,
    volume: u8,
    mode: Mode,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    played: usize,
}

struct Current {
    track: Track,
    stream: Option<Stream>,
    /// Where the next start should seek to.
    offset_ms: u64,
}

struct Playing {
    gen: u64,
    task: JoinHandle<()>,
    pos: Arc<AtomicU64>,
    base_ms: u64,
}

pub struct Bot {
    cfg: Arc<Config>,
    resolver: Arc<Resolver>,
    cache: Arc<Cache>,
    http: reqwest::Client,
    conn: mpsc::Sender<ConnCmd>,
    audio: mpsc::Sender<AudioOut>,
    done_tx: mpsc::Sender<(u64, Result<()>)>,
    done_rx: mpsc::Receiver<(u64, Result<()>)>,
    queue: VecDeque<Track>,
    current: Option<Current>,
    player: Option<Playing>,
    gen: u64,
    volume: u8,
    mode: Mode,
    /// The last `played` tracks of the queue were already played this round and put
    /// back by list loop / shuffle; they come round again only after the rest.
    played: usize,
    listeners: usize,
    user_paused: bool,
    searches: HashMap<String, Vec<Song>>,
    playlist_searches: HashMap<String, Vec<PlaylistInfo>>,
    description: String,
    /// Consecutive unplayable tracks, for compact reporting.
    skipped: u32,
    fallback_failures: u32,
    fallback_off_until: Option<Instant>,
    saved_json: String,
    /// Only the main bot keeps its queue across restarts.
    persist: bool,
    busy: Arc<AtomicBool>,
}

impl Bot {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: Arc<Config>,
        resolver: Arc<Resolver>,
        cache: Arc<Cache>,
        http: reqwest::Client,
        conn: mpsc::Sender<ConnCmd>,
        audio: mpsc::Sender<AudioOut>,
        persist: bool,
        busy: Arc<AtomicBool>,
    ) -> Self {
        let (done_tx, done_rx) = mpsc::channel(4);
        Self {
            volume: cfg.audio.volume,
            cfg,
            resolver,
            cache,
            http,
            conn,
            audio,
            done_tx,
            done_rx,
            queue: VecDeque::new(),
            current: None,
            player: None,
            gen: 0,
            mode: Mode::Normal,
            played: 0,
            listeners: 0,
            user_paused: false,
            searches: HashMap::new(),
            playlist_searches: HashMap::new(),
            description: String::new(),
            skipped: 0,
            fallback_failures: 0,
            fallback_off_until: None,
            saved_json: String::new(),
            persist,
            busy,
        }
        .restored()
    }

    fn restored(mut self) -> Self {
        if !self.persist {
            return self;
        }
        let Ok(text) = std::fs::read_to_string(self.cfg.state_file()) else { return self };
        match serde_json::from_str::<Saved>(&text) {
            Ok(s) => {
                info!("restored queue: {} tracks{}", s.queue.len(), if s.current.is_some() { " + current" } else { "" });
                self.queue = s.queue;
                self.current = s.current.map(|track| Current { track, stream: None, offset_ms: s.offset_ms });
                self.volume = s.volume.min(self.cfg.audio.max_volume);
                self.mode = s.mode;
                self.played = s.played.min(self.queue.len());
                self.user_paused = s.paused;
                self.saved_json = text;
            }
            Err(e) => warn!("ignoring unreadable state.json: {e}"),
        }
        self
    }

    fn save_state(&mut self) {
        self.busy.store(self.current.is_some() || !self.queue.is_empty(), Ordering::Relaxed);
        if !self.persist {
            return;
        }
        let saved = Saved {
            queue: self.queue.clone(),
            current: self.current.as_ref().map(|c| c.track.clone()),
            offset_ms: self.position_ms(),
            volume: self.volume,
            mode: self.mode,
            paused: self.user_paused,
            played: self.played,
        };
        let Ok(json) = serde_json::to_string(&saved) else { return };
        if json == self.saved_json {
            return;
        }
        let path = self.cfg.state_file();
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, &json).and_then(|_| std::fs::rename(&tmp, &path)).is_ok() {
            self.saved_json = json;
        }
    }

    pub async fn run(mut self, mut events: mpsc::Receiver<BotEvent>) {
        loop {
            tokio::select! {
                ev = events.recv() => match ev {
                    None => break,
                    Some(BotEvent::Listeners(n)) => {
                        if (n == 0) != (self.listeners == 0) {
                            info!("listeners: {n}");
                        }
                        self.listeners = n;
                        self.sync().await;
                    }
                    Some(BotEvent::Command { reply, uid, name, text }) => {
                        info!("{name}: {text}");
                        if let Err(e) = self.command(reply, &uid, &name, &text).await {
                            self.say(reply, format!("[color=#c0392b]{e}[/color]")).await;
                        }
                    }
                },
                Some((gen, res)) = self.done_rx.recv() => {
                    if self.player.as_ref().is_some_and(|p| p.gen == gen) {
                        self.player = None;
                        if let Err(e) = &res {
                            warn!("playback failed: {e:#}");
                            let name = self.current.as_ref().map(|c| c.track.display()).unwrap_or_default();
                            self.say(Reply::Channel, format!("[color=#c0392b]播放失败：{name}（{e}）[/color]")).await;
                        }
                        self.finish_current(res.is_err());
                        self.sync().await;
                    }
                }
            }
            self.save_state();
        }
        self.stop_player();
        self.save_state();
    }

    async fn say(&self, reply: Reply, text: impl Into<String>) {
        let _ = self.conn.send(ConnCmd::Text(reply, text.into())).await;
    }

    fn should_play(&self) -> bool {
        !self.user_paused && (!self.cfg.bot.pause_when_alone || self.listeners > 0)
    }

    /// Brings the player in line with the state: starts, stops or moves to the next track.
    async fn sync(&mut self) {
        loop {
            if self.current.is_none() {
                let Some(track) = self.pop_next() else {
                    self.report_skips().await;
                    self.set_description("💤 空闲中 · 发送 !帮助 查看命令".into()).await;
                    return;
                };
                self.current = Some(Current { track, stream: None, offset_ms: 0 });
            }
            if !self.should_play() {
                if self.player.is_some() {
                    self.stop_player();
                }
                let title = self.current.as_ref().unwrap().track.display();
                self.set_description(format!("⏸ {title}")).await;
                return;
            }
            if self.player.is_some() {
                return;
            }
            match self.start_player().await {
                Ok(()) => {
                    self.report_skips().await;
                    let cur = self.current.as_ref().unwrap();
                    let title = cur.track.display();
                    if cur.offset_ms == 0 {
                        let mut msg = format!("▶ 正在播放：[b]{title}[/b]");
                        if cur.track.duration_ms > 0 {
                            msg += &format!(" ({})", fmt_time(cur.track.duration_ms));
                        }
                        msg += &format!(" · 点歌人：{}", cur.track.requester);
                        self.say(Reply::Channel, msg).await;
                    }
                    self.set_description(format!("▶ {title}")).await;
                    return;
                }
                Err(e) => {
                    let name = self.current.as_ref().unwrap().track.display();
                    warn!("cannot play {name}: {e:#}");
                    // A playlist full of unplayable songs would flood the chat: report the
                    // first two, then just count.
                    self.skipped += 1;
                    if self.skipped <= 2 {
                        self.say(Reply::Channel, format!("[color=#c0392b]跳过 {name}：{e}[/color]")).await;
                    }
                    self.current = None;
                }
            }
        }
    }

    async fn report_skips(&mut self) {
        if self.skipped > 2 {
            let n = self.skipped - 2;
            self.say(Reply::Channel, format!("[color=#c0392b]另外跳过了 {n} 首无法播放的歌[/color]")).await;
        }
        self.skipped = 0;
    }

    /// Shuffle keeps the queue itself shuffled, so it always plays from the front:
    /// every song once per round, and !列表 / !插播 / !置顶 mean what they say.
    fn pop_next(&mut self) -> Option<Track> {
        if self.unplayed() == 0 {
            // Everything was played: start the next round.
            self.played = 0;
            if self.mode == Mode::Shuffle {
                self.queue.make_contiguous().shuffle(&mut rand::rng());
            }
        }
        self.take(0)
    }

    fn unplayed(&self) -> usize {
        self.queue.len() - self.played
    }

    fn take(&mut self, i: usize) -> Option<Track> {
        let t = self.queue.remove(i)?;
        if i + self.played > self.queue.len() {
            self.played -= 1;
        }
        Some(t)
    }

    /// Puts a played track back for the next round (list loop / shuffle).
    fn requeue(&mut self, t: Track) {
        self.queue.push_back(t);
        self.played += 1;
    }

    fn clear_queue(&mut self) {
        self.queue.clear();
        self.played = 0;
    }

    /// Shuffles the tracks that haven't been played this round.
    fn shuffle_unplayed(&mut self) {
        let n = self.unplayed();
        self.queue.make_contiguous()[..n].shuffle(&mut rand::rng());
    }

    /// The current track ended (or failed): apply the play mode.
    fn finish_current(&mut self, failed: bool) {
        let Some(cur) = self.current.take() else { return };
        if failed {
            return;
        }
        match self.mode {
            Mode::RepeatOne => {
                self.current = Some(Current { track: cur.track, stream: cur.stream, offset_ms: 0 });
            }
            Mode::RepeatAll | Mode::Shuffle => self.requeue(cur.track),
            Mode::Normal => {}
        }
    }

    async fn start_player(&mut self) -> Result<()> {
        let cache_enabled = self.cfg.cache.enabled;
        let cur = self.current.as_mut().unwrap();
        let seek = cur.offset_ms;

        let cached = match cur.track.source {
            Source::Netease(id) if cache_enabled => self.cache.get(id),
            _ => None,
        };
        let (input, norm_gain) = if let Some((path, gain)) = cached {
            (Input::File(path), gain)
        } else {
            if cur.stream.as_ref().is_none_or(Stream::is_stale) {
                cur.stream = self.resolver.stream(&cur.track).await?;
            }
            if cur.stream.as_ref().is_none_or(|s| s.trial) {
                let why = if cur.stream.is_some() { "只有试听片段" } else { "没有这首歌的版权" };
                let hint = if self.resolver.ne.has_login() { "" } else { "（登录有会员的网易云账号可完整播放）" };
                let fallback_up = self.fallback_off_until.is_none_or(|t| Instant::now() >= t);
                if !self.cfg.bot.bilibili_fallback || !fallback_up {
                    bail!("网易云{why}{hint}");
                }
                let keyword = format!("{} {}", cur.track.title, cur.track.artist);
                let requester = cur.track.requester.clone();
                match self.resolver.bilibili(&keyword, &requester).await {
                    Ok((track, stream)) => {
                        self.fallback_failures = 0;
                        let msg = format!("网易云{why}，改放 B 站搜索结果：{}", track.title);
                        cur.track.source = track.source;
                        cur.track.duration_ms = track.duration_ms;
                        cur.stream = Some(stream);
                        let _ = self.conn.send(ConnCmd::Text(Reply::Channel, msg)).await;
                    }
                    Err(e) => {
                        // B站 turns server IPs away now and then; stop trying for a while
                        // instead of stalling on every song.
                        self.fallback_failures += 1;
                        if self.fallback_failures >= 3 {
                            warn!("Bilibili fallback failing, pausing it for 10 minutes");
                            self.fallback_off_until = Some(Instant::now() + Duration::from_secs(600));
                            self.fallback_failures = 0;
                        }
                        bail!("网易云{why}{hint}，B 站搜索也失败了（{}）", short_error(&e));
                    }
                }
            }
            let s = cur.stream.as_ref().unwrap();
            let input = match &s.cache_as {
                Some((id, ext)) if seek == 0 => Input::Download {
                    url: s.url.clone(),
                    headers: s.headers.clone(),
                    song_id: *id,
                    gain_db: s.gain_db,
                    ext: ext.clone(),
                },
                _ => Input::Url { url: s.url.clone(), headers: s.headers.clone() },
            };
            (input, s.gain_db)
        };

        let gain_db = volume_db(self.volume) + if self.cfg.audio.normalize { norm_gain.clamp(-12.0, 6.0) } else { 0.0 };
        self.gen += 1;
        let gen = self.gen;
        let pos = Arc::new(AtomicU64::new(0));
        let params = PlayParams { input, seek_ms: seek, gain_db };
        let (cfg, cache, http, audio, done, p) =
            (self.cfg.clone(), self.cache.clone(), self.http.clone(), self.audio.clone(), self.done_tx.clone(), pos.clone());
        let task = tokio::spawn(async move {
            let r = player::play(params, &cfg.audio, &cfg.cache, cache, http, audio, p).await;
            let _ = done.send((gen, r)).await;
        });
        self.player = Some(Playing { gen, task, pos, base_ms: seek });
        Ok(())
    }

    /// Stops audio, remembering the position for a later resume.
    fn stop_player(&mut self) {
        if let Some(p) = self.player.take() {
            p.task.abort();
            let _ = self.audio.try_send(AudioOut::End);
            if let Some(cur) = self.current.as_mut() {
                cur.offset_ms = p.base_ms + p.pos.load(Ordering::Relaxed);
            }
        }
    }

    fn position_ms(&self) -> u64 {
        match (&self.player, &self.current) {
            (Some(p), _) => p.base_ms + p.pos.load(Ordering::Relaxed),
            (None, Some(c)) => c.offset_ms,
            _ => 0,
        }
    }

    /// Restart the current track from `ms` (volume change, seek).
    async fn restart_at(&mut self, ms: u64) {
        if self.current.is_none() {
            return;
        }
        self.stop_player();
        self.current.as_mut().unwrap().offset_ms = ms;
        self.sync().await;
    }

    async fn set_description(&mut self, text: String) {
        if self.description != text {
            self.description = text.clone();
            let _ = self.conn.send(ConnCmd::Description(text)).await;
        }
    }

    async fn enqueue(&mut self, reply: Reply, mut tracks: Vec<Track>, label: Option<String>, front: bool) {
        let room = self.cfg.bot.max_queue.saturating_sub(self.queue.len());
        if room == 0 {
            self.say(reply, "队列已满").await;
            return;
        }
        let dups = dedup(&mut tracks);
        tracks.truncate(room);
        let n = tracks.len();
        let first = tracks[0].display();
        let idle = self.current.is_none();
        if front {
            for t in tracks.into_iter().rev() {
                self.queue.push_front(t);
            }
        } else {
            // Ahead of the songs already played this round; in shuffle, somewhere random among the rest.
            let played = self.queue.split_off(self.unplayed());
            for t in tracks {
                let at = if self.mode == Mode::Shuffle { rand::rng().random_range(0..=self.queue.len()) } else { self.queue.len() };
                self.queue.insert(at, t);
            }
            self.queue.extend(played);
        }
        let mut msg = match (&label, idle) {
            (Some(l), _) if dups > 0 => format!("已添加{l}，共 {n} 首（去掉了 {dups} 首重复的）"),
            (Some(l), _) => format!("已添加{l}，共 {n} 首"),
            (None, true) => String::new(),
            (None, false) if front => format!("下一首播放：{first}"),
            (None, false) if self.mode == Mode::Shuffle => format!("已添加：{first}（随机播放中）"),
            (None, false) => format!("已添加：{first}（队列第 {} 首）", self.unplayed()),
        };
        if idle && !self.should_play() && self.listeners == 0 && self.cfg.bot.pause_when_alone {
            msg = format!("{msg}\n频道里现在没人在听，有人进来后自动开始播放").trim().to_string();
        }
        if !msg.is_empty() {
            self.say(reply, msg).await;
        }
        if idle {
            self.sync().await;
        }
    }

    async fn command(&mut self, reply: Reply, uid: &str, name: &str, text: &str) -> Result<()> {
        let body = text.trim_start_matches(['!', '！']).trim();
        let (cmd, arg) = match body.split_once(char::is_whitespace) {
            Some((c, a)) => (c.to_lowercase(), a.trim()),
            None => (body.to_lowercase(), ""),
        };
        match cmd.as_str() {
            "p" | "play" | "点歌" | "播放" => {
                if arg.is_empty() {
                    if self.user_paused {
                        self.user_paused = false;
                        self.sync().await;
                    } else {
                        self.say(reply, "用法：!点歌 歌名 / 链接，或 !搜 歌名 后 !点歌 序号").await;
                    }
                    return Ok(());
                }
                let tracks = self.pick_or_resolve(arg, uid, name).await?;
                self.enqueue(reply, tracks.0, tracks.1, false).await;
            }
            "pn" | "插播" => {
                if arg.is_empty() {
                    bail!("用法：!插播 歌名 / 链接 / 搜索序号");
                }
                let tracks = self.pick_or_resolve(arg, uid, name).await?;
                self.enqueue(reply, tracks.0, tracks.1, true).await;
            }
            "s" | "search" | "搜" | "搜索" => {
                if arg.is_empty() {
                    bail!("用法：!搜 歌名");
                }
                let songs = self.resolver.ne.search(arg, self.cfg.bot.search_results).await?;
                if songs.is_empty() {
                    bail!("没有搜到「{arg}」");
                }
                let vip_note = !self.resolver.ne.has_login();
                let mut msg = format!("搜索「{arg}」：");
                for (i, s) in songs.iter().enumerate() {
                    let vip = if vip_note && (s.fee == 1 || s.fee == 4) { " [VIP]" } else { "" };
                    msg += &format!("\n[b]{}.[/b] {} - {} 《{}》 {}{vip}", i + 1, s.name, s.artists, s.album, fmt_time(s.duration_ms));
                }
                msg += "\n发送 !点歌 序号 来点歌";
                if self.searches.len() > 200 {
                    self.searches.clear();
                }
                self.searches.insert(uid.to_string(), songs);
                self.say(reply, msg).await;
            }
            "b" | "bili" | "b站" => {
                if arg.is_empty() {
                    bail!("用法：!b 关键词（从 B 站搜索播放）");
                }
                let (track, _) = self.resolver.bilibili(arg, name).await?;
                self.enqueue(reply, vec![track], None, false).await;
            }
            "n" | "next" | "skip" | "切歌" | "下一首" => {
                let Some(cur) = self.current.take() else { bail!("现在没有在放歌") };
                self.stop_player();
                self.say(reply, format!("已切掉：{}", cur.track.display())).await;
                if matches!(self.mode, Mode::RepeatAll | Mode::Shuffle) {
                    self.requeue(cur.track);
                }
                self.sync().await;
            }
            "pause" | "暂停" => {
                self.user_paused = true;
                self.sync().await;
                self.say(reply, "已暂停，发送 !继续 恢复").await;
            }
            "resume" | "继续" | "恢复" => {
                self.user_paused = false;
                self.sync().await;
            }
            "stop" | "停止" => {
                self.stop_player();
                self.current = None;
                self.clear_queue();
                self.user_paused = false;
                self.sync().await;
                self.say(reply, "已停止并清空队列").await;
            }
            "q" | "queue" | "list" | "ls" | "列表" | "队列" => {
                let mut msg = String::new();
                if let Some(c) = &self.current {
                    let state = if self.player.is_some() { "▶" } else { "⏸" };
                    msg += &format!("{state} [b]{}[/b] {}/{}", c.track.display(), fmt_time(self.position_ms()), fmt_time(c.track.duration_ms));
                } else {
                    msg += "现在没有在放歌";
                }
                for (i, t) in self.queue.iter().take(15).enumerate() {
                    msg += &format!("\n{}. {} · {}", i + 1, t.display(), t.requester);
                }
                if self.queue.len() > 15 {
                    msg += &format!("\n…… 共 {} 首", self.queue.len());
                }
                msg += &format!("\n模式：{} · 音量：{}", self.mode.name(), self.volume);
                self.say(reply, msg).await;
            }
            "np" | "now" | "当前" => {
                let Some(c) = &self.current else { bail!("现在没有在放歌") };
                let msg = format!("{} {}/{} · 点歌人：{}", c.track.display(), fmt_time(self.position_ms()), fmt_time(c.track.duration_ms), c.track.requester);
                self.say(reply, msg).await;
            }
            "rm" | "del" | "删" | "删除" => {
                let i: usize = arg.parse().ok().filter(|i| *i >= 1).ok_or_else(|| anyhow::anyhow!("用法：!删 序号"))?;
                let t = self.take(i - 1).ok_or_else(|| anyhow::anyhow!("队列里没有第 {i} 首"))?;
                self.say(reply, format!("已删除：{}", t.display())).await;
            }
            "top" | "置顶" => {
                let i: usize = arg.parse().ok().filter(|i| *i >= 1).ok_or_else(|| anyhow::anyhow!("用法：!置顶 序号"))?;
                let t = self.take(i - 1).ok_or_else(|| anyhow::anyhow!("队列里没有第 {i} 首"))?;
                let msg = format!("下一首播放：{}", t.display());
                self.queue.push_front(t);
                self.say(reply, msg).await;
            }
            "pl" | "playlist" | "歌单" | "播放歌单" => self.play_playlist(reply, uid, name, arg).await?,
            "spl" | "搜歌单" => {
                if arg.is_empty() {
                    bail!("用法：!搜歌单 关键词");
                }
                let found = self.resolver.ne.search_playlists(arg, self.cfg.bot.search_results).await?;
                if found.is_empty() {
                    bail!("没有搜到「{arg}」相关的歌单");
                }
                let mut msg = format!("歌单搜索「{arg}」：");
                for (i, p) in found.iter().enumerate() {
                    msg += &format!("\n[b]{}.[/b] {} · {} 首 · {}", i + 1, p.name, p.track_count, p.creator);
                }
                msg += "\n发送 !歌单 序号 播放，可以加上 随机 / 倒序 / 第5首 / 循环";
                if self.playlist_searches.len() > 200 {
                    self.playlist_searches.clear();
                }
                self.playlist_searches.insert(uid.to_string(), found);
                self.say(reply, msg).await;
            }
            "clear" | "清空" => {
                self.clear_queue();
                self.say(reply, "队列已清空").await;
            }
            "vol" | "v" | "volume" | "音量" => {
                if arg.is_empty() {
                    self.say(reply, format!("当前音量 {}（最大 {}）", self.volume, self.cfg.audio.max_volume)).await;
                    return Ok(());
                }
                let v: u8 = arg.trim_end_matches('%').parse().map_err(|_| anyhow::anyhow!("用法：!音量 0-{}", self.cfg.audio.max_volume))?;
                self.volume = v.min(self.cfg.audio.max_volume);
                self.say(reply, format!("音量 → {}", self.volume)).await;
                if self.player.is_some() {
                    let at = self.position_ms();
                    self.restart_at(at).await;
                }
            }
            "mode" | "模式" | "循环" | "随机" | "单曲" | "顺序" => {
                let key = if arg.is_empty() { cmd.as_str() } else { arg };
                self.mode = match key {
                    "单曲" | "单曲循环" | "one" => Mode::RepeatOne,
                    "循环" | "列表循环" | "all" | "loop" => Mode::RepeatAll,
                    "随机" | "shuffle" | "random" => Mode::Shuffle,
                    "顺序" | "normal" | "off" => Mode::Normal,
                    _ => {
                        self.say(reply, format!("当前模式：{}。可选：顺序 / 单曲 / 循环 / 随机", self.mode.name())).await;
                        return Ok(());
                    }
                };
                let mut msg = format!("播放模式 → {}", self.mode.name());
                match self.mode {
                    Mode::Shuffle => self.shuffle_unplayed(),
                    // Songs kept only for the next loop round would otherwise play a second time.
                    Mode::Normal if self.played > 0 => {
                        msg += &format!("，去掉了队列末尾已经放过的 {} 首", self.played);
                        self.queue.truncate(self.unplayed());
                        self.played = 0;
                    }
                    _ => {}
                }
                self.say(reply, msg).await;
            }
            "seek" | "跳" | "跳到" => {
                if self.current.is_none() {
                    bail!("现在没有在放歌");
                }
                let ms = parse_time(arg).ok_or_else(|| anyhow::anyhow!("用法：!跳 1:30"))?;
                self.restart_at(ms).await;
            }
            "status" | "状态" => {
                let login = if self.resolver.ne.has_login() { "已登录" } else { "未登录（会员歌曲只能试听）" };
                let msg = format!(
                    "网易云：{login}\n音质：{} · 发送码率：{} kbps · 帧长：{} ms\n听众：{} · 队列：{} 首",
                    self.cfg.netease.quality, self.cfg.audio.bitrate_kbps, self.cfg.audio.frame_ms, self.listeners, self.queue.len()
                );
                self.say(reply, msg).await;
            }
            "h" | "help" | "帮助" | "?" | "？" => self.say(reply, HELP).await,
            _ => {}
        }
        Ok(())
    }

    /// `!歌单 <链接|ID|名字|搜索序号> [第N首|从歌名] [随机|倒序] [循环]`: replaces the queue.
    async fn play_playlist(&mut self, reply: Reply, uid: &str, requester: &str, arg: &str) -> Result<()> {
        let (spec, opts) = parse_playlist_args(arg);
        if spec.is_empty() {
            bail!("用法：!歌单 歌单链接/ID/名字 [第5首 | 从歌名] [随机 | 倒序] [循环]");
        }
        // A small number right after !搜歌单 picks from those results.
        let picked = spec
            .parse::<usize>()
            .ok()
            .filter(|i| (1..=50).contains(i))
            .and_then(|i| self.playlist_searches.get(uid)?.get(i - 1))
            .map(|p| p.id.to_string());
        let (label, mut ids) = self.resolver.playlist_ids(picked.as_deref().unwrap_or(&spec)).await?;
        if ids.is_empty() {
            bail!("{label}是空的");
        }
        let total = ids.len();
        let start_id = match &opts.start {
            Some(PlStart::Index(n)) => {
                Some(*ids.get(n.wrapping_sub(1)).ok_or_else(|| anyhow::anyhow!("{label}只有 {total} 首"))?)
            }
            Some(PlStart::Title(t)) => {
                let t = t.to_lowercase();
                let songs = self.resolver.ne.song_details(&ids).await?;
                let s = songs.iter().find(|s| s.name.to_lowercase().contains(&t));
                Some(s.ok_or_else(|| anyhow::anyhow!("{label}里没有「{t}」"))?.id)
            }
            None => None,
        };
        // The play mode stays as it was; in shuffle mode the new list is shuffled too.
        let shuffle = opts.shuffle || self.mode == Mode::Shuffle;
        if opts.reverse {
            ids.reverse();
        }
        if shuffle {
            ids.shuffle(&mut rand::rng());
        }
        if let Some(pos) = start_id.and_then(|id| ids.iter().position(|x| *x == id)) {
            if shuffle {
                let first = ids.remove(pos);
                ids.insert(0, first);
            } else {
                // Start there and wrap around, so the whole list still gets played.
                ids.rotate_left(pos);
            }
        }
        ids.truncate(self.cfg.bot.max_playlist_import.min(self.cfg.bot.max_queue));
        let details: HashMap<u64, Song> =
            self.resolver.ne.song_details(&ids).await?.into_iter().map(|s| (s.id, s)).collect();
        let mut tracks: Vec<Track> =
            ids.iter().filter_map(|id| details.get(id)).map(|s| Track::from_song(s, requester)).collect();
        let dups = dedup(&mut tracks);
        let Some(first) = tracks.first().map(Track::display) else { bail!("{label}里没有可以播放的歌") };

        self.stop_player();
        self.current = None;
        let n = tracks.len();
        self.queue = tracks.into();
        self.played = 0;
        self.user_paused = false;
        // Shuffle already comes round again; otherwise 循环 means list loop.
        if opts.repeat && self.mode != Mode::Shuffle {
            self.mode = Mode::RepeatAll;
        }

        let mut how = Vec::new();
        if start_id.is_some() {
            how.push(format!("从《{first}》开始"));
        }
        if shuffle {
            how.push("随机顺序".to_string());
        } else if opts.reverse {
            how.push("倒序".to_string());
        }
        if matches!(self.mode, Mode::RepeatAll | Mode::RepeatOne) {
            how.push(self.mode.name().to_string());
        }
        let mut msg = format!("📃 播放{label}：{n} 首");
        if !how.is_empty() {
            msg += &format!("，{}", how.join("，"));
        }
        if dups > 0 {
            msg += &format!("（去掉了 {dups} 首重复的）");
        }
        if n + dups < total {
            msg += &format!("（歌单共 {total} 首，最多加载 {} 首）", n + dups);
        }
        self.say(reply, msg).await;
        self.sync().await;
        Ok(())
    }

    /// `arg` is either a number from the user's last search, a link or keywords.
    async fn pick_or_resolve(&self, arg: &str, uid: &str, name: &str) -> Result<(Vec<Track>, Option<String>)> {
        if let Ok(i) = arg.parse::<usize>() {
            if let Some(results) = self.searches.get(uid) {
                let song = results.get(i.wrapping_sub(1)).ok_or_else(|| anyhow::anyhow!("搜索结果里没有第 {i} 个"))?;
                return Ok((vec![Track::from_song(song, name)], None));
            }
        }
        let r = self.resolver.resolve(arg, name).await?;
        if r.tracks.is_empty() {
            bail!("没有可以播放的歌曲");
        }
        Ok((r.tracks, r.label))
    }
}

#[derive(Debug, PartialEq)]
enum PlStart {
    Index(usize),
    Title(String),
}

#[derive(Debug, Default, PartialEq)]
struct PlOpts {
    start: Option<PlStart>,
    shuffle: bool,
    reverse: bool,
    repeat: bool,
}

/// Splits `!歌单` arguments into the playlist spec and trailing options.
fn parse_playlist_args(arg: &str) -> (String, PlOpts) {
    let mut opts = PlOpts::default();
    let mut tokens: Vec<&str> = arg.split_whitespace().collect();
    // "从晴天" / "从 晴天": everything after it is a song title.
    if let Some(i) = tokens.iter().skip(1).position(|t| t.starts_with('从')).map(|i| i + 1) {
        let title = tokens[i..].join(" ");
        let title = title.trim_start_matches('从').trim().to_string();
        if !title.is_empty() {
            opts.start = Some(PlStart::Title(title));
        }
        tokens.truncate(i);
    }
    while tokens.len() > 1 {
        let t = *tokens.last().unwrap();
        match t {
            "随机" | "乱序" | "shuffle" | "random" => opts.shuffle = true,
            "倒序" | "reverse" => opts.reverse = true,
            "循环" | "loop" => opts.repeat = true,
            "顺序" => {}
            _ => {
                let n = t.trim_start_matches('第').trim_end_matches('首').trim_end_matches('曲');
                match n.parse::<usize>() {
                    Ok(n) if opts.start.is_none() => opts.start = Some(PlStart::Index(n)),
                    _ => break,
                }
            }
        }
        tokens.pop();
    }
    (tokens.join(" "), opts)
}

const HELP: &str = "[b]点歌机命令[/b]（! 或 ！ 开头，在点歌机所在频道发送，或私聊点歌机）
[b]点歌[/b]：!点歌 歌名 / 网易云链接 / B站链接（!p）— 加入播放队列，按顺序一首接一首放；歌单、专辑链接整张加入
[b]歌单[/b]：!歌单 链接/ID/歌单名 — 换成这个歌单播放（播放模式不变），后面可加 第5首 / 从歌名 / 随机 / 倒序 / 循环；!搜歌单 关键词 → !歌单 序号
!搜 歌名 → !点歌 序号（!s）· !插播 歌名 — 排到下一首（!pn）· !b 关键词 — 从 B 站找
[b]控制[/b]：!切歌 — 跳到下一首（!n）· !暂停 · !继续 · !停止 — 停下并清空队列
[b]队列[/b]：!列表 — 查看队列（!q）· !当前 · !删 序号 · !置顶 序号 · !清空
[b]其他[/b]：!音量 数字 · !跳 1:30 · !模式 顺序/单曲/循环/随机 · !状态
[b]换频道[/b]：!来 — 叫点歌机到你的频道（有密码用 !来 密码）· !回去 — 回音乐频道 · !离开 — 让临时点歌机退出
在别的频道：私聊点歌机或在服务器聊天发命令。主点歌机那边没人，它会直接过来；有人在听，点歌时会开一个临时点歌机到你的频道，有自己的队列";

/// Drops repeats of the same song under another ID (same title and artist), which
/// playlists often have from the album, deluxe edition and single.
fn dedup(tracks: &mut Vec<Track>) -> usize {
    let before = tracks.len();
    let mut seen = std::collections::HashSet::new();
    tracks.retain(|t| seen.insert((t.title.trim().to_lowercase(), t.artist.trim().to_lowercase())));
    before - tracks.len()
}

/// yt-dlp errors are long; keep the useful tail.
fn short_error(e: &anyhow::Error) -> String {
    let s = e.to_string();
    let s = s.split(" (caused by").next().unwrap_or(&s);
    let s = s.rsplit(": ").find(|p| !p.trim().is_empty()).unwrap_or(s);
    s.chars().take(60).collect()
}

fn volume_db(v: u8) -> f32 {
    if v == 0 {
        -60.0
    } else {
        40.0 * (v as f32 / 100.0).log10()
    }
}

fn fmt_time(ms: u64) -> String {
    if ms == 0 {
        return "--:--".into();
    }
    let s = ms / 1000;
    format!("{}:{:02}", s / 60, s % 60)
}

fn parse_time(s: &str) -> Option<u64> {
    let mut total = 0u64;
    for part in s.trim().split([':', '：']) {
        total = total * 60 + part.trim().parse::<u64>().ok()?;
    }
    Some(total * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn playlist_args() {
        let (spec, o) = parse_playlist_args("https://music.163.com/playlist?id=1 第5首 随机 循环");
        assert_eq!(spec, "https://music.163.com/playlist?id=1");
        assert_eq!(o, PlOpts { start: Some(PlStart::Index(5)), shuffle: true, reverse: false, repeat: true });
        let (spec, o) = parse_playlist_args("3778678");
        assert_eq!(spec, "3778678");
        assert_eq!(o, PlOpts::default());
        let (spec, o) = parse_playlist_args("华语 经典 倒序 从 海阔 天空");
        assert_eq!(spec, "华语 经典");
        assert_eq!(o.start, Some(PlStart::Title("海阔 天空".into())));
        assert!(o.reverse);
        let (spec, o) = parse_playlist_args("2 12");
        assert_eq!((spec.as_str(), o.start), ("2", Some(PlStart::Index(12))));
    }

    #[test]
    fn dedup_same_song() {
        let t = |title: &str, artist: &str, id| Track {
            title: title.into(),
            artist: artist.into(),
            duration_ms: 0,
            source: Source::Netease(id),
            requester: String::new(),
        };
        let mut v = vec![t("Heat Waves", "Glass Animals", 1), t("Heat Waves", "Glass Animals/iann dior", 2), t("heat waves ", "Glass Animals", 3)];
        assert_eq!(dedup(&mut v), 1);
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn times() {
        assert_eq!(parse_time("1:30"), Some(90_000));
        assert_eq!(parse_time("45"), Some(45_000));
        assert_eq!(fmt_time(269_000), "4:29");
        assert!((volume_db(100)).abs() < 1e-6);
        assert!((volume_db(50) + 12.04).abs() < 0.01);
    }
}
