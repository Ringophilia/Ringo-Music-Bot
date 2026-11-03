mod bot;
mod config;
mod fleet;
mod netease;
mod player;
mod resolve;
mod ts;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::sync::{mpsc, watch};
use tracing::info;

use crate::config::Config;
use crate::netease::Netease;
use crate::player::Cache;
use crate::resolve::Resolver;

const USAGE: &str = "ringo-music — TeamSpeak 3 点歌机

用法:
  ringo-music [-c config.toml] run            运行点歌机（默认）
  ringo-music [-c config.toml] login          扫码登录网易云（在终端显示二维码）
  ringo-music [-c config.toml] search 关键词   测试搜索
  ringo-music [-c config.toml] probe 歌名或链接 测试解析出的播放地址";

fn main() -> Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let mut config_path = PathBuf::from("config.toml");
    if let Some(i) = args.iter().position(|a| a == "-c" || a == "--config") {
        args.remove(i);
        if i >= args.len() {
            bail!("-c needs a path");
        }
        config_path = PathBuf::from(args.remove(i));
    }
    let cmd = if args.is_empty() { "run".to_string() } else { args.remove(0) };
    if matches!(cmd.as_str(), "-h" | "--help" | "help") {
        println!("{USAGE}");
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tsproto=warn,tsclientlib=warn".into()),
        )
        .with_target(false)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .without_time() // journald adds timestamps
        .init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async {
        let cfg = Arc::new(Config::load(&config_path)?);
        match cmd.as_str() {
            "run" => run(cfg).await,
            "login" => login(&cfg).await,
            "search" => search(&cfg, &args.join(" ")).await,
            "probe" => probe(&cfg, &args.join(" ")).await,
            "test-client" => {
                let [channel, nick, secs, commands @ ..] = args.as_slice() else {
                    bail!("test-client <channel id> <nickname> <seconds> [command...]");
                };
                ts::test_client(&cfg, channel.parse()?, nick, secs.parse()?, commands.to_vec()).await
            }
            other => bail!("unknown command `{other}`\n\n{USAGE}"),
        }
    })
}

fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(60))
        .build()?)
}

fn netease(cfg: &Config, http: &reqwest::Client) -> Arc<Netease> {
    Arc::new(Netease::new(http.clone(), &cfg.netease.quality, &cfg.netease.cookie, cfg.cookie_file()))
}

async fn run(cfg: Arc<Config>) -> Result<()> {
    let http = http_client()?;
    let ne = netease(&cfg, &http);
    info!("Netease login: {}", if ne.has_login() { "yes" } else { "no (members-only songs play as previews)" });
    let resolver = Arc::new(Resolver::new(ne, &cfg.bot.ytdlp, &cfg.bot.ytdlp_cookies, cfg.bot.max_playlist_import));
    let cache = Arc::new(Cache::open(cfg.cache_dir(), cfg.cache.max_mb, cfg.cache.min_free_mb)?);

    let (audio_tx, audio_rx) = mpsc::channel(4);
    let (conn_tx, conn_rx) = mpsc::channel(64);
    let (bot_tx, bot_rx) = mpsc::channel(64);
    let (stop_tx, stop_rx) = watch::channel(false);

    tokio::spawn(async move {
        shutdown_signal().await;
        info!("shutting down");
        let _ = stop_tx.send(true);
    });

    // Made up front: creating an identity is CPU work that would stall live audio.
    for i in 2..=cfg.bot.max_bots {
        ts::load_identity(&cfg, i)?;
    }
    let fleet = fleet::Fleet::new(cfg.clone(), resolver.clone(), cache.clone(), http.clone(), stop_rx.clone(), conn_tx.clone());
    let busy = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let bot = bot::Bot::new(cfg.clone(), resolver, cache, http, conn_tx, audio_tx, true, busy.clone());
    let bot_task = tokio::spawn(bot.run(bot_rx));
    let channel = tsclientlib::ChannelId(cfg.ts.channel_id);
    let password = Some(cfg.ts.channel_password.clone());
    let role = ts::Role { index: 0, fleet: fleet.clone(), channel, channel_password: password, requester: None, busy };
    let result = ts::run(cfg, role, audio_rx, conn_rx, bot_tx, stop_rx).await;
    fleet.wait_empty(Duration::from_secs(4)).await;
    // bot_tx is gone now, so the bot saves its queue and stops.
    let _ = tokio::time::timeout(Duration::from_secs(3), bot_task).await;
    result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("signal handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn login(cfg: &Config) -> Result<()> {
    let ne = netease(cfg, &http_client()?);
    let key = ne.qr_key().await?;
    let url = format!("https://music.163.com/login?codekey={key}");
    let code = qrcode::QrCode::new(url.as_bytes())?;
    println!("{}", code.render::<qrcode::render::unicode::Dense1x2>().quiet_zone(true).build());
    println!("用网易云音乐 App 扫码登录（链接：{url}）");
    let mut last = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let (code, cookie) = ne.qr_check(&key).await?;
        if code != last {
            match code {
                801 => println!("等待扫码……"),
                802 => println!("已扫码，请在手机上确认"),
                800 => bail!("二维码已过期，请重新运行"),
                803 => {}
                other => println!("状态 {other}"),
            }
            last = code;
        }
        if let Some(cookie) = cookie {
            write_private(&cfg.cookie_file(), &cookie)?;
            println!("登录成功，Cookie 已保存到 {}（点歌机会自动使用，无需重启）", cfg.cookie_file().display());
            return Ok(());
        }
    }
}

async fn search(cfg: &Config, keyword: &str) -> Result<()> {
    let ne = netease(cfg, &http_client()?);
    for s in ne.search(keyword, 10).await? {
        println!("{}\t{} - {}\t《{}》\tfee={}", s.id, s.name, s.artists, s.album, s.fee);
    }
    Ok(())
}

async fn probe(cfg: &Config, input: &str) -> Result<()> {
    let ne = netease(cfg, &http_client()?);
    println!("login: {}", ne.has_login());
    let resolver = Resolver::new(ne, &cfg.bot.ytdlp, &cfg.bot.ytdlp_cookies, cfg.bot.max_playlist_import);
    let req = resolver.resolve(input, "probe").await?;
    if let Some(label) = &req.label {
        println!("{label}: {} tracks", req.tracks.len());
    }
    let track = req.tracks.first().context("no tracks")?;
    println!("track: {} ({:?})", track.display(), track.source);
    match resolver.stream(track).await? {
        Some(s) => println!("stream: trial={} gain={} cache={:?}\n{}", s.trial, s.gain_db, s.cache_as, s.url),
        None => println!("stream: none (no copyright)"),
    }
    Ok(())
}

/// Writes a secret file readable only by the owner.
pub fn write_private(path: &Path, content: &str) -> Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
        f.write_all(content.as_bytes())?;
    }
    #[cfg(not(unix))]
    std::fs::write(path, content)?;
    Ok(())
}
