use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub ts: TsConfig,
    pub audio: AudioConfig,
    pub netease: NeteaseConfig,
    pub cache: CacheConfig,
    pub bot: BotConfig,
    /// Directory holding identity, cookie and cache. Defaults to the config file's directory.
    #[serde(skip)]
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TsConfig {
    pub address: String,
    pub password: String,
    /// Channel to join, by id (0 = server default channel).
    pub channel_id: u64,
    pub channel_password: String,
    pub nickname: String,
    /// Optional TeamSpeak identity to reuse (TS3AudioBot `key` + `offset`), so that the
    /// bot keeps the server groups of an existing identity.
    pub identity_key: String,
    pub identity_offset: u64,
    /// After `!来`, go back to `channel_id` once alone this long (0 = never).
    pub return_home_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AudioConfig {
    pub ffmpeg: String,
    /// Opus bitrate sent to TeamSpeak. Every listener in the channel receives one copy,
    /// so server upload ≈ (bitrate + ~18 kbps overhead) × listeners.
    pub bitrate_kbps: u32,
    /// Opus frame length: 20, 40 or 60 ms. Longer frames cut packet overhead.
    pub frame_ms: u32,
    pub volume: u8,
    pub max_volume: u8,
    /// Apply Netease's loudness gain so songs play at a similar level.
    pub normalize: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NeteaseConfig {
    /// Cookie of a logged-in account (`MUSIC_U=...`). `ringo-music login` writes it to
    /// `netease_cookie.txt` in the data directory, which takes precedence.
    pub cookie: String,
    /// standard (128k) | higher (192k) | exhigh (320k) | lossless
    pub quality: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    pub enabled: bool,
    pub max_mb: u64,
    /// Don't cache new songs while the disk has less free space than this.
    pub min_free_mb: u64,
    /// Inbound rate cap while downloading a song, so a burst can't saturate the line.
    pub download_kbps: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BotConfig {
    /// Stop sending audio while nobody (with speakers on) is in the bot's channel.
    pub pause_when_alone: bool,
    pub max_queue: usize,
    /// Most bots at once, including the main one (1 = no temporary bots).
    pub max_bots: usize,
    /// A temporary bot leaves after being alone or idle this long.
    pub temp_idle_secs: u64,
    pub max_playlist_import: usize,
    pub search_results: usize,
    pub ytdlp: String,
    /// Optional Netscape cookies.txt passed to yt-dlp (e.g. your own Bilibili login).
    pub ytdlp_cookies: String,
    /// Search Bilibili when Netease has no full copy. Off by default.
    pub bilibili_fallback: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ts: TsConfig::default(),
            audio: AudioConfig::default(),
            netease: NeteaseConfig::default(),
            cache: CacheConfig::default(),
            bot: BotConfig::default(),
            data_dir: PathBuf::from("."),
        }
    }
}

impl Default for TsConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:9987".into(),
            password: String::new(),
            channel_id: 0,
            channel_password: String::new(),
            nickname: "🎵 点歌机".into(),
            identity_key: String::new(),
            identity_offset: 0,
            return_home_secs: 300,
        }
    }
}

impl Default for AudioConfig {
    fn default() -> Self {
        Self {
            ffmpeg: "ffmpeg".into(),
            bitrate_kbps: 64,
            frame_ms: 20,
            volume: 40,
            max_volume: 80,
            normalize: true,
        }
    }
}

impl Default for NeteaseConfig {
    fn default() -> Self { Self { cookie: String::new(), quality: "higher".into() } }
}

impl Default for CacheConfig {
    fn default() -> Self { Self { enabled: true, max_mb: 2048, min_free_mb: 2048, download_kbps: 1500 } }
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            pause_when_alone: true,
            max_queue: 2000,
            max_bots: 3,
            temp_idle_secs: 180,
            max_playlist_import: 2000,
            search_results: 8,
            ytdlp: "yt-dlp".into(),
            ytdlp_cookies: String::new(),
            bilibili_fallback: false,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let mut cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        cfg.data_dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf();
        if ![20, 40, 60].contains(&cfg.audio.frame_ms) {
            cfg.audio.frame_ms = 20;
        }
        cfg.audio.bitrate_kbps = cfg.audio.bitrate_kbps.clamp(16, 192);
        cfg.audio.max_volume = cfg.audio.max_volume.clamp(1, 100);
        cfg.audio.volume = cfg.audio.volume.min(cfg.audio.max_volume);
        Ok(cfg)
    }

    pub fn cache_dir(&self) -> PathBuf { self.data_dir.join("cache") }
    /// Identity of bot `index` (0 = the main bot).
    pub fn identity_file(&self, index: usize) -> PathBuf {
        match index {
            0 => self.data_dir.join("identity.json"),
            i => self.data_dir.join(format!("identity-{i}.json")),
        }
    }
    pub fn cookie_file(&self) -> PathBuf { self.data_dir.join("netease_cookie.txt") }
    pub fn state_file(&self) -> PathBuf { self.data_dir.join("state.json") }
}
