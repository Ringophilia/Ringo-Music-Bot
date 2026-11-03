//! Turns what users type (keywords, Netease links, other links) into playable tracks.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::Value;
use tokio::process::Command;

use crate::netease::{Netease, Song};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Source {
    Netease(u64),
    /// Anything yt-dlp understands (Bilibili, YouTube, ...). `page` is re-resolved when
    /// the stream URL gets old.
    Ytdlp { page: String },
    /// A plain audio file URL.
    Direct(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Track {
    pub title: String,
    pub artist: String,
    pub duration_ms: u64,
    pub source: Source,
    pub requester: String,
}

impl Track {
    pub fn from_song(s: &Song, requester: &str) -> Self {
        Self {
            title: s.name.clone(),
            artist: s.artists.clone(),
            duration_ms: s.duration_ms,
            source: Source::Netease(s.id),
            requester: requester.to_string(),
        }
    }

    pub fn display(&self) -> String {
        if self.artist.is_empty() {
            self.title.clone()
        } else {
            format!("{} - {}", self.title, self.artist)
        }
    }
}

/// A stream ready for ffmpeg.
#[derive(Debug, Clone)]
pub struct Stream {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub gain_db: f32,
    /// Netease: cache key info (song id, file extension) when the file may be cached.
    pub cache_as: Option<(u64, String)>,
    pub resolved_at: Instant,
    /// Only a preview is available.
    pub trial: bool,
}

impl Stream {
    pub fn is_stale(&self) -> bool { self.resolved_at.elapsed() > Duration::from_secs(15 * 60) }
}

pub struct Request {
    pub tracks: Vec<Track>,
    /// "歌单《x》" etc. for multi-track imports.
    pub label: Option<String>,
}

pub struct Resolver {
    pub ne: Arc<Netease>,
    ytdlp: String,
    ytdlp_cookies: String,
    max_import: usize,
}

const AUDIO_EXTS: &[&str] = &[".mp3", ".flac", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".webm"];

impl Resolver {
    pub fn new(ne: Arc<Netease>, ytdlp: &str, ytdlp_cookies: &str, max_import: usize) -> Self {
        Self { ne, ytdlp: ytdlp.to_string(), ytdlp_cookies: ytdlp_cookies.to_string(), max_import }
    }

    /// Resolves user input. Keywords pick the best Netease match.
    pub async fn resolve(&self, input: &str, requester: &str) -> Result<Request> {
        let input = strip_bbcode(input);
        let input = input.trim();
        if let Some(url) = find_url(input) {
            return self.resolve_url(&url, requester).await;
        }
        let songs = self.ne.search(input, 5).await?;
        let Some(song) = songs.first() else { bail!("网易云没有搜到「{input}」") };
        Ok(Request { tracks: vec![Track::from_song(song, requester)], label: None })
    }

    async fn resolve_url(&self, url: &str, requester: &str) -> Result<Request> {
        let lower = url.to_lowercase();
        let is_ne = lower.contains("music.163.com") || lower.contains("163cn.tv");
        if is_ne {
            let url = if lower.contains("163cn.tv") { self.ne.expand_link(url).await? } else { url.to_string() };
            let (kind, id) = parse_netease_url(&url).context("看不懂这个网易云链接")?;
            return match kind {
                "song" => {
                    let songs = self.ne.song_details(&[id]).await?;
                    let s = songs.first().context("歌曲不存在")?;
                    Ok(Request { tracks: vec![Track::from_song(s, requester)], label: None })
                }
                "playlist" => {
                    let (name, mut ids) = self.ne.playlist(id).await?;
                    ids.truncate(self.max_import);
                    let songs = self.ne.song_details(&ids).await?;
                    let tracks = songs.iter().map(|s| Track::from_song(s, requester)).collect();
                    Ok(Request { tracks, label: Some(format!("歌单《{name}》")) })
                }
                "album" => {
                    let (name, mut songs) = self.ne.album(id).await?;
                    songs.truncate(self.max_import);
                    let tracks = songs.iter().map(|s| Track::from_song(s, requester)).collect();
                    Ok(Request { tracks, label: Some(format!("专辑《{name}》")) })
                }
                _ => bail!("暂不支持这种网易云链接"),
            };
        }
        let path = lower.split(['?', '#']).next().unwrap_or("");
        if AUDIO_EXTS.iter().any(|e| path.ends_with(e)) {
            let name = path.rsplit('/').next().unwrap_or(url);
            let title = percent_decode(name);
            let track = Track {
                title,
                artist: String::new(),
                duration_ms: 0,
                source: Source::Direct(url.to_string()),
                requester: requester.to_string(),
            };
            return Ok(Request { tracks: vec![track], label: None });
        }
        let (track, _) = self.ytdlp_track(url, requester).await?;
        Ok(Request { tracks: vec![track], label: None })
    }

    /// Playlist (or album) by link, id or name, as its name and all track ids in order.
    pub async fn playlist_ids(&self, spec: &str) -> Result<(String, Vec<u64>)> {
        let spec = strip_bbcode(spec);
        let spec = spec.trim();
        if let Some(url) = find_url(spec) {
            let url = if url.to_lowercase().contains("163cn.tv") { self.ne.expand_link(&url).await? } else { url };
            return match parse_netease_url(&url) {
                Some(("playlist", id)) => self.labeled_playlist(id).await,
                Some(("album", id)) => {
                    let (name, songs) = self.ne.album(id).await?;
                    Ok((format!("专辑《{name}》"), songs.iter().map(|s| s.id).collect()))
                }
                Some(_) => bail!("这是单曲链接，请用 !点歌"),
                None => bail!("只支持网易云歌单或专辑链接"),
            };
        }
        if let Ok(id) = spec.parse::<u64>() {
            return self.labeled_playlist(id).await;
        }
        let found = self.ne.search_playlists(spec, 1).await?;
        let p = found.first().with_context(|| format!("没有搜到叫「{spec}」的歌单"))?;
        self.labeled_playlist(p.id).await
    }

    async fn labeled_playlist(&self, id: u64) -> Result<(String, Vec<u64>)> {
        let (name, ids) = self.ne.playlist(id).await?;
        Ok((format!("歌单《{name}》"), ids))
    }

    /// Bilibili search through yt-dlp, first hit.
    pub async fn bilibili(&self, keyword: &str, requester: &str) -> Result<(Track, Stream)> {
        self.ytdlp_track(&format!("bilisearch1:{keyword}"), requester).await
    }

    pub async fn ytdlp_track(&self, target: &str, requester: &str) -> Result<(Track, Stream)> {
        let v = self.ytdlp_json(target).await?;
        let v = if v["_type"] == "playlist" { v["entries"][0].clone() } else { v };
        if v.is_null() {
            bail!("没有找到可播放的内容");
        }
        let page = v["webpage_url"].as_str().unwrap_or(target).to_string();
        let track = Track {
            title: v["title"].as_str().unwrap_or("未知").to_string(),
            artist: v["uploader"].as_str().unwrap_or("").to_string(),
            duration_ms: (v["duration"].as_f64().unwrap_or(0.0) * 1000.0) as u64,
            source: Source::Ytdlp { page },
            requester: requester.to_string(),
        };
        Ok((track, ytdlp_stream(&v)?))
    }

    async fn ytdlp_json(&self, target: &str) -> Result<Value> {
        let mut cmd = Command::new(&self.ytdlp);
        cmd.args(["-J", "--no-playlist", "--no-warnings", "-f", "bestaudio/best"]);
        if !self.ytdlp_cookies.is_empty() {
            cmd.arg("--cookies").arg(&self.ytdlp_cookies);
        }
        cmd.arg(target)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        let out = tokio::time::timeout(Duration::from_secs(30), cmd.output())
            .await
            .context("yt-dlp 超时")?
            .context("无法运行 yt-dlp")?;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            let line = err.lines().rev().find(|l| l.contains("ERROR")).unwrap_or("解析失败");
            bail!("{}", line.trim());
        }
        Ok(serde_json::from_slice(&out.stdout)?)
    }

    /// Gets a fresh stream for a track.
    pub async fn stream(&self, track: &Track) -> Result<Option<Stream>> {
        match &track.source {
            Source::Netease(id) => {
                let Some(u) = self.ne.song_url(*id).await? else { return Ok(None) };
                Ok(Some(Stream {
                    headers: vec![("Referer".into(), "https://music.163.com/".into())],
                    gain_db: u.gain,
                    cache_as: (!u.trial).then(|| (*id, u.kind.clone())),
                    resolved_at: Instant::now(),
                    trial: u.trial,
                    url: u.url,
                }))
            }
            Source::Ytdlp { page } => {
                let v = self.ytdlp_json(page).await?;
                Ok(Some(ytdlp_stream(&v)?))
            }
            Source::Direct(url) => Ok(Some(Stream {
                url: url.clone(),
                headers: Vec::new(),
                gain_db: 0.0,
                cache_as: None,
                resolved_at: Instant::now(),
                trial: false,
            })),
        }
    }
}

fn ytdlp_stream(v: &Value) -> Result<Stream> {
    // With `-f`, the selected format's url sits at top level (or in requested_formats).
    let fmt = if v["url"].is_string() { v.clone() } else { v["requested_formats"][0].clone() };
    let url = fmt["url"].as_str().context("yt-dlp 没有给出音频地址")?.to_string();
    let headers = fmt["http_headers"]
        .as_object()
        .map(|h| {
            h.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();
    Ok(Stream { url, headers, gain_db: 0.0, cache_as: None, resolved_at: Instant::now(), trial: false })
}

/// TeamSpeak wraps links as `[URL]x[/URL]` or `[URL=x]y[/URL]`.
pub fn strip_bbcode(s: &str) -> String {
    let re = regex_lite::Regex::new(r"(?i)\[url=([^\]]+)\][^\[]*\[/url\]|\[/?url\]|\[/?[biu]\]").unwrap();
    re.replace_all(s, |c: &regex_lite::Captures| c.get(1).map_or(String::new(), |m| m.as_str().to_string()))
        .into_owned()
}

fn find_url(s: &str) -> Option<String> {
    let re = regex_lite::Regex::new(r"https?://[^\s\]\[]+").unwrap();
    if let Some(m) = re.find(s) {
        return Some(m.as_str().to_string());
    }
    // Shared text without scheme, e.g. "music.163.com/song?id=1".
    let re = regex_lite::Regex::new(r"(?i)(?:y\.)?music\.163\.com/[^\s]+|163cn\.tv/[^\s]+").unwrap();
    re.find(s).map(|m| format!("https://{}", m.as_str()))
}

fn parse_netease_url(url: &str) -> Option<(&'static str, u64)> {
    let re = regex_lite::Regex::new(r"(song|playlist|album)(?:/(\d+)|[^#]*?[?&]id=(\d+))").unwrap();
    let c = re.captures(url)?;
    let kind = match &c[1] {
        "song" => "song",
        "playlist" => "playlist",
        _ => "album",
    };
    let id = c.get(2).or(c.get(3))?.as_str().parse().ok()?;
    Some((kind, id))
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn netease_links() {
        assert_eq!(parse_netease_url("https://music.163.com/#/song?id=186016"), Some(("song", 186016)));
        assert_eq!(parse_netease_url("https://music.163.com/song?id=186016&userid=1"), Some(("song", 186016)));
        assert_eq!(parse_netease_url("https://y.music.163.com/m/song/186016/"), Some(("song", 186016)));
        assert_eq!(parse_netease_url("https://music.163.com/playlist?id=3778678&uct2=x"), Some(("playlist", 3778678)));
        assert_eq!(parse_netease_url("https://music.163.com/#/album?id=18905"), Some(("album", 18905)));
    }

    #[test]
    fn bbcode_and_urls() {
        let s = strip_bbcode("[URL]https://music.163.com/song?id=1[/URL]");
        assert_eq!(find_url(&s).as_deref(), Some("https://music.163.com/song?id=1"));
        let s = strip_bbcode("分享歌曲 music.163.com/song?id=2 (来自网易云)");
        assert_eq!(find_url(&s).as_deref(), Some("https://music.163.com/song?id=2"));
        assert_eq!(find_url("晴天 周杰伦"), None);
        assert_eq!(percent_decode("a%20b.mp3"), "a b.mp3");
    }
}
