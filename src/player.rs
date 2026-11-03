//! Audio pipeline. ffmpeg decodes anything and encodes Opus (so the bot itself carries no
//! codec); we split the Ogg stream into Opus packets and send one every frame.
//!
//! Inputs:
//! - cached file: ffmpeg reads it from disk (no network at all);
//! - first play of a cacheable song: we download it (rate-capped) into the cache and pipe
//!   the same bytes into ffmpeg, so the song crosses the network once;
//! - everything else (seek/resume, yt-dlp streams): ffmpeg reads the URL itself.
//!
//! All stages are coupled by pipe back-pressure, so downloads run at roughly playback speed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::config::{AudioConfig, CacheConfig};

pub enum Input {
    File(PathBuf),
    Url { url: String, headers: Vec<(String, String)> },
    /// Download into the cache while playing.
    Download { url: String, headers: Vec<(String, String)>, song_id: u64, gain_db: f32, ext: String },
}

/// Audio packets going to the TeamSpeak connection.
pub enum AudioOut {
    Opus(Vec<u8>),
    /// Signals the end of a stream to the clients.
    End,
}

// ---- cache --------------------------------------------------------------------------

/// Song cache: `ne-<id>~<gain centi-dB>.<ext>`, evicted oldest-first.
pub struct Cache {
    dir: PathBuf,
    max_bytes: u64,
    /// Stop caching when the disk has less free space than this.
    min_free_bytes: u64,
    index: Mutex<HashMap<u64, PathBuf>>,
}

impl Cache {
    pub fn open(dir: PathBuf, max_mb: u64, min_free_mb: u64) -> Result<Self> {
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let mut index = HashMap::new();
        for e in std::fs::read_dir(&dir)?.flatten() {
            let path = e.path();
            let name = e.file_name().to_string_lossy().into_owned();
            if name.ends_with(".part") {
                let _ = std::fs::remove_file(&path);
            } else if let Some((id, _)) = parse_cache_name(&name) {
                index.insert(id, path);
            }
        }
        let mb = 1024 * 1024;
        Ok(Self { dir, max_bytes: max_mb * mb, min_free_bytes: min_free_mb * mb, index: Mutex::new(index) })
    }

    /// Whether the disk has room for one more song (the server shares it with other services).
    fn has_room(&self) -> bool {
        match free_bytes(&self.dir) {
            Some(free) => free >= self.min_free_bytes,
            None => true,
        }
    }

    /// Cached file and its gain, touching it so it counts as recently used.
    pub fn get(&self, id: u64) -> Option<(PathBuf, f32)> {
        let path = self.index.lock().unwrap().get(&id)?.clone();
        if !path.exists() {
            self.index.lock().unwrap().remove(&id);
            return None;
        }
        if let Ok(f) = std::fs::File::options().append(true).open(&path) {
            let _ = f.set_modified(SystemTime::now());
        }
        let gain = parse_cache_name(&path.file_name()?.to_string_lossy())?.1;
        Some((path, gain))
    }

    fn path_for(&self, id: u64, gain_db: f32, ext: &str) -> PathBuf {
        let ext: String = ext.chars().filter(char::is_ascii_alphanumeric).take(5).collect();
        self.dir.join(format!("ne-{id}~{}.{ext}", (gain_db * 100.0).round() as i32))
    }

    fn commit(&self, id: u64, part: &Path, dest: PathBuf) -> Result<()> {
        std::fs::rename(part, &dest)?;
        self.index.lock().unwrap().insert(id, dest);
        self.evict();
        Ok(())
    }

    fn evict(&self) {
        let Ok(rd) = std::fs::read_dir(&self.dir) else { return };
        let mut files: Vec<(SystemTime, u64, PathBuf)> = rd
            .flatten()
            .filter_map(|e| {
                let m = e.metadata().ok()?;
                Some((m.modified().ok()?, m.len(), e.path()))
            })
            .collect();
        let mut total: u64 = files.iter().map(|f| f.1).sum();
        files.sort();
        for (_, len, path) in files {
            if total <= self.max_bytes {
                break;
            }
            if std::fs::remove_file(&path).is_ok() {
                total -= len;
                if let Some((id, _)) = path.file_name().and_then(|n| parse_cache_name(&n.to_string_lossy())) {
                    self.index.lock().unwrap().remove(&id);
                }
            }
        }
    }
}

#[cfg(unix)]
fn free_bytes(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: valid NUL-terminated path and a zeroed out-struct.
    if unsafe { libc::statvfs(path.as_ptr(), &mut st) } != 0 {
        return None;
    }
    Some(st.f_bavail as u64 * st.f_frsize as u64)
}

#[cfg(not(unix))]
fn free_bytes(_dir: &Path) -> Option<u64> { None }

fn parse_cache_name(name: &str) -> Option<(u64, f32)> {
    let rest = name.strip_prefix("ne-")?;
    let (id, rest) = rest.split_once('~')?;
    let (gain, _ext) = rest.split_once('.')?;
    Some((id.parse().ok()?, gain.parse::<i32>().ok()? as f32 / 100.0))
}

/// Deletes the partial download unless it was committed.
struct PartGuard(Option<PathBuf>);
impl Drop for PartGuard {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

// ---- playback -----------------------------------------------------------------------

pub struct PlayParams {
    pub input: Input,
    pub seek_ms: u64,
    /// Total gain applied by ffmpeg (volume + normalisation).
    pub gain_db: f32,
}

/// Plays one track to the end. `pos_ms` is advanced as packets go out.
/// Cancel by dropping/aborting the task; ffmpeg is killed on drop.
pub async fn play(
    params: PlayParams,
    audio: &AudioConfig,
    cache_cfg: &CacheConfig,
    cache: Arc<Cache>,
    http: reqwest::Client,
    out: mpsc::Sender<AudioOut>,
    pos_ms: Arc<AtomicU64>,
) -> Result<()> {
    let mut cmd = Command::new(&audio.ffmpeg);
    cmd.args(["-hide_banner", "-nostdin", "-loglevel", "error"]);
    let mut download = None;
    match &params.input {
        Input::File(path) => {
            seek_arg(&mut cmd, params.seek_ms);
            cmd.arg("-i").arg(path);
        }
        Input::Url { url, headers } => {
            cmd.args(["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "4"]);
            cmd.args(["-rw_timeout", "15000000"]);
            if !headers.is_empty() {
                let h: String = headers.iter().map(|(k, v)| format!("{k}: {v}\r\n")).collect();
                cmd.arg("-headers").arg(h);
            }
            seek_arg(&mut cmd, params.seek_ms);
            cmd.arg("-i").arg(url);
        }
        Input::Download { url, headers, song_id, gain_db, ext } => {
            cmd.args(["-i", "pipe:0"]);
            let dest = cache.path_for(*song_id, *gain_db, ext);
            download = Some((url.clone(), headers.clone(), *song_id, dest));
        }
    }
    cmd.args(["-map", "0:a:0", "-vn", "-sn", "-dn"]);
    cmd.arg("-af").arg(format!("volume={:.2}dB", params.gain_db));
    cmd.args(["-ac", "2", "-ar", "48000", "-c:a", "libopus", "-application", "audio", "-vbr", "on"]);
    cmd.arg("-b:a").arg(format!("{}k", audio.bitrate_kbps));
    cmd.arg("-frame_duration").arg(audio.frame_ms.to_string());
    cmd.args(["-f", "ogg", "-page_duration", "100000", "-flush_packets", "1", "pipe:1"]);
    cmd.stdin(if download.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child: Child = cmd.spawn().context("无法启动 ffmpeg")?;
    let stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let stderr_task = tokio::spawn(async move {
        let mut s = String::new();
        let _ = stderr.read_to_string(&mut s).await;
        s
    });

    // Feeds the pipe and the cache. Aborted (and the partial file removed) when the
    // track stops early.
    let dl_task = download.map(|(url, headers, id, dest)| {
        let stdin = child.stdin.take().unwrap();
        let cache = cache.clone();
        let kbps = cache_cfg.download_kbps;
        let enabled = cache_cfg.enabled;
        tokio::spawn(async move {
            if let Err(e) = download_into(http, &url, &headers, stdin, &cache, id, dest, kbps, enabled).await {
                debug!("download ended: {e:#}");
            }
        })
    });
    let _dl_guard = AbortOnDrop(dl_task);

    let (pkt_tx, mut pkt_rx) = mpsc::channel::<Vec<u8>>(16);
    let reader = tokio::spawn(read_ogg(stdout, pkt_tx));
    let _reader_guard = AbortOnDrop(Some(reader));

    let frame = Duration::from_millis(audio.frame_ms as u64);
    let mut deadline: Option<Instant> = None;
    let mut sent = 0u64;
    while let Some(pkt) = pkt_rx.recv().await {
        let now = Instant::now();
        let at = match deadline {
            // Fell behind (slow source): restart the clock instead of bursting.
            Some(d) if now <= d + Duration::from_millis(200) => d,
            _ => now,
        };
        tokio::time::sleep_until(at.into()).await;
        if out.send(AudioOut::Opus(pkt)).await.is_err() {
            bail!("connection closed");
        }
        sent += 1;
        pos_ms.fetch_add(audio.frame_ms as u64, Ordering::Relaxed);
        deadline = Some(at + frame);
    }
    let _ = out.send(AudioOut::End).await;

    let status = child.wait().await?;
    let err = stderr_task.await.unwrap_or_default();
    if !status.success() || sent == 0 {
        let msg = err.lines().rev().find(|l| !l.trim().is_empty()).unwrap_or("").trim().to_string();
        if sent == 0 {
            bail!("ffmpeg 没有输出音频 {msg}");
        }
        warn!("ffmpeg exited with {status}: {msg}");
    }
    Ok(())
}

fn seek_arg(cmd: &mut Command, seek_ms: u64) {
    if seek_ms > 0 {
        cmd.arg("-ss").arg(format!("{:.3}", seek_ms as f64 / 1000.0));
    }
}

struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(h) = &self.0 {
            h.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn download_into(
    http: reqwest::Client,
    url: &str,
    headers: &[(String, String)],
    mut stdin: ChildStdin,
    cache: &Cache,
    id: u64,
    dest: PathBuf,
    kbps: u32,
    keep: bool,
) -> Result<()> {
    let mut req = http.get(url);
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req.send().await?.error_for_status()?;
    let keep = keep && cache.has_room();
    // Two bots may fetch the same song at once, so each gets its own partial file.
    static PART: AtomicU64 = AtomicU64::new(0);
    let part = dest.with_extension(format!("{}.part", PART.fetch_add(1, Ordering::Relaxed)));
    let mut guard = PartGuard(keep.then(|| part.clone()));
    let mut file = if keep { Some(tokio::fs::File::create(&part).await?) } else { None };

    let start = Instant::now();
    let mut total = 0u64;
    let mut body = resp.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if let Some(f) = file.as_mut() {
            f.write_all(&chunk).await?;
        }
        // Errors here mean ffmpeg is gone (track skipped): stop without caching.
        stdin.write_all(&chunk).await?;
        total += chunk.len() as u64;
        if kbps > 0 {
            let due = Duration::from_secs_f64(total as f64 * 8.0 / (kbps as f64 * 1000.0));
            if let Some(wait) = due.checked_sub(start.elapsed()) {
                tokio::time::sleep(wait).await;
            }
        }
    }
    drop(stdin);
    if let Some(mut f) = file {
        f.flush().await?;
        drop(f);
        guard.0 = None;
        cache.commit(id, &part, dest)?;
    }
    Ok(())
}

/// Splits an Ogg Opus stream into raw Opus packets, skipping OpusHead and OpusTags.
async fn read_ogg(stdout: impl AsyncRead + Unpin, tx: mpsc::Sender<Vec<u8>>) {
    let mut r = BufReader::with_capacity(16 * 1024, stdout);
    let mut packet = Vec::new();
    let mut index = 0u64;
    let mut header = [0u8; 27];
    let mut lacing = [0u8; 255];
    loop {
        if r.read_exact(&mut header).await.is_err() || &header[..4] != b"OggS" {
            return;
        }
        let segments = header[26] as usize;
        if r.read_exact(&mut lacing[..segments]).await.is_err() {
            return;
        }
        for &len in &lacing[..segments] {
            let start = packet.len();
            packet.resize(start + len as usize, 0);
            if r.read_exact(&mut packet[start..]).await.is_err() {
                return;
            }
            if len < 255 {
                let done = std::mem::take(&mut packet);
                if index >= 2 && !done.is_empty() && tx.send(done).await.is_err() {
                    return;
                }
                index += 1;
            }
        }
    }
}
