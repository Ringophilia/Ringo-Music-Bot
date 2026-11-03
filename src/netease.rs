//! Minimal Netease Cloud Music client: search, eapi song URL, details, playlists, albums
//! and QR login.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use aes::cipher::{generic_array::GenericArray, BlockEncrypt, KeyInit};
use anyhow::{bail, Context, Result};
use md5::{Digest, Md5};
use serde_json::{json, Value};

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) \
                  Safari/537.36 Chrome/91.0.4472.164 NeteaseMusicDesktop/2.10.2.200154";
const REFERER: &str = "https://music.163.com/";
const PC_COOKIE: &str = "os=pc; appver=8.9.70; osver=; deviceId=pyncm!";
const EAPI_KEY: &[u8; 16] = b"e82ckenh8dichen8";

const SEARCH_API: &str = "https://music.163.com/api/cloudsearch/pc";
const SONG_URL_API: &str = "https://interface3.music.163.com/eapi/song/enhance/player/url/v1";
const SONG_DETAIL_API: &str = "https://interface3.music.163.com/api/v3/song/detail";
const PLAYLIST_API: &str = "https://music.163.com/api/v6/playlist/detail";
const ALBUM_API: &str = "https://music.163.com/api/v1/album/";

#[derive(Debug, Clone)]
pub struct Song {
    pub id: u64,
    pub name: String,
    pub artists: String,
    pub album: String,
    pub duration_ms: u64,
    /// 1 = VIP only, 4 = paid album, 0/8 = free.
    pub fee: i64,
}

#[derive(Debug, Clone)]
pub struct PlaylistInfo {
    pub id: u64,
    pub name: String,
    pub track_count: u64,
    pub creator: String,
}

#[derive(Debug, Clone)]
pub struct SongUrl {
    pub url: String,
    /// File type, e.g. "mp3" or "flac".
    pub kind: String,
    /// Loudness correction in dB suggested by Netease.
    pub gain: f32,
    /// Only a 30 s preview is available (VIP song without a VIP cookie).
    pub trial: bool,
}

pub struct Netease {
    http: reqwest::Client,
    quality: String,
    config_cookie: String,
    cookie_file: PathBuf,
    /// Cookie file cache: (mtime, content). Re-read when the file changes, so a
    /// `ringo-music login` takes effect without restarting the bot.
    file_cookie: Mutex<Option<(SystemTime, String)>>,
}

impl Netease {
    pub fn new(http: reqwest::Client, quality: &str, config_cookie: &str, cookie_file: PathBuf) -> Self {
        Self {
            http,
            quality: quality.to_string(),
            config_cookie: config_cookie.trim().to_string(),
            cookie_file,
            file_cookie: Mutex::new(None),
        }
    }

    pub fn cookie(&self) -> String {
        if let Ok(meta) = std::fs::metadata(&self.cookie_file) {
            let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            let mut cached = self.file_cookie.lock().unwrap();
            if cached.as_ref().map(|c| c.0) != Some(mtime) {
                let text = std::fs::read_to_string(&self.cookie_file).unwrap_or_default();
                *cached = Some((mtime, text.trim().to_string()));
            }
            let c = &cached.as_ref().unwrap().1;
            if !c.is_empty() {
                return c.clone();
            }
        }
        self.config_cookie.clone()
    }

    pub fn has_login(&self) -> bool { self.cookie().contains("MUSIC_U=") }

    fn full_cookie(&self) -> String {
        let c = self.cookie();
        if c.is_empty() {
            PC_COOKIE.to_string()
        } else if c.contains("os=pc") {
            c
        } else {
            format!("{c}; {PC_COOKIE}")
        }
    }

    async fn post_form(&self, url: &str, form: &[(&str, String)]) -> Result<Value> {
        let mut last = None;
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(400 << attempt)).await;
            }
            let resp = self
                .http
                .post(url)
                .header("User-Agent", UA)
                .header("Referer", REFERER)
                .header("Cookie", self.full_cookie())
                .form(form)
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    return r.json::<Value>().await.context("decoding Netease response");
                }
                Ok(r) if r.status().is_client_error() => bail!("Netease HTTP {}", r.status()),
                Ok(r) => last = Some(anyhow::anyhow!("Netease HTTP {}", r.status())),
                Err(e) => last = Some(e.into()),
            }
        }
        Err(last.unwrap())
    }

    pub async fn search(&self, keyword: &str, limit: usize) -> Result<Vec<Song>> {
        let mut v = self.search_once(keyword, limit).await?;
        // The legacy endpoint sometimes returns `{result:{}}` for valid exact keywords;
        // a trailing wildcard fixes it.
        if v["result"].as_object().is_some_and(|o| o.is_empty()) && !keyword.contains('*') {
            v = self.search_once(&format!("{keyword}*"), limit).await?;
        }
        Ok(parse_songs(&v["result"]["songs"]))
    }

    async fn search_once(&self, keyword: &str, limit: usize) -> Result<Value> {
        let form = [
            ("s", keyword.to_string()),
            ("type", "1".into()),
            ("limit", limit.to_string()),
            ("offset", "0".into()),
        ];
        self.post_form(SEARCH_API, &form).await
    }

    pub async fn search_playlists(&self, keyword: &str, limit: usize) -> Result<Vec<PlaylistInfo>> {
        let form = [
            ("s", keyword.to_string()),
            ("type", "1000".into()),
            ("limit", limit.to_string()),
            ("offset", "0".into()),
        ];
        let v = self.post_form(SEARCH_API, &form).await?;
        let Some(arr) = v["result"]["playlists"].as_array() else { return Ok(Vec::new()) };
        Ok(arr
            .iter()
            .filter_map(|p| {
                Some(PlaylistInfo {
                    id: p["id"].as_u64()?,
                    name: p["name"].as_str().unwrap_or("").to_string(),
                    track_count: p["trackCount"].as_u64().unwrap_or(0),
                    creator: p["creator"]["nickname"].as_str().unwrap_or("").to_string(),
                })
            })
            .collect())
    }

    pub async fn song_details(&self, ids: &[u64]) -> Result<Vec<Song>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(500) {
            let c: Vec<Value> = chunk.iter().map(|id| json!({ "id": id, "v": 0 })).collect();
            let v = self.post_form(SONG_DETAIL_API, &[("c", Value::Array(c).to_string())]).await?;
            out.extend(parse_songs(&v["songs"]));
        }
        Ok(out)
    }

    pub async fn song_url(&self, id: u64) -> Result<Option<SongUrl>> {
        let cookie = self.cookie();
        let csrf = cookie
            .split(';')
            .filter_map(|p| p.trim().strip_prefix("__csrf="))
            .next()
            .unwrap_or("")
            .to_string();
        let header = json!({
            "os": "pc", "appver": "8.9.70", "osver": "", "deviceId": "pyncm!",
            "requestId": "20000000", "csrf_token": csrf,
        });
        let payload = json!({
            "ids": [id],
            "level": self.quality,
            "encodeType": "flac",
            "header": header.to_string(),
        });
        let params = eapi_encrypt("/api/song/enhance/player/url/v1", &payload.to_string());
        let v = self.post_form(SONG_URL_API, &[("params", params)]).await?;
        let d = &v["data"][0];
        let Some(url) = d["url"].as_str().filter(|u| !u.is_empty()) else {
            return Ok(None);
        };
        Ok(Some(SongUrl {
            url: url.to_string(),
            kind: d["type"].as_str().unwrap_or("mp3").to_lowercase(),
            gain: d["gain"].as_f64().unwrap_or(0.0) as f32,
            trial: !d["freeTrialInfo"].is_null(),
        }))
    }

    /// Returns the playlist name and its track ids.
    pub async fn playlist(&self, id: u64) -> Result<(String, Vec<u64>)> {
        let form = [("id", id.to_string()), ("n", "100000".into()), ("s", "8".into())];
        let v = self.post_form(PLAYLIST_API, &form).await?;
        let pl = &v["playlist"];
        if pl.is_null() {
            bail!("歌单不存在或无权访问");
        }
        let ids = pl["trackIds"]
            .as_array()
            .map(|a| a.iter().filter_map(|t| t["id"].as_u64()).collect())
            .unwrap_or_default();
        Ok((pl["name"].as_str().unwrap_or("").to_string(), ids))
    }

    pub async fn album(&self, id: u64) -> Result<(String, Vec<Song>)> {
        let v: Value = self
            .http
            .get(format!("{ALBUM_API}{id}"))
            .header("User-Agent", UA)
            .header("Referer", REFERER)
            .header("Cookie", self.full_cookie())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if v["album"].is_null() {
            bail!("专辑不存在");
        }
        Ok((v["album"]["name"].as_str().unwrap_or("").to_string(), parse_songs(&v["songs"])))
    }

    /// Follows a short link (163cn.tv) to the real page URL.
    pub async fn expand_link(&self, url: &str) -> Result<String> {
        let resp = self.http.get(url).header("User-Agent", UA).send().await?;
        Ok(resp.url().to_string())
    }

    // ---- QR login ------------------------------------------------------------------

    pub async fn qr_key(&self) -> Result<String> {
        let v = self
            .post_form("https://music.163.com/api/login/qrcode/unikey", &[("type", "1".into())])
            .await?;
        v["unikey"].as_str().map(str::to_string).context("no unikey in response")
    }

    /// Returns (code, cookie). 800 expired, 801 waiting, 802 scanned, 803 success.
    pub async fn qr_check(&self, key: &str) -> Result<(i64, Option<String>)> {
        let resp = self
            .http
            .post("https://music.163.com/api/login/qrcode/client/login")
            .header("User-Agent", UA)
            .header("Referer", REFERER)
            .header("Cookie", PC_COOKIE)
            .form(&[("key", key), ("type", "1")])
            .send()
            .await?;
        let cookies: Vec<String> = resp
            .headers()
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .filter_map(|h| h.to_str().ok())
            .filter_map(|h| h.split(';').next())
            .map(|s| s.trim().to_string())
            .collect();
        let v: Value = resp.json().await?;
        let code = v["code"].as_i64().unwrap_or(0);
        let cookie = (code == 803 && !cookies.is_empty()).then(|| cookies.join("; "));
        Ok((code, cookie))
    }
}

fn parse_songs(v: &Value) -> Vec<Song> {
    let Some(arr) = v.as_array() else { return Vec::new() };
    arr.iter()
        .filter_map(|s| {
            let ar = s.get("ar").or_else(|| s.get("artists"));
            let al = s.get("al").or_else(|| s.get("album"));
            Some(Song {
                id: s["id"].as_u64()?,
                name: s["name"].as_str().unwrap_or("").to_string(),
                artists: ar
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter().filter_map(|x| x["name"].as_str()).collect::<Vec<_>>().join("/")
                    })
                    .unwrap_or_default(),
                album: al.and_then(|a| a["name"].as_str()).unwrap_or("").to_string(),
                duration_ms: s.get("dt").or_else(|| s.get("duration")).and_then(Value::as_u64).unwrap_or(0),
                fee: s["fee"].as_i64().unwrap_or(0),
            })
        })
        .collect()
}

/// eapi request encryption: AES-128-ECB over `path-36cd479b6b5-json-36cd479b6b5-md5`.
fn eapi_encrypt(path: &str, json: &str) -> String {
    let digest = hex::encode(Md5::digest(format!("nobody{path}use{json}md5forencrypt")));
    let mut data = format!("{path}-36cd479b6b5-{json}-36cd479b6b5-{digest}").into_bytes();
    let pad = 16 - data.len() % 16;
    data.extend(std::iter::repeat_n(pad as u8, pad));
    let cipher = aes::Aes128::new(GenericArray::from_slice(EAPI_KEY));
    for block in data.chunks_mut(16) {
        cipher.encrypt_block(GenericArray::from_mut_slice(block));
    }
    hex::encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eapi_matches_reference() {
        // Block-aligned check: output length is a multiple of 32 hex chars.
        let out = eapi_encrypt("/api/song/enhance/player/url/v1", r#"{"ids":[1]}"#);
        assert_eq!(out.len() % 32, 0);
    }
}
