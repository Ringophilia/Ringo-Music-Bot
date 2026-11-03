<div align="center">

# 🎵 Ringo-Music-Bot

**一个只有 6 MB、常驻约 7 MB 内存的 TeamSpeak 3 点歌机**

在频道里发 `!点歌 黄昏泣き`，歌就开始放了。

[![Rust](https://img.shields.io/badge/Rust-2021-orange?logo=rust)](https://www.rust-lang.org/)
[![Platform](https://img.shields.io/badge/platform-Linux%20x86__64-blue?logo=linux&logoColor=white)](#-安装)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

[功能](#-功能) · [命令](#-命令) · [安装](#-安装) · [配置](#-配置) · [省带宽](#-省带宽的设计) · [从源码构建](#-从源码构建) · [免责声明](#%EF%B8%8F-免责声明)

</div>

---

Ringo-Music-Bot 用 Rust 写成，目标是替代 TS3AudioBot，程序名是 `ringo-music`。它直接以 TS3 客户端的身份连上服务器，从网易云音乐搜歌和取音源，也能放 B 站等 yt-dlp 支持的网站和音频直链，边下边放。

## ✨ 功能

- **轻**：静态链接的 Linux 程序，只有 6 MB，常驻约 7 MB 内存。播放时 ffmpeg 另外占约 2% CPU；3 个点歌机同时放 2 路歌，整个服务约 32 MB。
- **依赖少**：不需要 .NET、OpenSSL 或 libopus。解码和 Opus 编码交给系统的 `ffmpeg`，TS3 协议用 [tsclientlib](https://github.com/ReSpeak/tsclientlib)。
- **点歌方式多**：关键词、网易云单曲 / 歌单 / 专辑链接（含 `163cn.tv` 短链和分享文字）、B 站等 yt-dlp 支持的链接、音频直链。
- **队列完整**：插播、置顶、删除，顺序 / 单曲 / 循环 / 随机四种模式。随机模式下每首都放过一遍才会重复。
- **多频道**：可以把点歌机叫到自己的频道；原来那边还有人在听时，会开一个临时点歌机过来，它有自己的队列。
- **省资源**：频道里没人就停止发送；放过的歌缓存在本地；下载跟着播放速度走。详见[省带宽的设计](#-省带宽的设计)。
- **不怕重启**：队列、播放位置、音量和模式都会保存，重启后接着放。
- **扫码登录**：在终端里扫码登录网易云，换 Cookie 不用重启。

## 💬 命令

命令以 `!` 或 `！` 开头，在点歌机所在的频道里发，或者私聊它。英文别名（`!play` `!next` `!help` 等）也能用。

| 命令 | 作用 |
|---|---|
| `!点歌 黄昏泣き` · `!p 黄昏泣き` | 搜网易云，把第一首加入队列 |
| `!点歌 <链接>` | 单曲、歌单、专辑、B 站视频或音频直链；歌单和专辑整张加入 |
| `!搜 黄昏泣き` → `!点歌 2` | 先列出搜索结果，再按序号点 |
| `!插播 黄昏泣き` | 排到下一首 |
| `!b 黄昏泣き` | 从 B 站搜索播放 |
| `!歌单 <链接/ID/名字> [选项]` | 换成这个歌单播放，替换当前队列。选项可以组合：`第5首`、`从黄昏泣き`、`随机`、`倒序`、`循环` |
| `!搜歌单 关键词` → `!歌单 2` | 先列出歌单，再按序号播放 |
| `!切歌` `!暂停` `!继续` `!停止` | 下一首 / 暂停 / 继续 / 停止并清空队列 |
| `!列表` `!当前` `!删 3` `!置顶 3` `!清空` | 管理队列 |
| `!音量 40` `!跳 1:30` `!模式 随机` | 音量 / 跳转 / 播放模式（顺序、单曲、循环、随机） |
| `!来` `!回去` `!离开` | 叫点歌机到你的频道（有密码用 `!来 密码`）/ 主点歌机回家 / 让临时点歌机退出 |
| `!状态` `!帮助` | 登录状态、码率和听众数 / 列出全部命令 |

<details>
<summary><b>多个频道同时听歌是怎么工作的</b></summary>

<br>

TS 只允许客户端往自己所在的频道发消息，也不会把别的频道的聊天转给它，所以每个要点歌的频道里都得有一个点歌机。人在别的频道时，第一条命令要私聊点歌机，或者发在服务器聊天里：

| 情况 | 结果 |
|---|---|
| 主点歌机那边没人 | 主点歌机带着队列直接过来；你这边没人超过 `return_home_secs` 秒后，它回到原来的频道 |
| 主点歌机那边有人 | 开一个临时点歌机（「🎵 点歌机 2」「3」…）到你的频道，有自己的队列 |
| 你的频道已经有临时点歌机 | 命令直接交给它处理 |
| 点歌机数量已达 `max_bots` | 提示都在忙，点的歌排进主点歌机的队列 |
| 需要发言权限的频道 | 不会过去，私聊回复 |

临时点歌机没人或闲置 `temp_idle_secs` 秒后自动退出，队列不保存。所有点歌机共用缓存和网易云登录，每个占一个 TS 名额，身份保存为 `identity-2.json`、`identity-3.json`……

</details>

## 📦 安装

前提：一台 x86_64 Linux 服务器（在 Ubuntu 上测试过），最好和 TS 服务器在同一台机器上。

1. 从 [Releases](../../releases) 下载，或者[自己构建](#-从源码构建)，得到 `dist/` 目录：

   ```
   dist/
   ├── ringo-music            # 程序
   ├── install.sh             # 安装 / 更新脚本
   ├── config.example.toml    # 配置模板
   └── ringo-music.service    # systemd 服务
   ```

2. 上传到服务器，运行：

   ```bash
   sudo bash install.sh
   ```

   脚本会做这些事：缺 ffmpeg 和 yt-dlp 时自动安装；创建 `ringomusic` 系统用户；把程序装到 `/opt/ringo-music`；注册并启动 systemd 服务。重复运行只更新程序，已有的 `config.toml` 不会被覆盖。

   > **从 TS3AudioBot 迁移**：第一次安装时，脚本会从 `/opt/ts3audiobot/bots/default/bot.toml` 读取服务器密码、频道和身份，所以点歌机原来的服务器组和权限都不变，最后再停用 TS3AudioBot。路径不同时用 `IDENTITY_FROM=...` 指定。

3. 编辑 `/opt/ringo-music/config.toml`，然后重启服务：

   ```bash
   sudo systemctl restart ringo-music
   journalctl -u ringo-music -f      # 查看日志
   ```

### 登录网易云（可选）

不登录也能放免费歌曲，会员歌曲只有 30 秒试听。要用自己的账号登录：

```bash
sudo -u ringomusic /opt/ringo-music/ringo-music -c /opt/ringo-music/config.toml login
```

终端里会显示二维码，用网易云音乐 App 扫码确认即可。Cookie 保存在 `netease_cookie.txt`，点歌机会自动读取，不用重启。

> 请只登录你自己的账号，并遵守网易云音乐的用户协议。

## ⚙️ 配置

完整说明见 [`config.example.toml`](config.example.toml)，每一项都有中文注释。常用的几项：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `ts.address` | `127.0.0.1:9987` | TS 服务器地址，同机部署走回环地址 |
| `ts.channel_id` | `0` | 点歌机常驻的频道（0 = 默认频道） |
| `audio.bitrate_kbps` | `64` | 发给 TS 的 Opus 码率 |
| `audio.volume` / `max_volume` | `40` / `80` | 默认音量和用户能调到的上限 |
| `netease.quality` | `higher` | 网易云音源：`standard` / `higher` / `exhigh` / `lossless` |
| `cache.max_mb` | `2048` | 歌曲缓存上限 |
| `bot.max_bots` | `3` | 最多同时几个点歌机（1 = 不开临时点歌机） |
| `bot.bilibili_fallback` | `false` | 网易云没有完整音源时，按歌名搜 B 站播放 |

<details>
<summary><b>命令行</b></summary>

<br>

```text
ringo-music [-c config.toml] run                 # 运行（默认）
ringo-music [-c config.toml] login               # 扫码登录网易云
ringo-music [-c config.toml] search <关键词>      # 在终端里测试搜索
ringo-music [-c config.toml] probe <链接或关键词>  # 测试链接解析和取音源
ringo-music [-c config.toml] test-client <频道ID> <昵称> <秒数> [命令...]
                                                 # 模拟普通用户私聊点歌机，打印回复
```

</details>

## 📉 省带宽的设计

TS 服务器会给频道里**每个听众各发一份**语音，所以点歌机的上行占用约等于“每人码率 × 听众数”。在小带宽服务器上，这个数很可观。

| 做法 | 效果 |
|---|---|
| Opus 64 kbps | 每个听众实测约 88 kbps（含包头）；改成 48 kbps 约 70 kbps |
| 频道里没人，或者大家都关了扬声器，就停止发送；有人来了从断点继续 | 没人听时上下行都是 0 |
| 第一次播放时边下边放，同时存进缓存，再点直接读本地文件；超过上限先删最久没放的，磁盘剩余空间不足时不再缓存 | 热门歌只下载一次，也不会占满磁盘 |
| 下载速度跟着播放走（管道背压），另外限速 `download_kbps` | 拉歌时不会占满入口带宽、导致语音丢包 |
| 网易云默认取 192k 音源 | 反正最后都转成 64k Opus，取 320k 只是多耗流量 |

## 🛠 从源码构建

在 Windows 上交叉编译成静态 Linux 程序。需要 [rustup](https://rustup.rs/)（MSVC 工具链，加 `x86_64-unknown-linux-musl` 目标）和 `pip install ziglang cargo-zigbuild`：

```bash
bash build.sh             # 正式构建，输出到 dist/
FAST=1 bash build.sh      # 快速构建，适合测试（体积大、优化少）
```

在 Linux 上可以直接用 `cargo build --release --target x86_64-unknown-linux-musl`。

<details>
<summary><b>代码结构</b></summary>

<br>

| 文件 | 作用 |
|---|---|
| `src/main.rs` | 命令行入口 |
| `src/fleet.rs` | 管理主点歌机和临时点歌机 |
| `src/ts.rs` | TS 连接、断线重连、收发聊天、发送语音包、统计听众 |
| `src/bot.rs` | 队列、命令、播放模式、自动暂停 |
| `src/player.rs` | ffmpeg 管道、拆 Ogg/Opus 包、按帧定时发送、下载缓存 |
| `src/netease.rs` | 网易云接口：搜索、取音源地址、歌曲详情、歌单、专辑、扫码登录 |
| `src/resolve.rs` | 解析用户输入：关键词、各种链接、yt-dlp |
| `src/config.rs` | 配置 |

</details>

## ⚠️ 免责声明

- 本项目是个人学习和技术交流用的开源软件，**与网易云音乐、哔哩哔哩、TeamSpeak 没有任何关联**，也没有得到它们的授权或认可。文中提到的名称和商标归各自的权利人所有。
- 本项目**不提供、不托管、不分发任何音频内容**。所有音频都由使用者自行部署的程序，按使用者的指令，从第三方平台公开提供的接口获取；能听到什么，取决于使用者自己账号的权限。
- 本项目**不会**破解付费内容，也不会绕过 DRM 或会员限制。没有会员权限的歌曲只能播放平台提供的试听片段。
- 使用者应当遵守所在地的法律法规，以及各平台的用户协议，并自行承担使用本项目产生的一切后果。**禁止把本项目用于商业用途**，也禁止用它向公众传播受版权保护的内容。
- 歌曲版权归原作者和唱片公司所有，请支持正版。
- 如果权利人认为本项目侵犯了你的合法权益，请[提交 Issue](../../issues) 联系，我们会尽快处理。

## 📄 许可证

[MIT](LICENSE) © Ringophilia
