#!/usr/bin/env bash
# Install or update ringo-music on Ubuntu. Re-runnable; an existing config.toml is kept.
#
#   sudo bash install.sh        # binary, config.example.toml and service next to this script
#
# On first install the config is filled from a TS3AudioBot bot.toml (server password,
# channel, identity — so the bot keeps its server groups) and from the environment:
#   IDENTITY_FROM  bot.toml to read (default /opt/ts3audiobot/bots/default/bot.toml)
#   TS_PASSWORD, CHANNEL_ID  override what bot.toml says
# TS3AudioBot is disabled if present.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run with sudo"; exit 1; }

HERE=$(cd "$(dirname "$0")" && pwd)
DIR=/opt/ringo-music
BIN=$HERE/ringo-music
EXAMPLE=$HERE/config.example.toml
UNIT=$HERE/ringo-music.service
IDENTITY_FROM=${IDENTITY_FROM:-/opt/ts3audiobot/bots/default/bot.toml}

for f in "$BIN" "$EXAMPLE" "$UNIT"; do [ -f "$f" ] || { echo "missing $f"; exit 1; }; done
command -v ffmpeg >/dev/null || { apt-get update -qq; DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ffmpeg; }
if ! command -v yt-dlp >/dev/null; then
    curl --connect-timeout 15 --max-time 300 -fsSL -o /usr/local/bin/yt-dlp \
        https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux \
        && chmod 755 /usr/local/bin/yt-dlp || echo "[!] yt-dlp download failed; Bilibili/other links won't work"
fi

id ringomusic &>/dev/null || useradd --system --home-dir $DIR --shell /usr/sbin/nologin ringomusic
install -d -m 750 $DIR
install -m 755 "$BIN" $DIR/ringo-music.new && mv -f $DIR/ringo-music.new $DIR/ringo-music
install -m 644 "$EXAMPLE" $DIR/config.example.toml

if [ ! -f $DIR/config.toml ]; then
    python3 - "$EXAMPLE" "$IDENTITY_FROM" > $DIR/config.toml <<'EOF'
import os, re, sys, tomllib
text = open(sys.argv[1], encoding="utf-8").read()
vals = {}
try:
    with open(sys.argv[2], "rb") as f:
        c = tomllib.load(f).get("connect", {})
    pw = c.get("server_password", {}).get("pw", "")
    if pw and "__" not in pw:  # skip unrendered templates
        vals["password"] = pw
    m = re.fullmatch(r"/(\d+)", c.get("channel", ""))
    if m:
        vals["channel_id"] = int(m.group(1))
    ident = c.get("identity", {})
    if ident.get("key"):
        vals["identity_key"] = ident["key"]
        vals["identity_offset"] = int(ident.get("offset", 0))
except FileNotFoundError:
    pass
if os.environ.get("TS_PASSWORD"):
    vals["password"] = os.environ["TS_PASSWORD"]
if os.environ.get("CHANNEL_ID"):
    vals["channel_id"] = int(os.environ["CHANNEL_ID"])
for k, v in vals.items():
    lit = str(v) if isinstance(v, int) else '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
    text = re.sub(rf"(?m)^{k} = [^#\n]*?(\s*#.*)?$", lambda m: f"{k} = {lit}" + ("  " + m.group(1).strip() if m.group(1) else ""), text, count=1)
sys.stdout.write(text)
EOF
fi
chown -R ringomusic:ringomusic $DIR
chmod 600 $DIR/config.toml

install -m 644 "$UNIT" /etc/systemd/system/ringo-music.service
systemctl daemon-reload
if [ -f /etc/systemd/system/ts3audiobot.service ]; then
    systemctl disable --now ts3audiobot.service 2>/dev/null || true
fi
systemctl enable ringo-music >/dev/null 2>&1
systemctl restart ringo-music
sleep 3
systemctl --no-pager --lines=0 status ringo-music | head -3
echo "logs: journalctl -u ringo-music -f    Netease login: sudo -u ringomusic $DIR/ringo-music -c $DIR/config.toml login"
