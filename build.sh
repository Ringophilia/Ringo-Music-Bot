#!/usr/bin/env bash
# Build the static Linux binary and assemble dist/ (what install.sh deploys).
#   bash build.sh                               # build into dist/
#   bash build.sh ../my-deploy-kit               # also copy dist/ into another directory
#   FAST=1 bash build.sh                         # quick build for testing (bigger, less optimised)
# Needs: rustup (msvc host toolchain + x86_64-unknown-linux-musl target), pip install ziglang cargo-zigbuild
set -euo pipefail
cd "$(dirname "$0")"
ZIG_DIR=$(python -c "import ziglang, os; print(os.path.dirname(ziglang.__file__))")
command -v cygpath >/dev/null && ZIG_DIR=$(cygpath "$ZIG_DIR")
PROFILE=release
[ -n "${FAST:-}" ] && PROFILE=fast
PATH="$ZIG_DIR:$PATH" cargo +stable-x86_64-pc-windows-msvc zigbuild --profile "$PROFILE" --target x86_64-unknown-linux-musl
mkdir -p dist
cp "target/x86_64-unknown-linux-musl/$PROFILE/ringo-music" config.example.toml deploy/install.sh deploy/ringo-music.service dist/
ls -la dist
if [ -n "${1:-}" ]; then
    mkdir -p "$1"
    cp dist/* "$1"/
    echo "copied to $1"
fi
