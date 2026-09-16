#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
version='v2.0.0-alpha41533'
commit='10de957379'
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) platform='linux-amd64'; checksum='22af160c96d0b97ef2108244bf80e228227325655fd7706207a2803de479f927' ;;
  Darwin-arm64) platform='osx-arm64'; checksum='47f7047a597a221a8b9f75cf87fc1e8d8d35d4fb4962fcec6917ddc6d6447552' ;;
  *) echo 'Pinned installer supports Linux x86_64 (glibc) and macOS arm64. Configure another verified DuckDB v2 executable manually.' >&2; exit 1 ;;
esac
mkdir -p .tools
if [[ -x .tools/duckdb ]] && [[ "$(.tools/duckdb -init /dev/null -csv -noheader -c 'SELECT version()')" == "$version" ]]; then
  echo "Already installed: $PWD/.tools/duckdb ($version)"
  exit 0
fi
temp=$(mktemp -d "${TMPDIR:-/tmp}/varve-duckdb.XXXXXX")
trap 'rm -rf "$temp"' EXIT
url="https://duckdb-staging.duckdb.org/$commit/$version/duckdb/duckdb/github_release/duckdb-cli-$platform.tar.gz"
curl --fail --location --silent --show-error --max-time 120 --max-filesize 67108864 "$url" -o "$temp/duckdb.tar.gz"
if command -v sha256sum >/dev/null; then
  actual=$(sha256sum "$temp/duckdb.tar.gz" | cut -d ' ' -f 1)
else
  actual=$(shasum -a 256 "$temp/duckdb.tar.gz" | cut -d ' ' -f 1)
fi
[[ "$actual" == "$checksum" ]] || { echo 'DuckDB archive checksum mismatch' >&2; exit 1; }
[[ "$(tar -tzf "$temp/duckdb.tar.gz")" == 'duckdb' ]] || { echo 'Unexpected DuckDB archive layout' >&2; exit 1; }
tar -xzf "$temp/duckdb.tar.gz" -C "$temp" duckdb
chmod +x "$temp/duckdb"
[[ "$("$temp/duckdb" -init /dev/null -csv -noheader -c 'SELECT version()')" == "$version" ]] || { echo 'DuckDB version mismatch' >&2; exit 1; }
mv "$temp/duckdb" .tools/duckdb
echo "Installed $PWD/.tools/duckdb ($version). Add $PWD/.tools to PATH."
