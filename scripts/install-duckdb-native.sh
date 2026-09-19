#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

version='v2.0.0-alpha41533'
commit='10de957379'
archive_sha='934507594428409754d3e76d162ee5763df3df53b570bb770b2b38082c3795da'
lib_sha='69bdd44e0d2426e7ba44ed14644b54d8bd99a9703e5cbb4aec3f8beef40f817d'
duckdb_h_sha='9ff8bfb6f88ed1be4f845399ed7ff35980a65e011848d027cef88467fe9e0886'
duckdb_v2_h_sha='62ad0df66b9f4193a657540d2429ba23cb32c41c5c98ea9f3f4e7a3b64e30544'
extension_h_sha='a67312ee4c88b1c3ede25af0a74c253ce39354efe1c3beaf576c441451f84a36'
extension_v2_h_sha='45b19dd8588fd2cfc32dabe72fbe83f5926734bedeead7128a31a57575981807'

if [[ "$(uname -s)-$(uname -m)" != 'Linux-x86_64' ]]; then
  echo 'Pinned native probe artifact supports only Linux x86_64 (x86-64-v3).' >&2
  exit 1
fi

sha256() {
  sha256sum "$1" | cut -d ' ' -f 1
}

verify_payload() {
  local dir=$1
  [[ "$(sha256 "$dir/libduckdb.so")" == "$lib_sha" ]]
  [[ "$(sha256 "$dir/duckdb.h")" == "$duckdb_h_sha" ]]
  [[ "$(sha256 "$dir/duckdb_v2.h")" == "$duckdb_v2_h_sha" ]]
  [[ "$(sha256 "$dir/duckdb_extension.h")" == "$extension_h_sha" ]]
  [[ "$(sha256 "$dir/duckdb_extension_v2.h")" == "$extension_v2_h_sha" ]]
  [[ "$(find "$dir" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)" == \
$'duckdb.h\nduckdb_extension.h\nduckdb_extension_v2.h\nduckdb_v2.h\nlibduckdb.so' ]]
}

destination=".tools/duckdb-native-${version}-linux-amd64"
print_instructions() {
  cat <<EOF
Railway Stage A proof commands:
  native_dir="$PWD/$destination"
  probe_bin="$PWD/.tools/native-scan-probe-stage-a"
  gcc -std=c11 -O2 -Wall -Wextra -Werror -I"\$native_dir" probes/native_scan.c -L"\$native_dir" -Wl,-rpath,"\$native_dir" -lduckdb -o "\$probe_bin"
  "\$probe_bin"
EOF
}
mkdir -p .tools
if [[ -d "$destination" ]]; then
  if verify_payload "$destination"; then
    echo "Already installed: $PWD/$destination"
    print_instructions
    exit 0
  fi
  echo "Existing native DuckDB directory failed verification: $PWD/$destination" >&2
  exit 1
fi

temp=$(mktemp -d '.tools/.duckdb-native-install.XXXXXX')
trap 'rm -rf "$temp"' EXIT
archive="$temp/duckdb-shared-libs-linux-amd64.tar.gz"
url="https://duckdb-staging.duckdb.org/$commit/$version/duckdb/duckdb/github_release/duckdb-shared-libs-linux-amd64.tar.gz"

curl --fail --location --silent --show-error --max-time 120 --max-filesize 33554432 "$url" -o "$archive"
[[ "$(sha256 "$archive")" == "$archive_sha" ]] || {
  echo 'DuckDB native archive checksum mismatch' >&2
  exit 1
}

expected=$'duckdb.h\nduckdb_extension.h\nduckdb_extension_v2.h\nduckdb_v2.h\nlibduckdb.so'
actual=$(tar -tzf "$archive" | LC_ALL=C sort)
[[ "$actual" == "$expected" ]] || {
  echo 'Unexpected DuckDB native archive layout' >&2
  printf '%s\n' "$actual" >&2
  exit 1
}

mkdir "$temp/payload"
tar -xzf "$archive" -C "$temp/payload"
verify_payload "$temp/payload" || {
  echo 'DuckDB native payload checksum or layout mismatch' >&2
  exit 1
}
mv "$temp/payload" "$destination"

echo "Installed: $PWD/$destination"
print_instructions
