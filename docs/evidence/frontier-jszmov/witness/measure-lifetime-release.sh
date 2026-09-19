#!/bin/sh
set -eu
cd /Users/monotykamary/VCS/working-remote/open-source/varve
R=/tmp/varve-frontier-gate.JsZmOV
trap 'code=$?; printf "%s\n" "$code" > "$R/measure-lifetime-release.exit"' EXIT
waited=0
while [ ! -f "$R/lifetime-release-ready.exit" ]; do
 if [ -f "$R/qualify-lifetime.exit" ]; then
  test "$(cat "$R/qualify-lifetime.exit")" = 0 || exit 1
 fi
 test "$waited" -lt 1200 || exit 124
 sleep 5
 waited=$((waited+5))
done
test "$(cat "$R/lifetime-release-ready.exit")" = 0
node "$R/freeze-lifetime-source.mjs" --verify
shasum -a 256 -c "$R/lifetime-binary.sha256"
H=$(shasum -a 256 "$R/lifetime-varve" | cut -d ' ' -f 1)
run() {
 name=$1; rows=$2; rate=$3; trace=$4
 set +e
 "$R/venv/bin/python" benchmarks/timescale/frontier.py --binary "$R/lifetime-varve" --expected-binary-sha256 "$H" --profile "$R/profile.json" --output "$R/$name" --rows "$rows" --batch 1000 --writers 4 --rate "$rate" --seconds 30 --read-interval .1 --read-mode fresh --trace-capacity "$trace" --max-seconds 240
 code=$?
 printf '%s\n' "$code" > "$R/$name.exit"
 set -e
 node "$R/freeze-lifetime-source.mjs" --verify
 test "$code" -eq 0 || exit "$code"
}
# The first point must cross the earlier final-oracle failure without drops.
run lifetime-release-fresh-large-01 307200 5000 512
# This is the earlier failing release workload, now untraced. Not a Timescale comparison.
run lifetime-release-fresh-load-01 102400 15000 0
