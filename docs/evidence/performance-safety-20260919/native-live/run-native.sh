#!/bin/sh
set -eu
root=/results/native-live-0919
cd /results/review-0918-next
test "$(id -u)" = 10001
test "$(id -g)" = 10001
sha256sum -c "$root/driver.sha256" > "$root/source-before.log"
for path in "$root/old-b128-after.json" "$root/old-b1-after.json" "$root/native01_b128_01.json" "$root/finished.exit"; do test ! -e "$path"; done
id > "$root/identity.txt"
python --version >> "$root/identity.txt"
date -u +%FT%TZ >> "$root/identity.txt"
finish() { rc=$?; printf '%s\n' "$rc" > "$root/finished.exit"; sha256sum -c "$root/driver.sha256" > "$root/source-after.log"; }
trap finish EXIT
python verify_exact.py --report /results/io13_b128_01.json --output "$root/old-b128-after.json" --max-seconds 300
python verify_exact.py --report /results/io13_b1_01.json --output "$root/old-b1-after.json" --max-seconds 300
set +e
python benchmark.py --run-id native01_b128_01 --rows 8192 --batch 128 --writers 4 --query-samples 50 --mixed-seconds 60 --rate 512 --mixed-read-interval 1 --mixed-readers 1 --drain-seconds 60 --max-seconds 900 --require-rebuilt --output "$root/native01_b128_01.json"
bench_rc=$?
set -e
printf '%s\n' "$bench_rc" > "$root/benchmark.exit"
if test -f "$root/native01_b128_01.json"; then
  set +e
  python verify_exact.py --report "$root/native01_b128_01.json" --output "$root/native01_b128_01.exact.json" --max-seconds 300
  exact_rc=$?
  set -e
  printf '%s\n' "$exact_rc" > "$root/exact.exit"
else
  exact_rc=1
fi
if test "$bench_rc" -ne 0; then exit "$bench_rc"; fi
exit "$exact_rc"
