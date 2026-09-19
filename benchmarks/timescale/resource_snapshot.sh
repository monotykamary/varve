#!/bin/sh
set -eu
role=${1:?role required}
data=${2:?data path required}
case "$role" in varve|timescale|driver) ;; *) echo 'unsupported role' >&2; exit 64;; esac
case "$data" in /*) ;; *) echo 'absolute data path required' >&2; exit 64;; esac
json_string() {
  awk 'BEGIN { printf "\"" } { if (NR > 1) printf "\\n"; for (i=1; i<=length($0); i++) { c=substr($0,i,1); if (c=="\\" || c=="\"") printf "\\%s",c; else if (c=="\t") printf "\\t"; else if (c=="\r") printf "\\r"; else printf "%s",c } } END { printf "\"" }'
}
field_file() {
  printf ',"%s":' "$1"
  if value=$(head -c 65536 "$2" 2>/dev/null); then printf %s "$value" | json_string; else printf null; fi
}
field_hash() {
  printf ',"%s":' "$1"
  if value=$(sha256sum "$2" 2>/dev/null); then printf %s "${value%% *}" | json_string; else printf null; fi
}
printf '{"role":"%s","captured_unix_seconds":%s,"uid":%s,"service_id":' "$role" "$(date +%s)" "$(id -u)"
printf %s "${RAILWAY_SERVICE_ID:-}" | json_string
printf ',"deployment_id":'
printf %s "${RAILWAY_DEPLOYMENT_ID:-}" | json_string
printf ',"region":'
printf %s "${RAILWAY_REPLICA_REGION:-}" | json_string
printf ',"data_path":'; printf %s "$data" | json_string
printf ',"disk_kib":'; du -sk "$data" | awk '{print $1}'
printf ',"df_kib":'; df -Pk "$data" | json_string
field_file boot_id /proc/sys/kernel/random/boot_id
field_file process_stat /proc/1/stat
field_file process_status /proc/1/status
field_file process_io /proc/1/io
field_file process_cgroup /proc/1/cgroup
field_file cpu_max /sys/fs/cgroup/cpu.max
field_file cpu_stat /sys/fs/cgroup/cpu.stat
field_file cpuset_effective /sys/fs/cgroup/cpuset.cpus.effective
field_file memory_max /sys/fs/cgroup/memory.max
field_file memory_current /sys/fs/cgroup/memory.current
field_file memory_peak /sys/fs/cgroup/memory.peak
field_file memory_events /sys/fs/cgroup/memory.events
field_file io_stat /sys/fs/cgroup/io.stat
if [ "$role" = varve ]; then
  field_hash binary_sha256 /usr/local/bin/varve
  field_hash source_manifest_sha256 /usr/share/doc/varve/source-manifest.sha256
  field_hash config_sha256 /app/config-rebuild-benchmark.json
  field_hash native_library_sha256 /usr/local/lib/varve/libduckdb.so
elif [ "$role" = timescale ]; then
  field_hash binary_sha256 /usr/local/bin/postgres
else
  field_hash benchmark_sha256 /app/benchmark.py
  field_hash core_sha256 /app/core.py
  field_hash requirements_sha256 /app/requirements.txt
  field_hash dockerfile_sha256 /app/Dockerfile
fi
printf '}\n'
