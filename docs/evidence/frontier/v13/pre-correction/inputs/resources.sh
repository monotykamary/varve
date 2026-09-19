set -eu
printf 'at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
printf 'region=%s\n' "$RAILWAY_REPLICA_REGION"
printf 'boot_id='; cat /proc/sys/kernel/random/boot_id
printf 'process_start_ticks='; awk '{print $22}' /proc/1/stat
printf 'uid='; awk '/^Uid:/{print $2}' /proc/1/status
for name in cpu.max memory.max memory.peak memory.events cpu.stat; do
  printf '%s=' "$name"
  tr '\n' ';' < "/sys/fs/cgroup/$name"
  printf '\n'
done
