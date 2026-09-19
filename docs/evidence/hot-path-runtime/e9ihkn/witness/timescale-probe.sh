set -eu
printf 'RAILWAY_PROJECT_ID=%s\nRAILWAY_ENVIRONMENT_ID=%s\nRAILWAY_SERVICE_ID=%s\nRAILWAY_DEPLOYMENT_ID=%s\n' "$RAILWAY_PROJECT_ID" "$RAILWAY_ENVIRONMENT_ID" "$RAILWAY_SERVICE_ID" "$RAILWAY_DEPLOYMENT_ID"
printf 'PGDATA=%s\nPG_VERSION=' "$PGDATA"
cat "$PGDATA/PG_VERSION"
pid=$(head -n 1 "$PGDATA/postmaster.pid")
test "$pid" -gt 0
printf 'POSTMASTER_PID=%s\n' "$pid"
awk '/^Uid:/{printf "POSTMASTER_UID=%s\n",$2}' "/proc/$pid/status"
printf 'POSTMASTER_COMM='
cat "/proc/$pid/comm"
for name in cpu.max memory.max memory.current; do printf '%s=' "$name"; cat "/sys/fs/cgroup/$name"; done
printf 'VOLUME_MOUNT='
awk '$5=="/var/lib/postgresql/data"{print $5}' /proc/self/mountinfo
