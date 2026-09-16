#!/bin/sh
set -eu
umask 077
if [ "${VARVE_REQUIRE_VOLUME:-1}" = 1 ] && ! mountpoint -q /data; then
  echo 'Refusing ephemeral database storage: mount a persistent volume at /data.' >&2
  exit 1
fi
mkdir -p /data/varve /data/probes
if [ "$(id -u)" = 0 ]; then
  chown 10001:10001 /data/varve /data/probes
fi
if [ "$#" -gt 0 ]; then
  if [ "$(id -u)" = 0 ]; then exec gosu varve "$@"; else exec "$@"; fi
fi
if [ -z "${VARVE_API_TOKEN:-}" ]; then
  echo 'VARVE_API_TOKEN must be configured before exposing the service.' >&2
  exit 1
fi
set -- varve --data "${VARVE_DATA_DIR:-/data/varve}" --config "${VARVE_CONFIG:-/app/config.json}"
if [ "${VARVE_USE_S3:-false}" = true ]; then set -- "$@" --s3; fi
set -- "$@" serve --bind 0.0.0.0 --port "${PORT:-8080}" --allow-remote
if [ "$(id -u)" = 0 ]; then exec gosu varve "$@"; else exec "$@"; fi
