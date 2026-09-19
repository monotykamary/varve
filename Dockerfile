FROM rust:1.98.1-slim-bookworm@sha256:ebd900bae66fd508b466cef82d64a83a5fb34682e4c8b2797a42908bddc95a57 AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake pkg-config ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN scripts/install-duckdb.sh && scripts/install-duckdb-native.sh
RUN mkdir -p /out && find . -type f ! -path './.tools/*' ! -path './target/*' -print0 | LC_ALL=C sort -z | xargs -0 sha256sum > /out/source-manifest.sha256
RUN cargo build --locked --release --bin varve --example cloud_probe && mkdir -p /out && cp target/release/varve /out/varve && cp target/release/examples/cloud_probe /out/varve-cloud-probe && strip /out/varve /out/varve-cloud-probe

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 curl gosu util-linux python3 && rm -rf /var/lib/apt/lists/* && groupadd --gid 10001 varve && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin varve
COPY --from=build /out/source-manifest.sha256 /usr/share/doc/varve/source-manifest.sha256
COPY --from=build /out/varve /usr/local/bin/varve
COPY --from=build /out/varve-cloud-probe /usr/local/bin/varve-cloud-probe
COPY --from=build /src/.tools/duckdb /usr/local/bin/duckdb
COPY --from=build /src/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so /usr/local/lib/varve/libduckdb.so
COPY scripts/container-entrypoint.sh /usr/local/bin/varve-entrypoint
COPY scripts/stress.py scripts/verify-stress.py /app/scripts/
COPY config/railway.json /app/config.json
COPY config/railway-rebuilt.json /app/config-rebuilt.json
COPY config/railway-rebuild-benchmark.json /app/config-rebuild-benchmark.json
RUN chmod 755 /usr/local/bin/varve-entrypoint && mkdir -p /data
COPY LICENSE /usr/share/doc/varve/LICENSE
COPY licenses/duckdb.txt /usr/share/doc/varve/licenses/duckdb.txt
WORKDIR /app
ENV VARVE_DATA_DIR=/data/varve VARVE_CONFIG=/app/config.json VARVE_REQUIRE_VOLUME=1
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/varve-entrypoint"]
