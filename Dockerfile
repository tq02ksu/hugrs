FROM rust:slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash hugrs \
    && mkdir -p /data && chown hugrs:hugrs /data
COPY --from=builder /app/target/release/hugrs /usr/local/bin/
EXPOSE 3000
VOLUME /data
USER hugrs
ENV HUGRS_DB_PATH=/data/hugrs.db \
    HUGRS_LOCAL_ROOT=/data/trunks
ENTRYPOINT ["hugrs"]
CMD ["serve"]
