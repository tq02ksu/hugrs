FROM rust:slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/hugrs /usr/local/bin/
EXPOSE 3000
VOLUME /data
ENV HUGRS_DB_PATH=/data/hugrs.db \
    HUGRS_LOCAL_ROOT=/data/trunks
ENTRYPOINT ["hugrs"]
CMD ["serve"]
