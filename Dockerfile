FROM rust:slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash hugrs
COPY --from=builder /app/target/release/hugrs /usr/local/bin/
ENV HUGRS_SERVER_HOST=0.0.0.0
EXPOSE 3000
USER hugrs
ENTRYPOINT ["hugrs"]
CMD ["serve"]
