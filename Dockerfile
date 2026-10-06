FROM rust:1.93.1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 65532 wan-recover \
    && useradd -u 65532 -g 65532 -M -d /nonexistent -s /usr/sbin/nologin wan-recover
COPY --from=build /src/target/release/wan-recover /usr/local/bin/wan-recover
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/wan-recover"]
CMD ["/etc/wan-recover/config.toml"]
