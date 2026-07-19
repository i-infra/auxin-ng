# auxin-ng builder: `docker build --target artifact -o dist .` drops the
# stripped release binary into dist/; the default target is a minimal
# runnable image. Toolchain version matches rust-toolchain.toml.
FROM rust:1.97-bookworm AS build
# libsignal's spqr crate validates its protobufs with protoc at build time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked \
    && strip target/release/auxin-ng

FROM scratch AS artifact
COPY --from=build /src/target/release/auxin-ng /auxin-ng

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/auxin-ng /usr/local/bin/auxin-ng
ENTRYPOINT ["auxin-ng"]
