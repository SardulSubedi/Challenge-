FROM rust:1-slim-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY sbe_schemas ./sbe_schemas
COPY src ./src
RUN cargo build --release --locked --bins

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home collector
COPY --from=build /app/target/release/collector /usr/local/bin/collector
COPY --from=build /app/target/release/inspect-sbe /usr/local/bin/inspect-sbe
COPY --from=build /app/target/release/latency-report /usr/local/bin/latency-report
COPY --from=build /app/target/release/feed-lab /usr/local/bin/feed-lab
USER collector
WORKDIR /home/collector
STOPSIGNAL SIGINT
ENTRYPOINT ["collector"]
