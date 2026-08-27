# 1단계: Rust 빌더 환경
FROM rust:1.80-alpine AS builder
RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# 릴리즈 최적화 빌드 (경량 정적 바이너리)
RUN cargo build --release

# 2단계: 초경량 런타임 환경 (15MB 미만)
FROM alpine:3.20
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/target/release/server-rs /app/server-rs

EXPOSE 8791
ENV PORT=8791
ENV RUST_LOG=info

CMD ["/app/server-rs"]
