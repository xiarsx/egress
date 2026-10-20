# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# WebUI build stage: produces webui/dist, which the Go binary embeds at
# /ui/ (internal/adapters/webui). No dist → the binary serves a 503 stub.
# ---------------------------------------------------------------------------
FROM node:22-bookworm AS webui

WORKDIR /web
COPY webui/package.json webui/package-lock.json* ./
RUN npm install --no-audit --no-fund
COPY webui/ ./
RUN node node_modules/vite/bin/vite.js build

# ---------------------------------------------------------------------------
# Build stage. NOTE: go.mod requires go >= 1.26, so the builder must be at
# least that (golang:1.25 cannot build this module without downloading a
# newer toolchain).
# ---------------------------------------------------------------------------
FROM golang:1.26-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO is off: the sqlite driver (modernc.org/sqlite) is pure Go, so the
# result is a fully static binary with no libc dependency.
# with_quic/with_utls compile in hysteria2/tuic/hysteria and VLESS
# REALITY support (sing-box build tags, see spec/resources/source.md).
COPY --from=webui /web/dist ./webui/dist
RUN CGO_ENABLED=0 go build -tags "with_quic with_utls" -trimpath -ldflags="-s -w" -o /out/egress ./cmd/egress

# ---------------------------------------------------------------------------
# Runtime stage: distroless static (no shell, no package manager).
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/egress /egress

# Data directories (override with EGRESS_STATE_DIR / EGRESS_CACHE_DIR /
# EGRESS_LOG_DIR). The nonroot user needs write access.
VOLUME ["/var/lib/egress", "/var/cache/egress", "/var/log/egress"]

# 2260 default endpoint: http-forward + socks5 + reverse + api + ui
EXPOSE 2260

ENV EGRESS_LISTEN_ADDRESS=0.0.0.0 \
    EGRESS_PORT=2260 \
    EGRESS_STATE_DIR=/var/lib/egress \
    EGRESS_CACHE_DIR=/var/cache/egress \
    EGRESS_LOG_DIR=/var/log/egress

ENTRYPOINT ["/egress"]
