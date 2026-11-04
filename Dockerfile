# ==========================================
# Stage 1: Build the Go application binary
# ==========================================
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -extldflags '-static'" \
    -o /build/bin/quicknotify \
    ./cmd/quicknotify

# ==========================================
# Stage 2: Minimal, secure runtime image
# ==========================================
FROM alpine:3.21 AS runner

RUN apk --no-cache add ca-certificates tzdata wget \
    && addgroup -S appgroup -g 10001 \
    && adduser -S appuser -u 10001 -G appgroup

WORKDIR /app

# Copy binary and web dashboard only. Config comes from .env at runtime.
COPY --from=builder /build/bin/quicknotify /app/quicknotify
COPY --from=builder /build/web /app/web
COPY --from=builder /build/admin.html /app/admin.html

RUN chown -R appuser:appgroup /app

USER appuser:appgroup

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
    CMD ["wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8080/healthz"]

# Config is injected via environment variables / .env file (env_file in docker-compose.yml).
# No -config flag needed; LoadConfig reads from OS env automatically.
ENTRYPOINT ["/app/quicknotify"]
