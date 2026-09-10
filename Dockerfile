# syntax=docker/dockerfile:1

# ============================================================================
# Build stage: compile the example binary with module caching.
# The bedrock module itself is consumed as a Go library; this image
# packages the runnable lifecycle example plus its data volume layout.
# ============================================================================
FROM golang:1.23-alpine AS build

WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

# Compile with the module sources.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w" \
    -o /out/bedrock-example ./cmd/example

# ============================================================================
# Runtime stage: minimal distroless-style runtime, non-root, volume for data.
# ============================================================================
FROM alpine:3.20 AS runtime

# ca-certificates: not required for local storage but conventional for
# images that later grow remote backup sinks (S3/GCS upload hooks).
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S kv && adduser -S kv -G kv

WORKDIR /data
VOLUME ["/data"]

COPY --from=build /out/bedrock-example /usr/local/bin/bedrock-example

USER kv

# The example performs one lifecycle pass and exits; under a supervisor
# (s6/tini) restart policies turn it into a self-healing demo workload.
# Production services built on bedrock should expose liveness/readiness
# via Store.HealthCheck wired into their HTTP mux — see docs/deployment.md.
ENTRYPOINT ["bedrock-example"]
CMD ["/data"]
