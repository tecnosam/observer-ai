# syntax=docker/dockerfile:1

# ---- build ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first, so editing source does not re-download them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/observer ./cmd/observer

# ---- runtime ----
FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 10001 observer

COPY --from=build /out/observer /usr/local/bin/observer
COPY observer.yaml /etc/observer/observer.yaml

# The baked-in config points at localhost, which inside a container is the
# container itself. Set OBSERVER_NATS_URL, OBSERVER_SCORER_ENDPOINT and
# OBSERVER_EXPORT_ENDPOINT, and mount the model manifest at this path.
ENV OBSERVER_MANIFEST=/etc/observer/manifest.json

USER observer
# 8080: JSON API. 4317: OTLP gRPC. 4318: OTLP HTTP.
EXPOSE 8080 4317 4318
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["observer"]
CMD ["-config", "/etc/observer/observer.yaml"]
