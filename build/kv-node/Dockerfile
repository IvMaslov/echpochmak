# syntax=docker/dockerfile:1

# Build stage: the Go version matches go.mod.
FROM golang:1.26.2-trixie AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for cmd in kv-node kvctl kv-backup; do \
      CGO_ENABLED=0 go build -trimpath \
        -ldflags "-s -w \
          -X echpochmak/internal/version.version=${VERSION} \
          -X echpochmak/internal/version.commit=${COMMIT} \
          -X echpochmak/internal/version.buildDate=${BUILD_DATE}" \
        -o /out/$cmd ./cmd/$cmd; \
    done

# The image also carries the operator tools: kvctl, and kv-backup, which runs
# as its own service with this image and another entrypoint.

# The data directory, owned by the runtime user. A new named volume copies
# this ownership, so the non-root node can write to it.
RUN mkdir -p /out/data

# Runtime stage: no shell, no package manager, non-root user.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3

COPY --from=build /out/kv-node /kv-node
COPY --from=build /out/kvctl /kvctl
COPY --from=build /out/kv-backup /kv-backup
COPY --from=build --chown=nonroot:nonroot /out/data /data

USER nonroot:nonroot
EXPOSE 2379 2380 2381
VOLUME ["/data"]

ENTRYPOINT ["/kv-node"]
