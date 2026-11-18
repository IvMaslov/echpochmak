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

# catalogctl ships next to the server: it reads the same configuration, so an
# operator runs it inside a catalog container.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for cmd in catalog-server catalogctl; do \
      CGO_ENABLED=0 go build -trimpath \
        -ldflags "-s -w \
          -X echpochmak/internal/version.version=${VERSION} \
          -X echpochmak/internal/version.commit=${COMMIT} \
          -X echpochmak/internal/version.buildDate=${BUILD_DATE}" \
        -o /out/$cmd ./cmd/$cmd; \
    done

# Runtime stage: no shell, no package manager, non-root user.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3

COPY --from=build /out/catalog-server /catalog-server
COPY --from=build /out/catalogctl /catalogctl

USER nonroot:nonroot
EXPOSE 8181

ENTRYPOINT ["/catalog-server"]
