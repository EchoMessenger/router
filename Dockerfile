############################################
# 1) Builder
############################################
ARG GO_VERSION=1.24
ARG TARGETOS=linux
ARG TARGETARCH=amd64

FROM --platform=linux/amd64 golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY go.mod go.sum ./

COPY pbx/ ./pbx/
RUN --mount=type=cache,target=/go/pkg/mod \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go mod download

COPY . .

ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/router .

FROM alpine:3.20

# CA для TLS к Kafka + часовые пояса
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S app && adduser -S app -G app

WORKDIR /app

COPY --from=builder /out/router /usr/local/bin/router

COPY .env.example /app/.env.example

# сетевые порты: gRPC плагина и health
EXPOSE 7911 8090

# безопасность
USER app

ENTRYPOINT ["/usr/local/bin/router"]
