############################################
# 1) Builder
############################################
ARG GO_VERSION=1.24
ARG TARGETOS=linux
ARG TARGETARCH=amd64

FROM --platform=linux/amd64 golang:${GO_VERSION}-alpine AS builder

WORKDIR /src
# инструменты (git для go mod, сертификаты не обязательны на этапе сборки)
RUN apk add --no-cache git

# 1.1 cache deps
COPY go.mod go.sum ./

# ВАЖНО: из-за replace -> ./pbx переносим локальный пакет раньше
COPY pbx/ ./pbx/
RUN --mount=type=cache,target=/go/pkg/mod \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go mod download

# 1.2 исходники
COPY . .

# 1.3 сборка
# 1.3 сборка — явный пакет
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/router .

############################################
# 2) Runner (alpine, non-root, CA certs)
############################################
FROM alpine:3.20

# CA для TLS к Kafka + часовые пояса
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S app && adduser -S app -G app

WORKDIR /app

# бинарь
COPY --from=builder /out/router /usr/local/bin/router

# опционально: положим пример env рядом (на проде не копируй .env в образ)
COPY .env.example /app/.env.example

# сетевые порты: gRPC плагина и health
EXPOSE 7911 8090

# безопасность
USER app

# если рядом будет файл ".env", наша программа его подхватит через godotenv
ENTRYPOINT ["/usr/local/bin/router"]
