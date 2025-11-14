# router

Сервис для прослушивания событий в Tinode и отправки их в Kafka, для последующей обработки этих событий другими сервисами.

Пример `.env` находится в `./.env.example`

## Инструкция по запуску

Создать `.env` по примеру `./.env.example` и положить в корень проекта

### Локальный запуск

Чтобы не запекать `.env` в Dockerfile смонтируем его при запуске:
```bash
# сборка
docker buildx build --platform linux/amd64 -t ghcr.io/echomessenger/tinode-router:latest-amd64 .

# локальный запуск, .env смонтируем (чтобы не печь внутрь образа)
docker run --rm \
  --env-file ./.env \
  -p 7911:7911 -p 8090:8090 \
  EchoMessanger/router:dev
```

### Compose

Указываем env_file: .env у сервиса

