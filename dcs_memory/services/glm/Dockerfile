# Используем официальный образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /service

# Установка системных зависимостей, если они понадобятся в будущем
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Создаем виртуальное окружение
ENV VIRTUAL_ENV=/service/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Устанавливаем зависимости для генерации gRPC кода и основные зависимости
# Копируем сначала только requirements файлы для кэширования этого слоя Docker
COPY ../../../requirements.txt /tmp/common_requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r /tmp/common_requirements.txt && \
    pip install --no-cache-dir -r requirements.txt

# Устанавливаем grpc_health_probe для health checks
# https://github.com/grpc-ecosystem/grpc-health-probe
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.40 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-${TARGETOS}-${TARGETARCH} && \
    chmod +x /bin/grpc_health_probe

# Копируем исходный код сервиса
COPY app/ app/

# Устанавливаем переменные окружения для конфигурации сервиса
# QDRANT_HOST будет именем сервиса qdrant в docker-compose сети
ENV QDRANT_HOST="qdrant"
ENV QDRANT_PORT="6333"
# Будет создан в app/ внутри контейнера
ENV SQLITE_DB_FILENAME="glm_metadata.sqlite3"
ENV GRPC_LISTEN_ADDRESS="[::]:50051"
# DEFAULT_VECTOR_SIZE и DEFAULT_PAGE_SIZE будут взяты из main.py по умолчанию

# Открываем порт, на котором будет слушать сервис
EXPOSE 50051

# Команда для запуска приложения
CMD ["python", "app/main.py"]
