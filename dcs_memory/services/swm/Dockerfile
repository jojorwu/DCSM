# Используем официальный образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /service

# Создаем виртуальное окружение
ENV VIRTUAL_ENV=/service/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Устанавливаем зависимости для генерации gRPC кода и основные зависимости
COPY ../../../requirements.txt /tmp/common_requirements.txt
RUN pip install --no-cache-dir -r /tmp/common_requirements.txt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем grpc_health_probe для health checks
# https://github.com/grpc-ecosystem/grpc-health-probe
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.40 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-${TARGETOS}-${TARGETARCH} && \
    chmod +x /bin/grpc_health_probe

# Копируем общие proto-файлы.
# Dockerfile находится в dcs_memory/services/swm/
# Нужные proto файлы в dcs_memory/common/grpc_protos/
COPY ../../common/grpc_protos /service/common_protos

# Копируем скрипт генерации gRPC и исходный код сервиса
COPY generate_grpc_code.sh .
COPY app/ app/
# Копируем также тестовый файл, если он нужен для каких-то проверок внутри Dockerfile (хотя обычно нет)
# Но лучше его исключить через .dockerignore, что и сделано.

# Адаптируем и запускаем скрипт генерации gRPC кода
# Скрипт dcs_memory/services/swm/generate_grpc_code.sh также использует PROTO_DIR="../../common/grpc_protos"
RUN sed -i 's|PROTO_DIR="../../common/grpc_protos"|PROTO_DIR="/service/common_protos"|' generate_grpc_code.sh && \
    chmod +x generate_grpc_code.sh && \
    ./generate_grpc_code.sh

# Устанавливаем переменные окружения для конфигурации сервиса
# Имя сервиса GLM в сети docker-compose
ENV GLM_SERVICE_ADDRESS="glm:50051"
ENV SWM_GRPC_LISTEN_ADDRESS="[::]:50053"
# SWM_CACHE_MAX_SIZE и DEFAULT_SWM_PAGE_SIZE будут взяты из main.py по умолчанию
# SWM_INDEXED_METADATA_KEYS будет пустой по умолчанию, можно переопределить в docker-compose

# Открываем порт, на котором будет слушать сервис
EXPOSE 50053

# Команда для запуска приложения
CMD ["python", "app/main.py"]
