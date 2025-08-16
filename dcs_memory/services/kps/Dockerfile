# Используем официальный образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /service

# Создаем виртуальное окружение
ENV VIRTUAL_ENV=/service/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Устанавливаем зависимости
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
# Имя сервиса GLM в сети docker-compose
ENV GLM_SERVICE_ADDRESS="glm:50051"
ENV KPS_GRPC_LISTEN_ADDRESS="[::]:50052"
# SENTENCE_TRANSFORMER_MODEL и DEFAULT_VECTOR_SIZE будут взяты из main.py по умолчанию.
# Модели sentence-transformers будут скачиваться при первом запуске KPS, если их нет.
# Чтобы включить их в образ, потребовалась бы дополнительная команда RUN для их скачивания,
# что увеличит размер образа, но ускорит первый старт. Пока оставляем скачивание при запуске.
# Пример для скачивания модели в образ (потребует python и нужный скрипт):
# RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer(os.getenv('SENTENCE_TRANSFORMER_MODEL', 'all-MiniLM-L6-v2'))"

# Открываем порт, на котором будет слушать сервис
EXPOSE 50052

# Команда для запуска приложения
CMD ["python", "app/main.py"]
