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
# ^^^ Это корневой requirements.txt, предполагая, что он содержит grpcio-tools
# Если grpcio-tools есть в requirements.txt самого сервиса, то корневой можно не копировать
# или копировать только его для grpcio-tools, если он там.
# Пока предположим, что grpcio-tools есть в корневом.
RUN pip install --no-cache-dir -r /tmp/common_requirements.txt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем общие proto-файлы. Путь относительно Dockerfile.
# Dockerfile находится в dcs_memory/services/glm/
# Нужные proto файлы в dcs_memory/common/grpc_protos/
COPY ../../common/grpc_protos /service/common_protos

# Копируем скрипт генерации gRPC и исходный код сервиса
COPY generate_grpc_code.sh .
COPY app/ app/

# Делаем скрипт генерации исполняемым и запускаем его
# Важно: скрипт generate_grpc_code.sh должен быть адаптирован для работы с путями внутри контейнера,
# если он использует относительные пути, предполагающие определенную структуру вне Docker.
# Текущий скрипт dcs_memory/services/glm/generate_grpc_code.sh:
# PYTHON_OUT_DIR="./generated_grpc"
# GRPC_PYTHON_OUT_DIR="./generated_grpc"
# PROTO_DIR="../../common/grpc_protos"
# Он должен использовать /service/common_protos в качестве PROTO_DIR внутри контейнера.
# Изменим это в Dockerfile через sed или передадим как аргумент, если скрипт поддерживает.
# Простой вариант: изменить PROTO_DIR в скрипте перед запуском.
RUN sed -i 's|PROTO_DIR="../../common/grpc_protos"|PROTO_DIR="/service/common_protos"|' generate_grpc_code.sh && \
    chmod +x generate_grpc_code.sh && \
    ./generate_grpc_code.sh

# Устанавливаем переменные окружения для конфигурации сервиса
# QDRANT_HOST будет именем сервиса qdrant в docker-compose сети
ENV QDRANT_HOST="qdrant"
ENV QDRANT_PORT="6333"
ENV SQLITE_DB_FILENAME="glm_metadata.sqlite3" # Будет создан в app/ внутри контейнера
ENV GRPC_LISTEN_ADDRESS="[::]:50051"
# DEFAULT_VECTOR_SIZE и DEFAULT_PAGE_SIZE будут взяты из main.py по умолчанию

# Открываем порт, на котором будет слушать сервис
EXPOSE 50051

# Команда для запуска приложения
CMD ["python", "app/main.py"]
