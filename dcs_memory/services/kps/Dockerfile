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
RUN pip install --no-cache-dir -r /tmp/common_requirements.txt

COPY requirements.txt .
# Установка sentence-transformers и его зависимостей (например, PyTorch) может быть долгой
# и может потребовать больше места.
# Убедимся, что pip использует --no-cache-dir для экономии места.
RUN pip install --no-cache-dir -r requirements.txt

# Копируем общие proto-файлы.
COPY ../../common/grpc_protos /service/common_protos

# Копируем скрипт генерации gRPC и исходный код сервиса
COPY generate_grpc_code.sh .
COPY app/ app/

# Адаптируем и запускаем скрипт генерации gRPC кода
# Скрипт dcs_memory/services/kps/generate_grpc_code.sh также использует PROTO_DIR="../../common/grpc_protos"
RUN sed -i 's|PROTO_DIR="../../common/grpc_protos"|PROTO_DIR="/service/common_protos"|' generate_grpc_code.sh && \
    chmod +x generate_grpc_code.sh && \
    ./generate_grpc_code.sh

# Устанавливаем переменные окружения для конфигурации сервиса
ENV GLM_SERVICE_ADDRESS="glm:50051" # Имя сервиса GLM в сети docker-compose
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
