#!/bin/bash
SCRIPT_DIR=$(dirname "$0") # Директория, где находится сам скрипт
cd "$SCRIPT_DIR" || exit 1 # Переходим в директорию скрипта для корректных относительных путей

echo "Текущая директория для генерации: $(pwd)"

CENTRAL_PROTO_PATH_FROM_SCRIPT="../../common/grpc_protos"
GENERATED_OUTPUT_DIR="./generated_grpc"

echo "Генерация gRPC кода для GLM сервиса..."
echo "Источник .proto файлов: $CENTRAL_PROTO_PATH_FROM_SCRIPT"
echo "Выходная директория для сгенерированного кода: $GENERATED_OUTPUT_DIR"

PYTHON_EXE=$(python3 -c "import sys; print(sys.executable)")

# Проверяем наличие proto файлов перед компиляцией
echo "Проверка наличия proto файлов в $CENTRAL_PROTO_PATH_FROM_SCRIPT:"
if [ ! -d "$CENTRAL_PROTO_PATH_FROM_SCRIPT" ]; then
    echo "Ошибка: Директория с proto файлами '$CENTRAL_PROTO_PATH_FROM_SCRIPT' не найдена!"
    # exit 1 # Commented out to prevent session termination
fi
ls -l "$CENTRAL_PROTO_PATH_FROM_SCRIPT"

# Создаем выходную директорию, если ее нет
mkdir -p "$GENERATED_OUTPUT_DIR"

# Компилируем только kem.proto и glm_service.proto для GLM сервиса
# swm_service.proto здесь не нужен для серверной части GLM.
# Путь -I должен указывать на директорию, где лежат импортируемые файлы (например, kem.proto для glm_service.proto)
# и сами файлы, которые компилируются.
$PYTHON_EXE -m grpc_tools.protoc \
    -I"$CENTRAL_PROTO_PATH_FROM_SCRIPT" \
    --python_out="$GENERATED_OUTPUT_DIR" \
    --grpc_python_out="$GENERATED_OUTPUT_DIR" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/kem.proto" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/glm_service.proto"
    # "$CENTRAL_PROTO_PATH_FROM_SCRIPT/swm_service.proto" # Пока не включаем SWM

# Создаем __init__.py в generated_grpc, чтобы сделать его пакетом
touch "$GENERATED_OUTPUT_DIR/__init__.py"

echo "Содержимое сгенерированной директории $GENERATED_OUTPUT_DIR:"
ls -l "$GENERATED_OUTPUT_DIR"
echo "Генерация кода для GLM сервиса завершена."
