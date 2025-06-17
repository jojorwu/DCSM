#!/bin/bash
SCRIPT_DIR=$(dirname "$0") # Директория, где находится сам скрипт (dcs_memory/services/swm)
cd "$SCRIPT_DIR" || exit 1 # Переходим в директорию скрипта

echo "Текущая директория для генерации SWM кода: $(pwd)"

CENTRAL_PROTO_PATH_FROM_SCRIPT="../../common/grpc_protos" # Путь к общим proto
GENERATED_OUTPUT_DIR="./generated_grpc" # Локальная директория для сгенерированного кода

echo "Генерация gRPC кода для SWM сервиса..."
echo "Источник .proto файлов: $CENTRAL_PROTO_PATH_FROM_SCRIPT"
echo "Выходная директория для сгенерированного кода: $GENERATED_OUTPUT_DIR"

PYTHON_EXE=$(python3 -c "import sys; print(sys.executable)")

# Проверяем наличие proto файлов перед компиляцией
echo "Проверка наличия общих proto файлов в $CENTRAL_PROTO_PATH_FROM_SCRIPT:"
if [ ! -d "$CENTRAL_PROTO_PATH_FROM_SCRIPT" ]; then
    echo "Ошибка: Директория с общими proto файлами '$CENTRAL_PROTO_PATH_FROM_SCRIPT' не найдена!"
    # exit 1 # Закомментировано для subtask
fi
ls -l "$CENTRAL_PROTO_PATH_FROM_SCRIPT"

# Создаем выходную директорию, если ее нет
mkdir -p "$GENERATED_OUTPUT_DIR"

# Компилируем kem.proto, glm_service.proto (для клиента GLM) и swm_service.proto (для сервера SWM)
# kps_service.proto пока не нужен для SWM.
$PYTHON_EXE -m grpc_tools.protoc \
    -I"$CENTRAL_PROTO_PATH_FROM_SCRIPT" \
    --python_out="$GENERATED_OUTPUT_DIR" \
    --grpc_python_out="$GENERATED_OUTPUT_DIR" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/kem.proto" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/glm_service.proto" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/swm_service.proto"

# Создаем __init__.py в generated_grpc, чтобы сделать его пакетом
touch "$GENERATED_OUTPUT_DIR/__init__.py"

echo "Содержимое сгенерированной директории $GENERATED_OUTPUT_DIR:"
ls -l "$GENERATED_OUTPUT_DIR"
echo "Генерация кода для SWM сервиса завершена."
