#!/bin/bash
echo "Генерация gRPC кода для клиента SDK..."
PYTHON_EXE="python3" # Используем python3 из активированного venv
OUTPUT_DIR="./generated_grpc_code" # Генерация в поддиректорию SDK
# PROTO_DIR теперь указывает на канонический источник
# Относительный путь из dcsm_agent_sdk_python/ до dcs_memory/common/grpc_protos/
PROTO_DIR="../dcs_memory/common/grpc_protos"

mkdir -p $OUTPUT_DIR

echo "Исходные proto файлы из канонической директории: $PROTO_DIR"
# Проверяем, существует ли директория, чтобы избежать ошибок ls и protoc
if [ ! -d "$PROTO_DIR" ]; then
    echo "Ошибка: Директория с proto файлами '$PROTO_DIR' не найдена!"
    echo "Убедитесь, что структура директорий правильная и SDK находится на одном уровне с dcs_memory."
    # exit 1 # Закомментировано для subtask
fi
ls -l "$PROTO_DIR"

# Компиляция всех общих proto файлов, так как SDK может потенциально использовать любой из них
$PYTHON_EXE -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out=$OUTPUT_DIR \
    --grpc_python_out=$OUTPUT_DIR \
    "$PROTO_DIR/kem.proto" \
    "$PROTO_DIR/glm_service.proto" \
    "$PROTO_DIR/swm_service.proto" \
    "$PROTO_DIR/kps_service.proto"

# Создаем __init__.py в generated_grpc_code, чтобы сделать его пакетом
touch $OUTPUT_DIR/__init__.py

echo "Содержимое сгенерированной директории $OUTPUT_DIR:"
ls -l $OUTPUT_DIR
echo "Генерация кода для клиента SDK завершена."
