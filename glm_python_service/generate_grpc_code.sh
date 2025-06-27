#!/bin/bash
echo "Генерация gRPC кода..."
PYTHON_EXE="python3" # Используем python3 из активированного venv
TEMP_OUTPUT_DIR="./app/generated_grpc_code"
# PROTO_DIR теперь указывает на канонический источник
# Относительный путь из glm_python_service/ до dcs_memory/common/grpc_protos/
PROTO_DIR="../dcs_memory/common/grpc_protos"

mkdir -p $TEMP_OUTPUT_DIR

echo "Исходные proto файлы из канонической директории: $PROTO_DIR"
if [ ! -d "$PROTO_DIR" ]; then
    echo "Ошибка: Директория с proto файлами '$PROTO_DIR' не найдена!"
    # exit 1
fi
ls -l "$PROTO_DIR"

$PYTHON_EXE -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out=$TEMP_OUTPUT_DIR \
    --grpc_python_out=$TEMP_OUTPUT_DIR \
    "$PROTO_DIR/kem.proto" \
    "$PROTO_DIR/glm_service.proto" \
    "$PROTO_DIR/swm_service.proto" \
    "$PROTO_DIR/kps_service.proto"

echo "Содержимое сгенерированной директории $TEMP_OUTPUT_DIR:"
ls -R $TEMP_OUTPUT_DIR
echo "Генерация завершена."
