#!/bin/bash
echo "Генерация gRPC кода..."
PYTHON_EXE="python3" # Используем python3 из активированного venv
TEMP_OUTPUT_DIR="temp_generated_grpc_code"
mkdir -p $TEMP_OUTPUT_DIR
# Проверяем наличие proto файлов перед компиляцией
echo "Проверка наличия proto файлов в dcs_memory/common/grpc_protos/:"
ls -l dcs_memory/common/grpc_protos/
$PYTHON_EXE -m grpc_tools.protoc -I./dcs_memory/common/grpc_protos \
    --python_out=$TEMP_OUTPUT_DIR \
    --grpc_python_out=$TEMP_OUTPUT_DIR \
    dcs_memory/common/grpc_protos/kem.proto \
    dcs_memory/common/grpc_protos/glm_service.proto \
    dcs_memory/common/grpc_protos/swm_service.proto \
    dcs_memory/common/grpc_protos/kps_service.proto
# Добавлен kps_service.proto для полноты, если он там есть и нужен глобально

echo "Содержимое сгенерированной директории $TEMP_OUTPUT_DIR:"
ls -R $TEMP_OUTPUT_DIR
echo "Генерация завершена."
