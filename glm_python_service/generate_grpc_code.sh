#!/bin/bash
echo "Генерация gRPC кода..."
PYTHON_EXE=$(python3 -c "import sys; print(sys.executable)")
TEMP_OUTPUT_DIR="./app/generated_grpc_code"
mkdir -p $TEMP_OUTPUT_DIR
# Проверяем наличие proto файлов перед компиляцией
echo "Проверка наличия proto файлов:"
ls -l protos/
$PYTHON_EXE -m grpc_tools.protoc -I./protos --python_out=$TEMP_OUTPUT_DIR --grpc_python_out=$TEMP_OUTPUT_DIR ./protos/kem.proto ./protos/glm_service.proto ./protos/swm_service.proto
echo "Содержимое сгенерированной директории $TEMP_OUTPUT_DIR:"
ls -R $TEMP_OUTPUT_DIR
echo "Генерация завершена."
