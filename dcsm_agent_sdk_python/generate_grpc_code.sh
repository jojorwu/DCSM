#!/bin/bash
echo "Генерация gRPC кода для клиента SDK..."
PYTHON_EXE=$(python3 -c "import sys; print(sys.executable)")
OUTPUT_DIR="./generated_grpc_code" # Генерация в поддиректорию SDK
PROTO_DIR="./protos"

mkdir -p $OUTPUT_DIR

echo "Исходные proto файлы в $PROTO_DIR:"
ls -l $PROTO_DIR

# Компиляция kem.proto, glm_service.proto, swm_service.proto
# Важно: пути к proto файлам должны быть относительно директории, указанной в -I
# или абсолютными/относительными от текущей директории, если -I указывает на их родителя.
# Здесь $PROTO_DIR это ./protos, а файлы там kem.proto и т.д.
$PYTHON_EXE -m grpc_tools.protoc -I$PROTO_DIR --python_out=$OUTPUT_DIR --grpc_python_out=$OUTPUT_DIR $PROTO_DIR/kem.proto $PROTO_DIR/glm_service.proto $PROTO_DIR/swm_service.proto

# Создаем __init__.py в generated_grpc_code, чтобы сделать его пакетом
touch $OUTPUT_DIR/__init__.py

echo "Содержимое сгенерированной директории $OUTPUT_DIR:"
ls -l $OUTPUT_DIR
echo "Генерация кода для клиента SDK завершена."
