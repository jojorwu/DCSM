#!/bin/bash
echo "Generating gRPC code..."
PYTHON_EXE="python3" # Using python3 from the activated venv
TEMP_OUTPUT_DIR="temp_generated_grpc_code"
mkdir -p $TEMP_OUTPUT_DIR
# Check for proto files before compilation
echo "Checking for proto files in dcs_memory/common/grpc_protos/:"
ls -l dcs_memory/common/grpc_protos/
$PYTHON_EXE -m grpc_tools.protoc -I./dcs_memory/common/grpc_protos \
    --python_out=$TEMP_OUTPUT_DIR \
    --grpc_python_out=$TEMP_OUTPUT_DIR \
    dcs_memory/common/grpc_protos/kem.proto \
    dcs_memory/common/grpc_protos/glm_service.proto \
    dcs_memory/common/grpc_protos/swm_service.proto \
    dcs_memory/common/grpc_protos/kps_service.proto
# Added kps_service.proto for completeness, if it exists and is needed globally

echo "Contents of the generated directory $TEMP_OUTPUT_DIR:"
ls -R $TEMP_OUTPUT_DIR
echo "Generation complete."
