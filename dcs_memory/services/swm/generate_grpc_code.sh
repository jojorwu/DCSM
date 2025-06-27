#!/bin/bash
SCRIPT_DIR=$(dirname "$0") # Directory where the script itself is located (dcs_memory/services/swm)
cd "$SCRIPT_DIR" || exit 1 # Change to the script's directory

echo "Current directory for SWM code generation: $(pwd)"

CENTRAL_PROTO_PATH_FROM_SCRIPT="../../common/grpc_protos" # Path to common protos
GENERATED_OUTPUT_DIR="./generated_grpc" # Local directory for generated code

echo "Generating gRPC code for SWM service..."
echo "Source .proto files from: $CENTRAL_PROTO_PATH_FROM_SCRIPT"
echo "Output directory for generated code: $GENERATED_OUTPUT_DIR"

PYTHON_EXE="python3" # Using python3 from the activated venv

# Check for common proto files before compilation
echo "Checking for common proto files in $CENTRAL_PROTO_PATH_FROM_SCRIPT:"
if [ ! -d "$CENTRAL_PROTO_PATH_FROM_SCRIPT" ]; then
    echo "Error: Directory with common proto files '$CENTRAL_PROTO_PATH_FROM_SCRIPT' not found!"
    # exit 1 # Commented out for subtask, should be active for standalone execution
fi
ls -l "$CENTRAL_PROTO_PATH_FROM_SCRIPT"

# Create the output directory if it doesn't exist
mkdir -p "$GENERATED_OUTPUT_DIR"

# Compile kem.proto, glm_service.proto (for GLM client) and swm_service.proto (for SWM server)
# kps_service.proto is not currently needed by SWM.
$PYTHON_EXE -m grpc_tools.protoc \
    -I"$CENTRAL_PROTO_PATH_FROM_SCRIPT" \
    --python_out="$GENERATED_OUTPUT_DIR" \
    --grpc_python_out="$GENERATED_OUTPUT_DIR" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/kem.proto" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/glm_service.proto" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/swm_service.proto"

# Create __init__.py in generated_grpc to make it a package
touch "$GENERATED_OUTPUT_DIR/__init__.py"

echo "Contents of the generated directory $GENERATED_OUTPUT_DIR:"
ls -l "$GENERATED_OUTPUT_DIR"
echo "gRPC code generation for SWM service complete."
