#!/bin/bash
SCRIPT_DIR=$(dirname "$0") # Directory where the script itself is located
cd "$SCRIPT_DIR" || exit 1 # Change to the script's directory for correct relative paths

echo "Current directory for generation: $(pwd)"

CENTRAL_PROTO_PATH_FROM_SCRIPT="../../common/grpc_protos"
GENERATED_OUTPUT_DIR="./generated_grpc"

echo "Generating gRPC code for GLM service..."
echo "Source .proto files from: $CENTRAL_PROTO_PATH_FROM_SCRIPT"
echo "Output directory for generated code: $GENERATED_OUTPUT_DIR"

PYTHON_EXE="python3" # Using python3 from the activated venv

# Check for proto files before compilation
echo "Checking for proto files in $CENTRAL_PROTO_PATH_FROM_SCRIPT:"
if [ ! -d "$CENTRAL_PROTO_PATH_FROM_SCRIPT" ]; then
    echo "Error: Directory with proto files '$CENTRAL_PROTO_PATH_FROM_SCRIPT' not found!"
    # exit 1 # Commented out to prevent session termination
fi
ls -l "$CENTRAL_PROTO_PATH_FROM_SCRIPT"

# Create the output directory if it doesn't exist
mkdir -p "$GENERATED_OUTPUT_DIR"

# Compile only kem.proto and glm_service.proto for the GLM service
# swm_service.proto is not needed here for the GLM server-side.
# The -I path should point to the directory containing imported files (e.g., kem.proto for glm_service.proto)
# and the files being compiled themselves.
$PYTHON_EXE -m grpc_tools.protoc \
    -I"$CENTRAL_PROTO_PATH_FROM_SCRIPT" \
    --python_out="$GENERATED_OUTPUT_DIR" \
    --grpc_python_out="$GENERATED_OUTPUT_DIR" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/kem.proto" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/glm_service.proto"
    # "$CENTRAL_PROTO_PATH_FROM_SCRIPT/swm_service.proto" # SWM not included for now

# Create __init__.py in generated_grpc to make it a package
touch "$GENERATED_OUTPUT_DIR/__init__.py"

echo "Contents of the generated directory $GENERATED_OUTPUT_DIR:"
ls -l "$GENERATED_OUTPUT_DIR"
echo "gRPC code generation for GLM service complete."
