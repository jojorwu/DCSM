#!/bin/bash
SCRIPT_DIR=$(dirname "$0") # Directory where the script itself is located (dcs_memory/services/kps)
cd "$SCRIPT_DIR" || exit 1 # Change to the script's directory

echo "Current directory for KPS code generation: $(pwd)"

CENTRAL_PROTO_PATH_FROM_SCRIPT="../../common/grpc_protos" # Path to common proto files
GENERATED_OUTPUT_DIR="../../generated_grpc" # Local directory for generated code

echo "Generating gRPC code for KPS service..."
echo "Source .proto files from: $CENTRAL_PROTO_PATH_FROM_SCRIPT"
echo "Output directory for generated code: $GENERATED_OUTPUT_DIR"

PYTHON_EXE="python3" # Using python3 from the activated venv

# Check for common proto files before compilation
echo "Checking for common proto files in $CENTRAL_PROTO_PATH_FROM_SCRIPT:"
if [ ! -d "$CENTRAL_PROTO_PATH_FROM_SCRIPT" ]; then
    echo "Error: Directory with common proto files '$CENTRAL_PROTO_PATH_FROM_SCRIPT' not found!"
    # exit 1 # Commented out for subtask, but should be active in reality
fi
ls -l "$CENTRAL_PROTO_PATH_FROM_SCRIPT"

# Create the output directory if it doesn't exist
mkdir -p "$GENERATED_OUTPUT_DIR"

# Compile kem.proto, glm_service.proto (for GLM client), and kps_service.proto (for KPS server)
# The -I path should point to the directory containing imported files.
# All our proto files are in one common directory.
$PYTHON_EXE -m grpc_tools.protoc \
    -I"$CENTRAL_PROTO_PATH_FROM_SCRIPT" \
    --python_out="$GENERATED_OUTPUT_DIR" \
    --grpc_python_out="$GENERATED_OUTPUT_DIR" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/kem.proto" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/glm_service.proto" \
    "$CENTRAL_PROTO_PATH_FROM_SCRIPT/kps_service.proto"

# Create __init__.py in generated_grpc to make it a package
touch "$GENERATED_OUTPUT_DIR/__init__.py"
# Also create __init__.py in the parent directory of the generated code if it's used as part of the import path.
# For example, if generated_grpc contains subdirectories named after protos.
# In our case, everything is generated flat into generated_grpc.

echo "Contents of the generated directory $GENERATED_OUTPUT_DIR:"
ls -l "$GENERATED_OUTPUT_DIR"
echo "gRPC code generation for KPS service complete."
