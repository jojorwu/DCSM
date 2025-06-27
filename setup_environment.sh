#!/bin/bash

# --- Basic environment setup and navigation ---
# set -e: Exit immediately if a command exits with a non-zero status.
# set -u: Treat unset variables as an error when substituting.
# set -x: Print commands and their arguments as they are executed (for debugging).
set -eux

# Navigate to the project root directory (assuming this script is run from a context where /app is the root)
cd /app

# --- 1. Python Environment Setup ---
echo "--- Setting up Python environment ---"

# Create and activate a virtual environment for dependency isolation
python3 -m venv venv
source venv/bin/activate

# Variable to isolate pip from system packages (useful in some environments)
export PIP_BREAK_SYSTEM_PACKAGES=1

# Find and install Python dependencies from all requirements.txt files
# (Searches in the current directory and one level deep)
echo "Searching for and installing Python dependencies from requirements.txt files..."
files=$(find . -maxdepth 2 -type f -wholename "*requirements*.txt")

# Check if requirements.txt files were found and install dependencies
if [ -n "$files" ]; then
    python -m pip install $(echo "$files" | xargs -I {{}} echo -r {{}}) || { echo "Error installing Python dependencies. Check requirements.txt files."; exit 1; }
else
    echo "'requirements.txt' files not found. Skipping Python dependency installation."
fi

# --- 2. Node.js Environment Setup (if 'dcs_memory_node' folder exists) ---
echo ""
echo "--- Setting up Node.js environment (if 'dcs_memory_node' folder exists) ---"

# Check for the Node.js components folder
if [ -d "dcs_memory_node" ]; then
    echo "Navigating to 'dcs_memory_node' to install Node.js dependencies..."
    cd dcs_memory_node

    # Initialize Node.js project if not already initialized (npm init -y)
    # Use '|| true' to prevent script interruption if package.json already exists
    npm init -y || true

    echo "Installing Node.js dependencies..."
    npm install || { echo "Error installing Node.js dependencies. Check package.json in dcs_memory_node."; exit 1; }

    echo "Returning to the project root directory..."
    cd ..
else
    echo "'dcs_memory_node' folder not found. Skipping Node.js dependency installation."
fi

# --- 3. gRPC Code Generation ---
echo ""
echo "--- Generating gRPC code for Python (and Node.js if applicable) ---"

# It is assumed that your generate_grpc_code.sh script handles generation for both languages if needed.
# Ensure `protoc` (Protocol Buffers compiler) is installed and available in PATH.
# If `generate_grpc_code.sh` doesn't work or needs setup,
# here are examples of manual generation (commented out):
#
# # For Python:
# python3 -m grpc_tools.protoc -I. --python_out=generated_grpc --grpc_python_out=generated_grpc dcs_memory/common/grpc_protos/*.proto
#
# # For Node.js (if `dcs_memory_node` exists and has `grpc-tools` in devDependencies):
# if [ -d "dcs_memory_node" ]; then
#     cd dcs_memory_node
#     ./node_modules/.bin/grpc_tools_node_protoc \
#         --plugin=protoc-gen-grpc=`which grpc_tools_node_protoc_plugin` \
#         --js_out=import_style=commonjs,binary:./generated_protos \
#         --grpc_out=grpc_js:./generated_protos \
#         -I../dcs_memory/common/grpc_protos \
#         ../dcs_memory/common/grpc_protos/*.proto
#     cd ..
# fi

if [ -f "./generate_grpc_code.sh" ]; then
    ./generate_grpc_code.sh || { echo "Error generating gRPC code with generate_grpc_code.sh. Ensure 'protoc' is installed and the script works correctly."; exit 1; }
else
    echo "Script 'generate_grpc_code.sh' not found. Skipping gRPC code generation."
fi

echo ""
echo "--------------------------------------------------------"
echo "DCSM development environment setup complete!"
echo "You can now run tests or services."
echo "--------------------------------------------------------"

# --- Additional commands for running tests and services (informational) ---
# These commands are not run automatically by this script;
# they need to be executed manually in separate terminals.

echo ""
echo "--- How to run tests: ---"
echo "  For Python tests:               python3 -m pytest"
echo "  For Node.js tests (from dcs_memory_node folder):"
echo "    cd dcs_memory_node && npm test && cd .."

echo ""
echo "--- How to run services (in separate terminals): ---"
echo "  Run GLM (Python):            python3 dcs_memory/services/glm/app/main.py"
echo "  Run SWM (Python):            python3 dcs_memory/services/swm/app/main.py"
echo "  Run Agent Example (Python): python3 example.py" # Assuming example.py is in the root
echo "  Run Node.js service (from dcs_memory_node folder):"
echo "    cd dcs_memory_node && node src/index.js && cd .."

echo ""
echo "--- Debugging and maintenance commands: ---"
echo "  View running Python processes: pgrep -lf python"
echo "  View service logs (example):       tail -f /path/to/your/service/log/file.log"
echo "  Clean generated files and cache:"
echo "    rm -rf generated_grpc generated_grpc_code temp_generated_grpc_code"
echo "    find . -name \"__pycache__\" -type d -exec rm -rf {} +"
echo "    find . -name \"*.pyc\" -type f -delete"
echo "    rm -rf venv"
echo "    if [ -d \"dcs_memory_node\" ]; then rm -rf dcs_memory_node/node_modules; fi"
