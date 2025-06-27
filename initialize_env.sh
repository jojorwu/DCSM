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

if [ -f "./generate_grpc_code.sh" ]; then
    ./generate_grpc_code.sh || { echo "Error generating gRPC code with generate_grpc_code.sh. Ensure 'protoc' is installed and the script works correctly."; exit 1; }
else
    echo "Script 'generate_grpc_code.sh' not found. Skipping gRPC code generation."
fi

echo ""
echo "--------------------------------------------------------"
echo "DCSM development environment initialization complete!"
echo "Virtual environment 'venv' created and dependencies installed."
echo "Cleanup commands from the original setup_environment.sh WERE NOT EXECUTED."
echo "--------------------------------------------------------"
