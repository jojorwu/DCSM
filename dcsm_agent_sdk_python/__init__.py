import os
import sys

# Add the generated_grpc_code directory to sys.path when the dcsm_agent_sdk_python package is imported.
# This allows the flat imports within the generated _pb2.py files to find each other.
sdk_root_dir = os.path.dirname(os.path.abspath(__file__))
generated_code_dir = os.path.join(sdk_root_dir, "generated_grpc_code")

if generated_code_dir not in sys.path:
    sys.path.insert(0, generated_code_dir)
