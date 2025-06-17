import os
import sys

# Add this directory (generated_grpc/) to sys.path when this package is imported.
# This allows the flat imports within the generated _pb2.py files (e.g., import kem_pb2)
# to find each other as they are all in this directory.
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
