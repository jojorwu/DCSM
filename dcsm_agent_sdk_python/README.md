# DCSM Python Agent SDK

This Python SDK facilitates interaction with the Dynamic Contextualized Shared Memory (DCSM) services, such as GLM (Global Long-Term Memory) and SWM (Shared Working Memory).
The SDK provides client classes and utilities to simplify agent development, including support for multi-agent scenarios.

## Features

*   **GLMClient (`GLMClient`)**: Allows storing, retrieving, updating, and deleting Contextualized Memory Units (KEMs) in long-term storage. Includes retry logic.
*   **SWMClient (`SWMClient`)**: Enables interaction with the shared working memory, including:
    *   Publishing KEMs to SWM.
    *   Querying KEMs from the SWM cache.
    *   Loading KEMs from GLM into SWM.
    *   Subscribing to KEM change events in SWM.
    *   Working with distributed locks.
    *   Working with distributed counters.
    *   Also includes retry logic.
*   **Local Agent Memory (`LocalAgentMemory` - LAM)**: A client-side LRU cache with metadata indexing capabilities for fast local access to frequently used KEMs.
*   **Main `AgentSDK` Class**: A high-level interface that combines the capabilities of `GLMClient`, `SWMClient`, and `LocalAgentMemory`, providing convenient methods for:
    *   Managing KEMs in GLM and LAM.
    *   Batch operations with SWM (publishing, loading into LAM).
    *   Handling SWM events with automatic LAM updates.
    *   Working with distributed locks (including a context manager).
    *   Working with distributed counters.

## Installation

1.  **Clone the SDK repository** (or copy the `dcsm_agent_sdk_python` directory into your project).

2.  **Install dependencies:**
    Navigate to the `dcsm_agent_sdk_python` directory and run:
    ```bash
    pip install -r requirements.txt
    ```
    This will install `grpcio`, `grpcio-tools`, `protobuf`, `cachetools`.

3.  **Generate gRPC code (if necessary):**
    The SDK comes with pre-generated gRPC code in the `generated_grpc_code/` directory. However, if you have modified the `.proto` files (note: canonical protos are in `dcs_memory/common/grpc_protos/` relative to the project root) or wish to regenerate the code, execute the script from the root directory of `dcsm_agent_sdk_python`:
    ```bash
    ./generate_grpc_code.sh
    ```
    *Ensure the script is executable (`chmod +x generate_grpc_code.sh`).*

## Usage

The main class for use is `AgentSDK`.

```python
# Example import if dcsm_agent_sdk_python is in PYTHONPATH
# or installed as a package
from dcsm_agent_sdk_python.sdk import AgentSDK
# For working with SWM-related proto types directly (e.g., for event handlers or creating topics):
from dcsm_agent_sdk_python.generated_grpc_code import swm_service_pb2 # for LockStatusValue, SubscriptionTopic, etc.

# Initialize the SDK (assumes GLM and SWM servers are running on default ports)
# sdk = AgentSDK(
#     glm_server_address='localhost:50051',
#     swm_server_address='localhost:50053',
#     lpa_max_size=200, # Size of Local Agent Memory (LAM)
#     lpa_indexed_keys=['topic', 'source'] # Example indexable keys in LAM
# )

# Use the context manager for automatic connection management:
# with AgentSDK(glm_server_address='localhost:50051', swm_server_address='localhost:50053') as sdk:
#     # Your SDK interaction code here
#     # Example: Store a KEM in GLM
#     kem_to_store_glm = {
#         "id": "example_glm_kem_001", "content_type": "text/plain",
#         "content": "This is a KEM for GLM.", "metadata": {"source": "readme_glm"}
#     }
#     stored_kems, _, _ = sdk.store_kems([kem_to_store_glm])
#     if stored_kems:
#         print(f"KEM '{stored_kems[0]['id']}' stored in GLM.")
#
#         # Retrieve KEM from GLM (LAM is checked first)
#         retrieved_kem = sdk.get_kem(stored_kems[0]['id'])
#         if retrieved_kem:
#             print(f"Retrieved KEM: {retrieved_kem['content']}")

### Interacting with SWM (Shared Working Memory)

# with AgentSDK(...) as sdk:
#     # 1. Batch publish KEMs to SWM (without persistence to GLM by default)
#     kems_for_swm = [
#         {"id": "swm_kem_1", "content": "Data for SWM 1", "metadata": {"group": "alpha"}},
#         {"id": "swm_kem_2", "content": "Data for SWM 2", "metadata": {"group": "alpha"}}
#     ]
#     successful_pubs, failed_pubs = sdk.publish_kems_to_swm_batch(kems_for_swm, persist_to_glm=False)
#     print(f"Published to SWM: {len(successful_pubs)} succeeded, {len(failed_pubs)} failed.")

#     # 2. Load KEMs from SWM to LAM by metadata
#     query_dict_swm = {"metadata_filters": {"group": "alpha"}}
#     loaded_to_lpa = sdk.load_kems_to_lpa_from_swm(query_dict_swm, max_kems_to_load=5)
#     print(f"Loaded into LAM from SWM: {len(loaded_to_lpa)} KEMs.")
#     for kem_in_lpa in loaded_to_lpa:
#         print(f"  LAM contains: {kem_in_lpa['id']} - {kem_in_lpa['content']}")

### Subscribing to SWM Events

# def my_event_handler(event: swm_service_pb2.SWMMemoryEvent):
#     print(f"SDK_README: Received SWM Event! Type: {swm_service_pb2.SWMMemoryEvent.EventType.Name(event.event_type)}")
#     if event.HasField("kem_payload"):
#         print(f"  For KEM ID: {event.kem_payload.id}, Content (start): {event.kem_payload.content.decode('utf-8', errors='ignore')[:50]}...")

# with AgentSDK(...) as sdk:
#     # Subscribe to all KEM_LIFECYCLE_EVENTS for KEMs with metadata.group = "alpha"
#     topic = swm_service_pb2.SubscriptionTopic(
#         type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
#         filter_criteria="metadata.group=alpha"
#     )
#     # Start the handler in a background thread, with LAM auto-update
#     event_thread = sdk.handle_swm_events(
#         topics=[topic],
#         event_callback=my_event_handler,
#         auto_update_lpa=True,
#         run_in_background=True
#     )
#     print("SWM event subscription active in background...")
#     # ... (agent can perform other work here) ...
#     # To stop (if needed explicitly, though thread is a daemon):
#     # sdk.swm_client.channel.close() # This would stop the stream, or manage thread with an event.

### Distributed Locks via SWM

# agent_id_for_lock = "my_agent_id_123"
# resource_name = "shared_resource_xyz"

# with AgentSDK(...) as sdk:
#     print(f"Attempting to acquire lock on '{resource_name}'...")
#     with sdk.distributed_lock(resource_name, agent_id_for_lock, acquire_timeout_ms=5000, lease_duration_ms=30000) as lock_resp:
#         if lock_resp and lock_resp.status == swm_service_pb2.LockStatusValue.ACQUIRED:
#             print(f"Lock '{resource_name}' acquired! Lock ID: {lock_resp.lock_id}. Working with resource...")
#             # ... (critical section, work with shared resource) ...
#             print(f"Work on resource '{resource_name}' finished.")
#         elif lock_resp and lock_resp.status == swm_service_pb2.LockStatusValue.ALREADY_HELD_BY_YOU:
#             print(f"Lock '{resource_name}' is already held by me (agent {agent_id_for_lock}).")
#         else:
#             status_name = swm_service_pb2.LockStatusValue.Name(lock_resp.status) if lock_resp else "N/A"
#             print(f"Failed to acquire lock on '{resource_name}'. Status: {status_name}, Message: {lock_resp.message if lock_resp else ''}")
#     print(f"Lock '{resource_name}' should be automatically released.")

### Distributed Counters via SWM

# counter_name = "global_event_counter"
# with AgentSDK(...) as sdk:
#     # Increment counter
#     new_value = sdk.increment_distributed_counter(counter_name, increment_by=1)
#     if new_value is not None:
#         print(f"Counter '{counter_name}' incremented. New value: {new_value}")

#     # Get counter value
#     current_value = sdk.get_distributed_counter(counter_name)
#     if current_value is not None:
#         print(f"Current value of counter '{counter_name}': {current_value}")

#     # Increment by -5 (decrement)
#     new_value_decr = sdk.increment_distributed_counter(counter_name, increment_by=-5)
#     if new_value_decr is not None:
#         print(f"Counter '{counter_name}' decremented by 5. New value: {new_value_decr}")
```

See `example.py` for more detailed examples of all these features.

## Running Services (for SDK Testing)

To fully test the SDK, running instances of the DCSM services (at least GLM and SWM) are required.
The recommended way to run all services is via `docker-compose` from the root directory of the DCSM project:

```bash
# From the project's root directory
docker-compose up --build
```

This will start:
*   GLM service (typically on `localhost:50051`)
*   SWM service (typically on `localhost:50053`)
*   KPS service (typically on `localhost:50052`)
*   Qdrant (vector database)

Ensure that the server addresses and ports in your `AgentSDK` initialization match the configuration of the running services.
