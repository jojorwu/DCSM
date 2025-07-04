# dcsm_agent_sdk_python/example.py
import sys
import os
import grpc # Import grpc for RpcError handling
import time
import logging # For logging within the example
import typing # For Optional type hint

# --- Start of SDK import block ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
     sys.path.insert(0, project_root)

try:
    from dcsm_agent_sdk_python.sdk import AgentSDK
    from dcsm_agent_sdk_python.config import DCSMClientSDKConfig # Import new config
    from dcsm_agent_sdk_python.generated_grpc_code import swm_service_pb2 as common_swm_pb2 # For SWM enums
    # For KPS example, if adding one:
    # from dcsm_agent_sdk_python.generated_grpc_code import kps_service_pb2
except ImportError:
    # Fallback for direct execution if the above fails
    logging.warning("Could not import from 'dcsm_agent_sdk_python.*', trying local imports for 'sdk', 'config', etc.")
    from sdk import AgentSDK
    from config import DCSMClientSDKConfig
    from generated_grpc_code import swm_service_pb2 as common_swm_pb2
    # from generated_grpc_code import kps_service_pb2


logger = logging.getLogger("AgentSDKExample")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# --- End of SDK import block ---

def run_example():
    logger.info("Starting comprehensive AgentSDK example...")
    logger.info("This example demonstrates usage with DCSMClientSDKConfig.")
    logger.info("Ensure GLM, SWM, and KPS servers are running and configured as per SDK defaults or .env/environment variables.")
    logger.info("To actually run network-dependent parts, set environment variable RUN_SDK_EXAMPLE=true")

    if os.getenv("RUN_SDK_EXAMPLE") != "true":
        logger.warning("RUN_SDK_EXAMPLE environment variable not set to 'true'. Example will not connect to servers.")
        # Demonstrate config loading even if not connecting
        try:
            sdk_config_nocoonnect = DCSMClientSDKConfig()
            logger.info(f"SDK Config (no connect): GLM @ {sdk_config_nocoonnect.glm_address}, SWM @ {sdk_config_nocoonnect.swm_address}, KPS @ {sdk_config_nocoonnect.kps_address}")
            sdk_instance_noconnect = AgentSDK(config=sdk_config_nocoonnect) # connect_on_init is part of config now
            sdk_instance_noconnect.local_memory.put("local_test_001", {"id":"local_test_001", "data":"test_content"})
            logger.info(f"Local Agent Memory contains local_test_001: {sdk_instance_noconnect.local_memory.get('local_test_001')}")
            sdk_instance_noconnect.close()
        except Exception as e_no_connect:
            logger.error(f"Error during no-connect demonstration: {e_no_connect}")
        return

    try:
        # Load configuration from .env file or environment variables
        sdk_config = DCSMClientSDKConfig()
        logger.info(f"SDK Config loaded: GLM @ {sdk_config.glm_address}, SWM @ {sdk_config.swm_address}, KPS @ {sdk_config.kps_address}, TLS: {sdk_config.tls_enabled}")

        with AgentSDK(config=sdk_config) as sdk:
            # sdk_instance = sdk # Not needed as 'sdk' is already the instance

            sdk.local_memory.clear()
            logger.info("\n--- Local Agent Memory (LAM) cleared ---")

            kems_data_glm = [
                {"id": "ex_sdk_glm_001", "content_type": "text/plain", "content": "First KEM for GLM.", "metadata": {"tag": "glm_example", "version": 1}},
                {"id": "ex_sdk_glm_002", "content_type": "application/json", "content": '{"message": "Second GLM KEM"}', "metadata": {"tag": "glm_example", "format": "json"}, "embeddings": [0.1,0.1,0.1]},
                {"id": "ex_sdk_glm_003", "content_type": "text/plain", "content": "Third GLM KEM, to be deleted.", "metadata": {"tag": "temp_glm"}},
            ]

            logger.info("\n--- Storing multiple KEMs in GLM ---")
            stored_kems_info, failed_refs_glm, error_msg_glm = sdk.store_kems(kems_data_glm)

            successful_glm_ids = []
            if stored_kems_info:
                successful_glm_ids = [k['id'] for k in stored_kems_info]
                logger.info(f"Successfully stored in GLM {len(successful_glm_ids)} KEMs. IDs: {successful_glm_ids}")
                for kem_id_glm in successful_glm_ids:
                    assert sdk.local_memory.contains(kem_id_glm), f"KEM {kem_id_glm} should be in LAM after GLM store!"
            if failed_refs_glm: logger.error(f"Errors during GLM store (references): {failed_refs_glm}")
            if error_msg_glm: logger.error(f"Overall error message from GLM store: {error_msg_glm}")
            if not successful_glm_ids : logger.warning("GLM store failed, subsequent GLM tests might be affected.")

            if "ex_sdk_glm_001" in successful_glm_ids:
                logger.info("\n--- Retrieving ex_sdk_glm_001 (from LAM) ---")
                kem1_lpa = sdk.get_kem("ex_sdk_glm_001")
                if kem1_lpa: logger.info(f"Retrieved (LAM) ex_sdk_glm_001: {kem1_lpa.get('content')}")
                else: logger.error("KEM ex_sdk_glm_001 not found in LAM, though it should be!")

            if "ex_sdk_glm_001" in successful_glm_ids:
                logger.info("\n--- Force retrieving ex_sdk_glm_001 (from GLM server) ---")
                kem1_remote = sdk.get_kem("ex_sdk_glm_001", force_remote=True)
                if kem1_remote: logger.info(f"Retrieved (GLM server) ex_sdk_glm_001: {kem1_remote.get('content')}")
                else: logger.error("KEM ex_sdk_glm_001 not found on GLM server!")

            if "ex_sdk_glm_002" in successful_glm_ids:
                logger.info("\n--- Updating ex_sdk_glm_002 in GLM ---")
                update_payload_glm = {"metadata": {"tag": "glm_example_updated", "format": "json_v2"}, "content": '{"message": "Updated second GLM KEM"}'}
                updated_kem2_glm = sdk.update_kem("ex_sdk_glm_002", update_payload_glm)
                if updated_kem2_glm:
                    logger.info(f"KEM ex_sdk_glm_002 updated. New content: '{updated_kem2_glm.get('content')}', new metadata: {updated_kem2_glm.get('metadata')}")
                    kem2_lpa_after_update = sdk.local_memory.get("ex_sdk_glm_002")
                    assert kem2_lpa_after_update and kem2_lpa_after_update.get("metadata", {}).get("format") == "json_v2", "Update of ex_sdk_glm_002 not reflected in LAM!"
                    logger.info("LAM check for ex_sdk_glm_002 after GLM update successful.")
                else: logger.error("Failed to update ex_sdk_glm_002 in GLM.")

            if "ex_sdk_glm_003" in successful_glm_ids:
                logger.info("\n--- Deleting ex_sdk_glm_003 from GLM ---")
                deleted_glm = sdk.delete_kem("ex_sdk_glm_003")
                if deleted_glm:
                    logger.info("KEM ex_sdk_glm_003 successfully deleted from GLM.")
                    assert not sdk.local_memory.contains("ex_sdk_glm_003"), "KEM ex_sdk_glm_003 still in LAM!"
                    assert sdk.get_kem("ex_sdk_glm_003") is None, "Deleted KEM ex_sdk_glm_003 still retrievable via get_kem!"
                    logger.info("Deletion checks for ex_sdk_glm_003 passed.")
                else: logger.error("Failed to delete ex_sdk_glm_003 from GLM.")

            logger.info("\n--- Demonstrating LRU in LAM (max_size=5) ---")
            sdk.local_memory.clear()
            for i in range(1, 7):
                kem_id_lru = f"kem_lru_{i}"
                sdk.local_memory.put(kem_id_lru, {"id": kem_id_lru, "content": f"LRU test {i}"})
            logger.info(f"LAM size after adding 6 items (max 5): {sdk.local_memory.current_size}")
            assert sdk.local_memory.current_size == 5, "LAM size does not match max_size."
            assert not sdk.local_memory.contains("kem_lru_1"), "kem_lru_1 (oldest) should have been evicted."
            assert sdk.local_memory.contains("kem_lru_2"), "kem_lru_2 should be in LAM."
            assert sdk.local_memory.contains("kem_lru_6"), "kem_lru_6 (newest) should be in LAM."
            logger.info("LRU checks in LAM passed.")

            logger.info("\n--- Testing SWM functions ---")
            kems_to_pub_swm = [
                {"id": "sdk_swm_pub_001", "content": "SWM pub 1", "metadata": {"tag": "swm_batch_example"}},
                {"id": "sdk_swm_pub_002", "content": "SWM pub 2", "metadata": {"tag": "swm_batch_example"}}
            ]
            s_pubs, f_pubs = sdk.publish_kems_to_swm_batch(kems_to_pub_swm, persist_to_glm=False)
            logger.info(f"SWM Batch Publish: Success {len(s_pubs)}, Failed {len(f_pubs)}")
            assert len(s_pubs) == len(kems_to_pub_swm); assert len(f_pubs) == 0

            time.sleep(0.5)
            sdk.local_memory.clear()
            loaded_kems_from_swm = sdk.load_kems_to_lpa_from_swm({"metadata_filters": {"tag": "swm_batch_example"}}, max_kems_to_load=5)
            logger.info(f"Loaded from SWM to LAM: {len(loaded_kems_from_swm)} KEMs.")
            assert len(loaded_kems_from_swm) == len(kems_to_pub_swm)
            for lk_swm in loaded_kems_from_swm:
                logger.info(f"  LAM: {lk_swm.get('id')} - {lk_swm.get('content')}")
                assert sdk.local_memory.contains(lk_swm.get('id'))

            counter_id_example = "sdk_example_counter"
            logger.info(f"Initial value of counter '{counter_id_example}': {sdk.get_distributed_counter(counter_id_example)}")
            sdk.increment_distributed_counter(counter_id_example, 3)
            logger.info(f"Value of counter '{counter_id_example}' after +3: {sdk.get_distributed_counter(counter_id_example)}")
            assert sdk.get_distributed_counter(counter_id_example) == 3
            sdk.increment_distributed_counter(counter_id_example, -1)
            logger.info(f"Value of counter '{counter_id_example}' after -1: {sdk.get_distributed_counter(counter_id_example)}")
            assert sdk.get_distributed_counter(counter_id_example) == 2

            res_id_example = "sdk_shared_resource_example"
            agent_example_id = "sdk_example_agent"
            logger.info(f"Attempting lock on '{res_id_example}' by agent '{agent_example_id}'...")
            with sdk.distributed_lock(res_id_example, agent_example_id, acquire_timeout_ms=1000, lease_duration_ms=10000) as lock_resp_example:
                if lock_resp_example and lock_resp_example.status == common_swm_pb2.LockStatusValue.ACQUIRED:
                    logger.info(f"Lock '{res_id_example}' acquired by agent '{agent_example_id}'. Lock ID: {lock_resp_example.lock_id}")
                    time.sleep(0.5)
                    logger.info(f"Work on resource '{res_id_example}' finished.")
                else:
                    status_name_example = common_swm_pb2.LockStatusValue.Name(lock_resp_example.status) if lock_resp_example else "N/A"
                    logger.warning(f"Failed to acquire lock '{res_id_example}'. Status: {status_name_example}")
            logger.info(f"Lock '{res_id_example}' should be released.")
            lock_info_after = sdk.get_distributed_lock_info(res_id_example)
            assert lock_info_after is not None and not lock_info_after.is_locked, "Lock was not released by context manager!"

            logger.info("\n--- Testing KPS functions (placeholder) ---")
            if sdk.config.kps_address: # Check if KPS is configured
                try:
                    # Example: Add a memory item to KPS
                    kps_kem_uri = "kem:example:doc:sdk_kps_test_001"
                    kps_content = "This is a test document for KPS via AgentSDK."
                    # Note: KPSClient.add_memory expects kem_uri and content directly.
                    # The AgentSDK could offer a higher-level wrapper if desired.
                    # For now, assume direct pass-through or simple wrapping if sdk.kps.add_memory existed.

                    # Placeholder: Direct call to a hypothetical sdk.kps.add_memory
                    # This assumes KPSClient has an add_memory method similar to what was built.
                    # add_response = sdk.kps.add_memory(kem_uri=kps_kem_uri, content=kps_content)
                    # if add_response and add_response.status_message == "OK": # Or however KPSClient signals success
                    #    logger.info(f"Successfully added/updated KEM '{kps_kem_uri}' in KPS.")
                    # else:
                    #    logger.error(f"Failed to add/update KEM '{kps_kem_uri}' in KPS. Response: {add_response}")
                    logger.info(f"KPS client is available at sdk.kps. Example KPS call placeholder executed for {kps_kem_uri}.")
                    logger.info("Actual KPS interaction example would involve calling methods on sdk.kps like sdk.kps.add_memory(...)")

                except RuntimeError as e_kps_runtime: # If KPS client wasn't configured/connected
                    logger.warning(f"KPS example skipped: {e_kps_runtime}")
                except grpc.RpcError as e_kps:
                    logger.error(f"KPS example: gRPC error communicating with KPS: {e_kps.code()} - {e_kps.details()}")
                except Exception as e_kps_other:
                    logger.error(f"KPS example: Unexpected error: {e_kps_other}", exc_info=True)
            else:
                logger.info("KPS not configured in DCSMClientSDKConfig, skipping KPS example.")


            logger.info("\n--- AgentSDK Example Run Successfully Completed! ---")

    except grpc.RpcError as e:
        logger.error(f"!!!!!!!!!! gRPC ERROR !!!!!!!!!")
        logger.error(f"Failed to connect to a DCSM server or other gRPC error occurred.")
        # Access config directly from the sdk instance if it was created
        if 'sdk' in locals() and hasattr(sdk, 'config'):
            if sdk.config.glm_address: logger.error(f"GLM Address from config: {sdk.config.glm_address}")
            if sdk.config.swm_address: logger.error(f"SWM Address from config: {sdk.config.swm_address}")
            if sdk.config.kps_address: logger.error(f"KPS Address from config: {sdk.config.kps_address}")
        logger.error("Please ensure DCSM servers (GLM, SWM, KPS) are running and accessible.")
        logger.error(f"Error Code: {e.code()}")
        logger.error(f"Details: {e.details()}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred in the example: {e}", exc_info=True)
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_example()
