# dcsm_agent_sdk_python/sdk.py
from .config import DCSMClientSDKConfig # Import the new config model
from .glm_client import GLMClient
from .swm_client import SWMClient
from .kps_client import KPSClient # Import the new KPSClient
from .local_memory import LocalAgentMemory
from .proto_utils import kem_proto_to_dict # For SWM event handling
import typing
from typing import Optional, List, Tuple, Callable
from datetime import datetime
import contextlib
import grpc # For grpc.RpcError
import threading # For background event handling
import logging # For logging

# Import generated proto types
from .generated_grpc_code import kem_pb2
from .generated_grpc_code import glm_service_pb2 as common_glm_pb2 # For KEMQuery
from .generated_grpc_code import swm_service_pb2 as common_swm_pb2 # For SWM specific types
from google.protobuf.timestamp_pb2 import Timestamp # For date parsing helper

logger = logging.getLogger(__name__)

class AgentSDK:
    def __init__(self, config: Optional[DCSMClientSDKConfig] = None):
        if config is None:
            self.config = DCSMClientSDKConfig() # Load from env vars or defaults
            logger.info("AgentSDK: No explicit config provided, loaded from environment/defaults.")
        else:
            self.config = config
            logger.info("AgentSDK: Initializing with provided DCSMClientSDKConfig.")

        self._glm_client: Optional[GLMClient] = None
        self._swm_client: Optional[SWMClient] = None
        self._kps_client: Optional[KPSClient] = None # Add KPS client attribute

        if self.config.glm_address:
            self._glm_client = GLMClient(
                server_address=self.config.glm_address,
                retry_max_attempts=self.config.retry_max_attempts,
                retry_initial_delay_s=self.config.retry_initial_delay_s,
                retry_backoff_factor=self.config.retry_backoff_factor,
                retry_jitter_fraction=self.config.retry_jitter_fraction,
                tls_enabled=self.config.tls_enabled,
                tls_ca_cert_path=self.config.tls_ca_cert_path,
                tls_client_cert_path=self.config.tls_client_cert_path,
                tls_client_key_path=self.config.tls_client_key_path,
                tls_server_override_authority=self.config.tls_server_override_authority
            )
            logger.info(f"AgentSDK: GLMClient configured for {self.config.glm_address}")

        if self.config.swm_address:
            self._swm_client = SWMClient(
                server_address=self.config.swm_address,
                retry_max_attempts=self.config.retry_max_attempts,
                retry_initial_delay_s=self.config.retry_initial_delay_s,
                retry_backoff_factor=self.config.retry_backoff_factor,
                retry_jitter_fraction=self.config.retry_jitter_fraction,
                tls_enabled=self.config.tls_enabled,
                tls_ca_cert_path=self.config.tls_ca_cert_path,
                tls_client_cert_path=self.config.tls_client_cert_path,
                tls_client_key_path=self.config.tls_client_key_path,
                tls_server_override_authority=self.config.tls_server_override_authority
            )
            logger.info(f"AgentSDK: SWMClient configured for {self.config.swm_address}")

        if self.config.kps_address:
            self._kps_client = KPSClient(
                server_address=self.config.kps_address,
                retry_max_attempts=self.config.retry_max_attempts,
                retry_initial_delay_s=self.config.retry_initial_delay_s,
                retry_backoff_factor=self.config.retry_backoff_factor,
                retry_jitter_fraction=self.config.retry_jitter_fraction,
                tls_enabled=self.config.tls_enabled,
                tls_ca_cert_path=self.config.tls_ca_cert_path,
                tls_client_cert_path=self.config.tls_client_cert_path,
                tls_client_key_path=self.config.tls_client_key_path,
                tls_server_override_authority=self.config.tls_server_override_authority
            )
            logger.info(f"AgentSDK: KPSClient configured for {self.config.kps_address}")

        self.local_memory = LocalAgentMemory(
            max_size=self.config.lpa_max_size,
            indexed_keys=self.config.lpa_indexed_keys if self.config.lpa_indexed_keys else []
        )

        if self.config.connect_on_init:
            if self._glm_client:
                try: self._glm_client.connect()
                except Exception as e_glm: logger.warning(f"AgentSDK: Warning - failed to connect to GLM during initialization: {e_glm}")
            if self._swm_client:
                try: self._swm_client.connect()
                except Exception as e_swm: logger.warning(f"AgentSDK: Warning - failed to connect to SWM during initialization: {e_swm}")
            if self._kps_client:
                try: self._kps_client.connect()
                except Exception as e_kps: logger.warning(f"AgentSDK: Warning - failed to connect to KPS during initialization: {e_kps}")

        logger.info(f"AgentSDK: Initialization complete. LAM indexed_keys: {self.config.lpa_indexed_keys if self.config.lpa_indexed_keys else []}.")

    @property
    def glm(self) -> GLMClient:
        if not self._glm_client:
            raise RuntimeError("AgentSDK: GLM service not configured or initialization failed.")
        self._glm_client._ensure_connected() # Ensure connection before returning
        return self._glm_client

    @property
    def swm(self) -> SWMClient:
        if not self._swm_client:
            raise RuntimeError("AgentSDK: SWM service not configured or initialization failed.")
        self._swm_client._ensure_connected()
        return self._swm_client

    @property
    def kps(self) -> KPSClient:
        if not self._kps_client:
            raise RuntimeError("AgentSDK: KPS service not configured or initialization failed.")
        self._kps_client._ensure_connected()
        return self._kps_client


    def query_local_memory(self, metadata_filters: Optional[dict] = None, ids: Optional[list[str]] = None) -> list[dict]:
        """Queries KEMs from Local Agent Memory (LAM) with filtering capabilities."""
        return self.local_memory.query(metadata_filters=metadata_filters, ids=ids)

    def get_kem(self, kem_id: str, force_remote: bool = False) -> Optional[dict]:
        logger.info(f"AgentSDK: Requesting get_kem for ID '{kem_id}', force_remote={force_remote}")
        if not force_remote:
            cached_kem = self.local_memory.get(kem_id)
            if cached_kem:
                logger.info(f"AgentSDK: KEM ID '{kem_id}' found in LAM.")
                return cached_kem

        logger.info(f"AgentSDK: KEM ID '{kem_id}' not found in LAM or remote fetch forced. Contacting GLM...")
        remote_kems_tuple = self.glm.retrieve_kems(ids_filter=[kem_id], page_size=1) # Use property
        kems_list_candidate = None
        if remote_kems_tuple and remote_kems_tuple[0] is not None:
            kems_list_candidate = remote_kems_tuple[0]

        if isinstance(kems_list_candidate, list) and len(kems_list_candidate) > 0:
            remote_kem = kems_list_candidate[0]
            if isinstance(remote_kem, dict) and remote_kem.get('id') == kem_id:
                self.local_memory.put(kem_id, remote_kem)
                return remote_kem
        logger.warning(f"AgentSDK: KEM ID '{kem_id}' not found in GLM or invalid response received.")
        return None

    def store_kems(self, kems_data: list[dict]) -> tuple[Optional[list[dict]], Optional[list[str]], Optional[str]]:
        """Stores a batch of KEMs in GLM and updates LAM with the server's response data."""
        logger.info(f"AgentSDK: Requesting store_kems for {len(kems_data)} KEMs.")
        stored_kems_dicts, failed_refs, error_msg = self.glm.batch_store_kems(kems_data) # Use property

        if stored_kems_dicts:
            logger.info(f"AgentSDK: Successfully stored/updated {len(stored_kems_dicts)} KEMs in GLM. Updating LAM.")
            for kem_dict in stored_kems_dicts:
                if kem_dict and 'id' in kem_dict:
                    self.local_memory.put(kem_dict['id'], kem_dict)
            return stored_kems_dicts, failed_refs, error_msg
        else:
            logger.error(f"AgentSDK: Failed to store KEMs in GLM. Error: {error_msg}")
            return None, failed_refs, error_msg

    def update_kem(self, kem_id: str, kem_data_update: dict) -> Optional[dict]:
        logger.info(f"AgentSDK: Requesting update_kem for ID '{kem_id}'.")
        updated_kem_on_server = self.glm.update_kem(kem_id, kem_data_update) # Use property
        if updated_kem_on_server:
            logger.info(f"AgentSDK: KEM ID '{kem_id}' successfully updated in GLM. Updating LAM.")
            self.local_memory.put(kem_id, updated_kem_on_server)
        else:
            logger.error(f"AgentSDK: Failed to update KEM ID '{kem_id}' in GLM.")
        return updated_kem_on_server

    def delete_kem(self, kem_id: str) -> bool:
        logger.info(f"AgentSDK: Requesting delete_kem for ID '{kem_id}'.")
        success = self.glm.delete_kem(kem_id) # Use property
        if success:
            logger.info(f"AgentSDK: KEM ID '{kem_id}' successfully deleted from GLM. Removing from LAM.")
            self.local_memory.delete(kem_id)
        else:
            logger.error(f"AgentSDK: Failed to delete KEM ID '{kem_id}' from GLM.")
        return success

    def close(self):
        logger.info("AgentSDK: Closing connections...")
        if self._glm_client:
            self._glm_client.close()
        if self._swm_client:
            self._swm_client.close()
        if self._kps_client: # Close KPS client
            self._kps_client.close()
        logger.info("AgentSDK: Connections closed.")

    def __enter__(self):
        # Connect on init now handles this if true, otherwise properties will connect on first use.
        # This __enter__ can ensure they are connected if not already.
        if self._glm_client and not self._glm_client.stub: # Check internal client, not property
            try: self._glm_client.connect()
            except Exception as e_glm: logger.warning(f"AgentSDK (__enter__): Warning - failed to connect to GLM: {e_glm}")

        if self._swm_client and not self._swm_client.stub:
            try: self._swm_client.connect()
            except Exception as e_swm: logger.warning(f"AgentSDK (__enter__): Warning - failed to connect to SWM: {e_swm}")

        if self._kps_client and not self._kps_client.stub:
            try: self._kps_client.connect()
            except Exception as e_kps: logger.warning(f"AgentSDK (__enter__): Warning - failed to connect to KPS: {e_kps}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # --- SWM Related High-Level Methods ---
    def publish_kems_to_swm_batch(self, kems_data: list[dict], persist_to_glm: bool = False) -> Tuple[List[dict], List[dict]]:
        """
        Publishes a batch of KEMs to SWM.
        Returns a tuple: (list of successfully published results, list of KEMs that failed to publish).
        Each item in the success list is a dict returned by SWMClient.publish_kem_to_swm.
        Each item in the failure list is the original KEM dict that failed.
        """
        # Use self.swm property which ensures client is available and connected
        swm_service = self.swm
        successfully_published_results = []
        failed_to_publish_kems = []
        for kem_data_item in kems_data:
            try:
                # swm_service is already ensured to be connected by the property access
                result = swm_service.publish_kem_to_swm(kem_data_item, persist_to_glm)
                if result and result.get("published_to_swm"):
                    successfully_published_results.append(result)
                else:
                    logger.warning(f"AgentSDK: Failed to publish KEM (ID: {kem_data_item.get('id', 'N/A')}) to SWM. Result: {result}")
                    failed_to_publish_kems.append(kem_data_item)
            except ConnectionError as e:
                logger.error(f"AgentSDK: Connection error publishing KEM (ID: {kem_data_item.get('id', 'N/A')}) to SWM: {e}")
                failed_to_publish_kems.append(kem_data_item)
            except grpc.RpcError as e:
                logger.error(f"AgentSDK: gRPC error publishing KEM (ID: {kem_data_item.get('id', 'N/A')}) to SWM: {e.code()} - {e.details()}")
                failed_to_publish_kems.append(kem_data_item)
            except Exception as e:
                logger.error(f"AgentSDK: Unexpected error publishing KEM (ID: {kem_data_item.get('id', 'N/A')}) to SWM: {e}", exc_info=True)
                failed_to_publish_kems.append(kem_data_item)
        logger.info(f"AgentSDK: Batch publish to SWM: {len(successfully_published_results)} succeeded, {len(failed_to_publish_kems)} failed.")
        return successfully_published_results, failed_to_publish_kems

    def load_kems_to_lpa_from_swm(self, kem_query_dict: dict, max_kems_to_load: int = 0) -> List[dict]:
        """
        Loads KEMs from SWM into Local Agent Memory (LAM) based on a query.

        :param kem_query_dict: Dictionary representing KEMQuery (e.g., {"metadata_filters": {"key": "value"}, "ids": ["id1"]}).
                               AgentSDK converts it to a proto KEMQuery.
        :param max_kems_to_load: Maximum number of KEMs to load. If 0, loads one page (SWM's default size).
                                 If > 0, attempts to load up to this number, possibly making multiple paginated requests.
        :return: List of KEMs (as dicts) loaded into LAM.
        """
        swm_service = self.swm # Use property
        query_proto = common_glm_pb2.KEMQuery()
        if kem_query_dict.get("text_query"): query_proto.text_query = kem_query_dict["text_query"]
        if kem_query_dict.get("embedding_query"): query_proto.embedding_query.extend(kem_query_dict["embedding_query"])
        if kem_query_dict.get("metadata_filters"):
            for key, value in kem_query_dict["metadata_filters"].items(): query_proto.metadata_filters[key] = str(value)
        if kem_query_dict.get("ids"): query_proto.ids.extend(kem_query_dict["ids"])

        def _parse_date_to_timestamp(date_input, ts_proto_field: Timestamp):
            if isinstance(date_input, str):
                try: ts_proto_field.FromJsonString(date_input if date_input.endswith("Z") else date_input + "Z"); return True
                except Exception as e: logger.error(f"AgentSDK: Error parsing date string '{date_input}': {e}"); return False
            elif isinstance(date_input, datetime):
                try: ts_proto_field.FromDatetime(date_input); return True
                except Exception as e: logger.error(f"AgentSDK: Error converting datetime to Timestamp: {e}"); return False
            elif date_input is not None: logger.warning(f"AgentSDK: Unsupported type for date field: {type(date_input)}")
            return False
        if "created_at_start" in kem_query_dict: _parse_date_to_timestamp(kem_query_dict["created_at_start"], query_proto.created_at_start)
        if "created_at_end" in kem_query_dict: _parse_date_to_timestamp(kem_query_dict["created_at_end"], query_proto.created_at_end)
        if "updated_at_start" in kem_query_dict: _parse_date_to_timestamp(kem_query_dict["updated_at_start"], query_proto.updated_at_start)
        if "updated_at_end" in kem_query_dict: _parse_date_to_timestamp(kem_query_dict["updated_at_end"], query_proto.updated_at_end)

        loaded_to_lpa_kems = []; current_page_token = ""; kems_loaded_count = 0

        keep_fetching = True
        while keep_fetching:
            try:
                # swm_service is already ensured to be connected
                page_size_for_this_request = 0
                if max_kems_to_load > 0:
                    remaining_to_load = max_kems_to_load - kems_loaded_count
                    if remaining_to_load <= 0: keep_fetching = False; break
                    page_size_for_this_request = remaining_to_load

                kems_page_list, next_page_token = swm_service.query_swm( # Use swm_service
                    kem_query=query_proto,
                    page_size=page_size_for_this_request,
                    page_token=current_page_token
                )
                if kems_page_list:
                    for kem_dict in kems_page_list:
                        if kem_dict and 'id' in kem_dict:
                            self.local_memory.put(kem_dict['id'], kem_dict); loaded_to_lpa_kems.append(kem_dict); kems_loaded_count += 1
                            if max_kems_to_load > 0 and kems_loaded_count >= max_kems_to_load:
                                keep_fetching = False; break

                if not next_page_token or not kems_page_list: keep_fetching = False
                current_page_token = next_page_token if next_page_token else ""
                if max_kems_to_load == 0: keep_fetching = False

            except Exception as e: logger.error(f"AgentSDK: Error loading KEMs from SWM: {e}", exc_info=True); keep_fetching = False
        logger.info(f"AgentSDK: Loaded {len(loaded_to_lpa_kems)} KEMs from SWM into LAM.")
        return loaded_to_lpa_kems

    def handle_swm_events(self, topics: Optional[List[common_swm_pb2.SubscriptionTopic]] = None,
                          event_callback: Optional[Callable[[common_swm_pb2.SWMMemoryEvent], None]] = None,
                          agent_id: str = "default_sdk_agent_handler", auto_update_lpa: bool = False,
                          requested_queue_size: int = 0, # Added new parameter
                          run_in_background: bool = False) -> Optional[threading.Thread]:
        """
        Subscribes to SWM events and processes them using a callback function.
        Can automatically update LAM based on events.
        :param requested_queue_size: Optional: Client's requested size for its event queue on SWM server.
        """
        swm_service = self.swm # Use property
        # Connection is ensured by property access

        event_stream_generator = swm_service.subscribe_to_swm_events( # Use swm_service
            topics=topics,
            agent_id=agent_id,
            requested_queue_size=requested_queue_size
        )
        if not event_stream_generator: logger.error(f"AgentSDK: Failed to get event stream generator from SWMClient for agent_id {agent_id}."); return None

        logger.info(f"AgentSDK: Starting to listen for SWM events for agent_id {agent_id} (req_q_size: {requested_queue_size})...")
        def _process_events():
            try:
                for event in event_stream_generator: # type: ignore
                    if event_callback:
                        try: event_callback(event)
                        except Exception as cb_exc: logger.error(f"AgentSDK: Error in SWM event callback: {cb_exc}", exc_info=True)

                    if auto_update_lpa and event.HasField("kem_payload") and event.kem_payload.id:
                        kem_payload_dict = kem_proto_to_dict(event.kem_payload) # Use global util
                        if event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED or \
                           event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_UPDATED:
                            self.local_memory.put(event.kem_payload.id, kem_payload_dict)
                            logger.info(f"AgentSDK (LAM AutoUpdate): KEM ID '{event.kem_payload.id}' {common_swm_pb2.SWMMemoryEvent.EventType.Name(event.event_type)} in LAM.")
                        elif event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_EVICTED:
                            self.local_memory.delete(event.kem_payload.id)
                            logger.info(f"AgentSDK (LAM AutoUpdate): KEM ID '{event.kem_payload.id}' removed from LAM (KEM_EVICTED).")
            except Exception as e_proc: logger.error(f"AgentSDK: Error in SWM event processing loop ({agent_id}): {e_proc}", exc_info=True)
            finally: logger.info(f"AgentSDK: Finished listening for SWM events for agent_id {agent_id}.")

        if run_in_background:
            thread = threading.Thread(target=_process_events, daemon=True); thread.start(); return thread
        else: _process_events(); return None

    # --- Distributed Lock High-Level Methods ---
    def acquire_distributed_lock(self, resource_id: str, agent_id: str, timeout_ms: int = 0,
                                 lease_duration_ms: int = 0, rpc_timeout: int = 5) -> Optional[common_swm_pb2.AcquireLockResponse]:
        """Acquires a distributed lock on a resource via SWM."""
        return self.swm.acquire_lock(resource_id, agent_id, timeout_ms, lease_duration_ms, rpc_timeout) # Use property

    def release_distributed_lock(self, resource_id: str, agent_id: str, lock_id: Optional[str] = None,
                                 rpc_timeout: int = 5) -> Optional[common_swm_pb2.ReleaseLockResponse]:
        """Releases a distributed lock on a resource via SWM."""
        return self.swm.release_lock(resource_id, agent_id, lock_id, rpc_timeout) # Use property

    def get_distributed_lock_info(self, resource_id: str, rpc_timeout: int = 5) -> Optional[common_swm_pb2.LockInfo]:
        """Gets information about the state of a distributed lock for a resource."""
        return self.swm.get_lock_info(resource_id, rpc_timeout) # Use property

    @contextlib.contextmanager
    def distributed_lock(self, resource_id: str, agent_id: str, acquire_timeout_ms: int = 10000,
                         lease_duration_ms: int = 60000, rpc_timeout: int = 5):
        """Context manager for convenient handling of distributed locks."""
        acquired_lock_response: Optional[common_swm_pb2.AcquireLockResponse] = None
        acquired_lock_id: Optional[str] = None
        lock_was_successfully_acquired_by_this_context = False
        try:
            logger.info(f"AgentSDK (distributed_lock): Attempting to acquire lock for '{resource_id}' by agent '{agent_id}'...")
            acquired_lock_response = self.acquire_distributed_lock(resource_id, agent_id, acquire_timeout_ms, lease_duration_ms, rpc_timeout)
            if acquired_lock_response and \
               (acquired_lock_response.status == common_swm_pb2.LockStatusValue.ACQUIRED):
                logger.info(f"AgentSDK (distributed_lock): Lock for '{resource_id}' successfully acquired. Lock ID: {acquired_lock_response.lock_id}")
                acquired_lock_id = acquired_lock_response.lock_id
                lock_was_successfully_acquired_by_this_context = True
                yield acquired_lock_response
            elif acquired_lock_response and \
                 acquired_lock_response.status == common_swm_pb2.LockStatusValue.ALREADY_HELD_BY_YOU:
                logger.info(f"AgentSDK (distributed_lock): Lock for '{resource_id}' is already held by this agent. Lock ID: {acquired_lock_response.lock_id}. Lease might have been updated.")
                acquired_lock_id = acquired_lock_response.lock_id
                lock_was_successfully_acquired_by_this_context = True
                yield acquired_lock_response
            else:
                status_name = common_swm_pb2.LockStatusValue.Name(acquired_lock_response.status) if acquired_lock_response else "N/A"
                message = acquired_lock_response.message if acquired_lock_response else "No response"
                logger.warning(f"AgentSDK (distributed_lock): Failed to acquire lock for '{resource_id}'. Status: {status_name}, Message: {message}")
                yield acquired_lock_response
        finally:
            if lock_was_successfully_acquired_by_this_context and acquired_lock_id:
                logger.info(f"AgentSDK (distributed_lock): Releasing lock for '{resource_id}', Lock ID: {acquired_lock_id} by agent '{agent_id}'.")
                self.release_distributed_lock(resource_id, agent_id, lock_id=acquired_lock_id, rpc_timeout=rpc_timeout)
            elif acquired_lock_response :
                 logger.debug(f"AgentSDK (distributed_lock): Lock for '{resource_id}' was not in ACQUIRED or ALREADY_HELD_BY_YOU state (status: {common_swm_pb2.LockStatusValue.Name(acquired_lock_response.status)}), no release needed by this context.")
            else:
                 logger.debug(f"AgentSDK (distributed_lock): No lock response received for '{resource_id}', no release needed.")

    # --- Distributed Counter High-Level Methods ---
    def increment_distributed_counter(self, counter_id: str, increment_by: int = 1, rpc_timeout: int = 5) -> Optional[int]:
        """Increments (or decrements) a distributed counter in SWM."""
        response = self.swm.increment_counter(counter_id, increment_by, rpc_timeout) # Use property
        if response and (not response.status_message or "successfully updated" in response.status_message.lower() or not "error" in response.status_message.lower()):
            return response.current_value
        else: logger.error(f"AgentSDK: Error incrementing counter '{counter_id}'. Message: {response.status_message if response else 'No response'}"); return None

    def get_distributed_counter(self, counter_id: str, rpc_timeout: int = 5) -> Optional[int]:
        """Gets the current value of a distributed counter from SWM."""
        response = self.swm.get_counter(counter_id, rpc_timeout) # Use property
        if response and (not response.status_message or
                         "successfully retrieved" in response.status_message.lower() or
                         "not found, returned default value 0" in response.status_message.lower() or
                         not "error" in response.status_message.lower()):
            return response.current_value
        else: logger.error(f"AgentSDK: Error getting counter '{counter_id}'. Message: {response.status_message if response else 'No response'}"); return None

if __name__ == '__main__':
    logger.info("Running AgentSDK example (requires GLM, SWM, and potentially KPS servers)")
    # This __main__ block is a simplified test. See example.py for more comprehensive usage.

    if os.getenv("RUN_AGENT_SDK_EXAMPLE") == "true":
        try:
            # Load configuration (from .env file or environment variables)
            sdk_config = DCSMClientSDKConfig()
            logger.info(f"SDK Config loaded: GLM @ {sdk_config.glm_address}, SWM @ {sdk_config.swm_address}, KPS @ {sdk_config.kps_address}")

            with AgentSDK(config=sdk_config) as sdk: # New constructor with config
                logger.info("\n--- Testing SWM functions (via sdk.swm) ---")
                kems_to_pub_swm = [
                    {"id": "sdk_swm_pub_001", "content": "SWM pub 1", "metadata": {"tag": "swm_batch"}},
                    {"id": "sdk_swm_pub_002", "content": "SWM pub 2", "metadata": {"tag": "swm_batch"}}
                ]
                s_pubs, f_pubs = sdk.publish_kems_to_swm_batch(kems_to_pub_swm, persist_to_glm=False)
                logger.info(f"SWM Batch Publish: Success {len(s_pubs)}, Failed {len(f_pubs)}")

                time.sleep(0.2)
                loaded_kems = sdk.load_kems_to_lpa_from_swm({"metadata_filters": {"tag": "swm_batch"}}, max_kems_to_load=5)
                logger.info(f"Loaded from SWM to LAM: {len(loaded_kems)} KEMs.")
                for lk in loaded_kems: logger.info(f"  LAM: {lk.get('id')} - {lk.get('content')}")

                counter_id = "my_test_counter_sdk"
                logger.info(f"Initial value of counter '{counter_id}': {sdk.get_distributed_counter(counter_id)}")
                sdk.increment_distributed_counter(counter_id, 5)
                logger.info(f"Value of counter '{counter_id}' after +5: {sdk.get_distributed_counter(counter_id)}")
                sdk.increment_distributed_counter(counter_id, -2)
                logger.info(f"Value of counter '{counter_id}' after -2: {sdk.get_distributed_counter(counter_id)}")

                res_id = "my_shared_resource_sdk_example"
                agent1_id = "sdk_example_agent_1"
                logger.info(f"Attempting lock on '{res_id}' by agent '{agent1_id}'...")
                with sdk.distributed_lock(res_id, agent1_id, acquire_timeout_ms=1000, lease_duration_ms=10000) as lock_resp:
                    if lock_resp and lock_resp.status == common_swm_pb2.LockStatusValue.ACQUIRED:
                        logger.info(f"Lock '{res_id}' acquired by '{agent1_id}'. Lock ID: {lock_resp.lock_id}")
                        time.sleep(1)
                        logger.info(f"Work on resource '{res_id}' finished.")
                    else:
                        status_name = common_swm_pb2.LockStatusValue.Name(lock_resp.status) if lock_resp else "N/A"
                        logger.warning(f"Failed to acquire lock '{res_id}'. Status: {status_name}")
                logger.info(f"Lock '{res_id}' should be released.")

        except grpc.RpcError as e:
            logger.error(f"gRPC ERROR in SDK example: {e.code()} - {e.details()}", exc_info=True)
        except Exception as e_main:
            logger.error(f"Unexpected error in AgentSDK example: {e_main}", exc_info=True)
    else:
        logger.info("Environment variable RUN_AGENT_SDK_EXAMPLE not set to 'true', main SDK example will not run.")
