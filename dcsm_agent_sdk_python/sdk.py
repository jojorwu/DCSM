# dcsm_agent_sdk_python/sdk.py
from .config import DCSMClientSDKConfig
from .glm_client import GLMClient
from .swm_client import SWMClient
from .kps_client import KPSClient
from .local_memory import LocalAgentMemory
from .proto_utils import kem_proto_to_dict
import typing
from typing import Optional, List, Tuple, Callable, Union, Dict # Added Dict
from datetime import datetime
import contextlib
import grpc
import threading
import logging
import time
import random # For jitter in SWM event handler

from .generated_grpc_code import kem_pb2
from .generated_grpc_code import glm_service_pb2 as common_glm_pb2
from .generated_grpc_code import swm_service_pb2 as common_swm_pb2
from google.protobuf.timestamp_pb2 import Timestamp

logger = logging.getLogger(__name__)

class AgentSDK:
    def __init__(self, config: Optional[DCSMClientSDKConfig] = None):
        if config is None:
            self.config = DCSMClientSDKConfig()
            logger.info("AgentSDK: No explicit config provided, loaded from environment/defaults.")
        else:
            self.config = config
            logger.info("AgentSDK: Initializing with provided DCSMClientSDKConfig.")

        self._glm_client: Optional[GLMClient] = None
        self._swm_client: Optional[SWMClient] = None
        self._kps_client: Optional[KPSClient] = None

        # For SWM event handlers
        self._swm_event_handlers: Dict[str, Tuple[threading.Thread, threading.Event]] = {}


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
                tls_server_override_authority=self.config.tls_server_override_authority,
                batch_store_kems_timeout_s=self.config.glm_batch_store_kems_timeout_s,
                retrieve_kems_timeout_s=self.config.glm_retrieve_kems_timeout_s,
                update_kem_timeout_s=self.config.glm_update_kem_timeout_s,
                delete_kem_timeout_s=self.config.glm_delete_kem_timeout_s
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
                tls_server_override_authority=self.config.tls_server_override_authority,
                publish_kem_timeout_s=self.config.swm_publish_kem_timeout_s,
                query_swm_timeout_s=self.config.swm_query_swm_timeout_s,
                load_kems_timeout_s=self.config.swm_load_kems_timeout_s,
                lock_rpc_timeout_s=self.config.swm_lock_rpc_timeout_s,
                counter_rpc_timeout_s=self.config.swm_counter_rpc_timeout_s
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
                tls_server_override_authority=self.config.tls_server_override_authority,
                process_raw_data_timeout_s=self.config.kps_process_raw_data_timeout_s
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
        self._glm_client._ensure_connected()
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
        return self.local_memory.query(metadata_filters=metadata_filters, ids=ids)

    def get_kem(self, kem_id: str, force_remote: bool = False, timeout: Optional[float] = None) -> Optional[dict]:
        logger.info(f"AgentSDK: Requesting get_kem for ID '{kem_id}', force_remote={force_remote}")
        if not force_remote:
            cached_kem = self.local_memory.get(kem_id)
            if cached_kem:
                logger.info(f"AgentSDK: KEM ID '{kem_id}' found in LAM.")
                return cached_kem

        logger.info(f"AgentSDK: KEM ID '{kem_id}' not found in LAM or remote fetch forced. Contacting GLM...")
        kems_list_candidate, _ = self.glm.retrieve_kems(ids_filter=[kem_id], page_size=1)

        if isinstance(kems_list_candidate, list) and len(kems_list_candidate) > 0:
            remote_kem = kems_list_candidate[0]
            if isinstance(remote_kem, dict) and remote_kem.get('id') == kem_id:
                self.local_memory.put(kem_id, remote_kem)
                return remote_kem
        logger.warning(f"AgentSDK: KEM ID '{kem_id}' not found in GLM or invalid response received.")
        return None

    def store_kems(self, kems_data: list[dict], timeout: Optional[float] = None) -> tuple[Optional[list[dict]], Optional[list[str]], Optional[str]]:
        logger.info(f"AgentSDK: Requesting store_kems for {len(kems_data)} KEMs.")
        stored_kems_dicts, failed_refs, error_msg = self.glm.batch_store_kems(kems_data)

        if stored_kems_dicts:
            logger.info(f"AgentSDK: Successfully stored/updated {len(stored_kems_dicts)} KEMs in GLM. Updating LAM.")
            for kem_dict in stored_kems_dicts:
                if kem_dict and 'id' in kem_dict:
                    self.local_memory.put(kem_dict['id'], kem_dict)
        else:
            logger.error(f"AgentSDK: Failed to store KEMs in GLM. Error: {error_msg}")
        return stored_kems_dicts, failed_refs, error_msg

    def update_kem(self, kem_id: str, kem_data_update: dict, timeout: Optional[float] = None) -> Optional[dict]:
        logger.info(f"AgentSDK: Requesting update_kem for ID '{kem_id}'.")
        updated_kem_on_server = self.glm.update_kem(kem_id, kem_data_update)
        if updated_kem_on_server:
            logger.info(f"AgentSDK: KEM ID '{kem_id}' successfully updated in GLM. Updating LAM.")
            self.local_memory.put(kem_id, updated_kem_on_server)
        else:
            logger.error(f"AgentSDK: Failed to update KEM ID '{kem_id}' in GLM.")
        return updated_kem_on_server

    def delete_kem(self, kem_id: str, timeout: Optional[float] = None) -> bool:
        logger.info(f"AgentSDK: Requesting delete_kem for ID '{kem_id}'.")
        success = self.glm.delete_kem(kem_id)
        if success:
            logger.info(f"AgentSDK: KEM ID '{kem_id}' successfully deleted from GLM. Removing from LAM.")
            self.local_memory.delete(kem_id)
        else:
            logger.error(f"AgentSDK: Failed to delete KEM ID '{kem_id}' from GLM.")
        return success

    def process_data_for_kps(self, data_id: str, content_type: str, raw_content: Union[str, bytes],
                               initial_metadata: Optional[dict] = None, timeout: Optional[float] = None) -> Optional[kps_service_pb2.ProcessRawDataResponse]:
        logger.info(f"AgentSDK: Requesting KPS to process data_id '{data_id}'.")
        content_bytes = raw_content if isinstance(raw_content, bytes) else raw_content.encode('utf-8')
        return self.kps.process_raw_data(data_id, content_type, content_bytes, initial_metadata, timeout=timeout)

    def close(self):
        logger.info("AgentSDK: Closing SWM event handlers and service connections...")
        for agent_handler_id, (_, stop_flag) in list(self._swm_event_handlers.items()): # Iterate over a copy
            logger.info(f"AgentSDK: Signaling SWM event handler {agent_handler_id} to stop...")
            stop_flag.set()

        for agent_handler_id, (thread, _) in list(self._swm_event_handlers.items()): # Iterate over a copy
            if thread.is_alive():
                logger.info(f"AgentSDK: Waiting for SWM event handler thread {agent_handler_id} to join...")
                thread.join(timeout=5.0)
                if thread.is_alive():
                    logger.warning(f"AgentSDK: SWM event handler thread {agent_handler_id} did not join in time.")
        self._swm_event_handlers.clear()

        if self._glm_client: self._glm_client.close()
        if self._swm_client: self._swm_client.close()
        if self._kps_client: self._kps_client.close()
        logger.info("AgentSDK: Connections closed.")

    def __enter__(self):
        if self._glm_client and not self._glm_client.stub:
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

    def publish_kems_to_swm_batch(self, kems_data: list[dict], persist_to_glm: bool = False, rpc_timeout: Optional[float] = None) -> Tuple[List[dict], List[dict]]:
        swm_service = self.swm
        successfully_published_results = []
        failed_to_publish_kems = []
        for kem_data_item in kems_data:
            try:
                result = swm_service.publish_kem_to_swm(kem_data_item, persist_to_glm, timeout=rpc_timeout)
                if result and result.get("published_to_swm"):
                    successfully_published_results.append(result)
                else:
                    logger.warning(f"AgentSDK: Failed to publish KEM (ID: {kem_data_item.get('id', 'N/A')}) to SWM. Result: {result}")
                    failed_to_publish_kems.append(kem_data_item)
            except Exception as e:
                logger.error(f"AgentSDK: Error publishing KEM (ID: {kem_data_item.get('id', 'N/A')}) to SWM: {e}", exc_info=True)
                failed_to_publish_kems.append(kem_data_item)
        logger.info(f"AgentSDK: Batch publish to SWM: {len(successfully_published_results)} succeeded, {len(failed_to_publish_kems)} failed.")
        return successfully_published_results, failed_to_publish_kems

    def load_kems_to_lpa_from_swm(self, kem_query_dict: dict, max_kems_to_load: int = 0, rpc_timeout: Optional[float] = None) -> List[dict]:
        swm_service = self.swm
        query_proto = common_glm_pb2.KEMQuery()
        if kem_query_dict.get("text_query"): query_proto.text_query = kem_query_dict["text_query"]
        if kem_query_dict.get("embedding_query"): query_proto.embedding_query.extend(kem_query_dict["embedding_query"])
        if kem_query_dict.get("metadata_filters"):
            for key, value in kem_query_dict["metadata_filters"].items(): query_proto.metadata_filters[key] = str(value)
        if kem_query_dict.get("ids"): query_proto.ids.extend(kem_query_dict["ids"])

        def _parse_date_to_timestamp(date_input, ts_proto_field: Timestamp):
            if isinstance(date_input, str):
                try: ts_proto_field.FromJsonString(date_input if date_input.endswith("Z") else date_input + "Z"); return True
                except Exception as e_ts: logger.error(f"AgentSDK: Error parsing date string '{date_input}': {e_ts}"); return False
            elif isinstance(date_input, datetime):
                try: ts_proto_field.FromDatetime(date_input); return True
                except Exception as e_dt: logger.error(f"AgentSDK: Error converting datetime to Timestamp: {e_dt}"); return False
            elif date_input is not None: logger.warning(f"AgentSDK: Unsupported type for date field: {type(date_input)}")
            return False
        if "created_at_start" in kem_query_dict: _parse_date_to_timestamp(kem_query_dict["created_at_start"], query_proto.created_at_start)
        if "created_at_end" in kem_query_dict: _parse_date_to_timestamp(kem_query_dict["created_at_end"], query_proto.created_at_end)
        if "updated_at_start" in kem_query_dict: _parse_date_to_timestamp(kem_query_dict["updated_at_start"], query_proto.updated_at_start)
        if "updated_at_end" in kem_query_dict: _parse_date_to_timestamp(kem_query_dict["updated_at_end"], query_proto.updated_at_end)

        loaded_to_lpa_kems = []; current_page_token = ""; kems_loaded_count = 0
        keep_fetching = True
        page_size_default = self.config.swm_query_swm_page_size_default

        while keep_fetching:
            try:
                page_size_for_this_request = page_size_default
                if max_kems_to_load > 0:
                    remaining_to_load = max_kems_to_load - kems_loaded_count
                    if remaining_to_load <= 0: keep_fetching = False; break
                    page_size_for_this_request = min(remaining_to_load, page_size_default)

                kems_page_list, next_page_token = swm_service.query_swm(
                    kem_query=query_proto,
                    page_size=page_size_for_this_request,
                    page_token=current_page_token,
                    timeout=rpc_timeout
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

    def _process_swm_events_resiliently(self,
                                       topics: Optional[List[common_swm_pb2.SubscriptionTopic]],
                                       event_callback: Optional[Callable[[common_swm_pb2.SWMMemoryEvent], None]],
                                       agent_id: str,
                                       auto_update_lpa: bool,
                                       requested_queue_size: int,
                                       stop_event: threading.Event):
        if not self.config.swm_event_handler_enabled:
            logger.info(f"AgentSDK: SWM event handler is disabled by configuration for agent_id {agent_id}.")
            return

        current_retry_delay = self.config.swm_event_initial_retry_delay_s
        retry_attempts = 0
        swm_service = self.swm # Ensures connection before loop

        logger.info(f"AgentSDK: Resilient SWM event handler thread started for agent_id {agent_id}.")

        while not stop_event.is_set():
            event_stream_generator = None
            try:
                logger.info(f"AgentSDK: Attempting to subscribe to SWM events for agent_id {agent_id} (attempt {retry_attempts + 1}).")
                event_stream_generator = swm_service.subscribe_to_swm_events(
                    topics=topics,
                    agent_id=agent_id,
                    requested_queue_size=requested_queue_size
                )

                if not event_stream_generator:
                    logger.warning(f"AgentSDK: Failed to get SWM event stream (SWMClient returned None) for {agent_id}. Retrying...")
                    raise RuntimeError("SWMClient.subscribe_to_swm_events returned None")

                logger.info(f"AgentSDK: Successfully subscribed to SWM events for {agent_id}.")
                retry_attempts = 0
                current_retry_delay = self.config.swm_event_initial_retry_delay_s

                for event in event_stream_generator:
                    if stop_event.is_set():
                        logger.info(f"AgentSDK: SWM event handler for {agent_id} received stop signal during event processing.")
                        break

                    if event_callback:
                        try: event_callback(event)
                        except Exception as cb_exc: logger.error(f"AgentSDK: Error in SWM event callback for {agent_id}: {cb_exc}", exc_info=True)

                    if auto_update_lpa and event.HasField("kem_payload") and event.kem_payload.id:
                        kem_payload_dict = kem_proto_to_dict(event.kem_payload)
                        if event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED or \
                           event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_UPDATED:
                            self.local_memory.put(event.kem_payload.id, kem_payload_dict)
                            logger.debug(f"AgentSDK (LAM AutoUpdate): KEM '{event.kem_payload.id}' {common_swm_pb2.SWMMemoryEvent.EventType.Name(event.event_type)} in LAM for {agent_id}.")
                        elif event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_EVICTED:
                            self.local_memory.delete(event.kem_payload.id)
                            logger.debug(f"AgentSDK (LAM AutoUpdate): KEM '{event.kem_payload.id}' removed from LAM (EVICTED) for {agent_id}.")

                if stop_event.is_set(): break

                logger.info(f"AgentSDK: SWM event stream for {agent_id} ended normally but unexpectedly. Re-subscribing after delay.")
                if stop_event.wait(timeout=self.config.swm_event_stream_normal_termination_reconnect_delay_s):
                    break
                continue # Retry by continuing the while loop

            except grpc.RpcError as e:
                logger.warning(f"AgentSDK: gRPC error in SWM event stream for {agent_id}: {e.code()} - {e.details()}.")
                if e.code() == grpc.StatusCode.CANCELLED:
                    logger.info(f"AgentSDK: SWM event stream for {agent_id} was cancelled. Not retrying.")
                    break
            except Exception as e_stream_setup:
                logger.error(f"AgentSDK: Error in SWM event subscription or processing for {agent_id}: {e_stream_setup}", exc_info=True)

            finally:
                if hasattr(event_stream_generator, 'cancel') and callable(event_stream_generator.cancel): # type: ignore
                    try: event_stream_generator.cancel() # type: ignore
                    except Exception: pass
                elif hasattr(event_stream_generator, 'close') and callable(event_stream_generator.close): # type: ignore
                    try: event_stream_generator.close() # type: ignore
                    except Exception: pass

            if stop_event.is_set(): break

            retry_attempts += 1
            if self.config.swm_event_max_retries != -1 and retry_attempts > self.config.swm_event_max_retries:
                logger.error(f"AgentSDK: Max retries ({self.config.swm_event_max_retries}) reached for SWM event subscription for {agent_id}. Stopping handler.")
                break

            jitter = random.uniform(-self.config.swm_event_jitter_fraction, self.config.swm_event_jitter_fraction) * current_retry_delay
            sleep_duration = max(0, current_retry_delay + jitter)

            logger.info(f"AgentSDK: Waiting {sleep_duration:.2f}s before SWM event re-subscription attempt {retry_attempts} for {agent_id}.")

            if stop_event.wait(timeout=sleep_duration):
                logger.info(f"AgentSDK: SWM event handler for {agent_id} received stop signal during retry wait.")
                break
            current_retry_delay = min(self.config.swm_event_max_retry_delay_s, current_retry_delay * self.config.swm_event_backoff_factor)

        logger.info(f"AgentSDK: SWM event handler thread for agent_id {agent_id} has finished.")
        # Remove from active handlers when thread exits
        if agent_id in self._swm_event_handlers:
            del self._swm_event_handlers[agent_id]


    def handle_swm_events(self, topics: Optional[List[common_swm_pb2.SubscriptionTopic]] = None,
                          event_callback: Optional[Callable[[common_swm_pb2.SWMMemoryEvent], None]] = None,
                          agent_id: str = "default_sdk_agent_handler", auto_update_lpa: bool = False,
                          requested_queue_size: int = 0,
                          run_in_background: bool = False) -> Optional[threading.Thread]:
        if not self.config.swm_event_handler_enabled and run_in_background:
            logger.info(f"AgentSDK: SWM event handling is disabled by swm_event_handler_enabled=False. Not starting background listener for {agent_id}.")
            return None

        if run_in_background:
            if agent_id in self._swm_event_handlers:
                thread, stop_event = self._swm_event_handlers[agent_id]
                if thread.is_alive():
                    logger.warning(f"AgentSDK: SWM event handler for agent_id '{agent_id}' is already running.")
                    return thread
                else: # Thread is dead, clean up
                    logger.info(f"AgentSDK: Found dead SWM event handler thread for agent_id '{agent_id}'. Cleaning up before restart.")
                    del self._swm_event_handlers[agent_id]

            stop_event = threading.Event()
            thread_args = (topics, event_callback, agent_id, auto_update_lpa, requested_queue_size, stop_event)
            thread = threading.Thread(target=self._process_swm_events_resiliently, args=thread_args, daemon=True)
            thread.name = f"SWMEventHandler-{agent_id}"

            self._swm_event_handlers[agent_id] = (thread, stop_event)
            thread.start()
            return thread
        else:
            # For foreground execution, create a dummy stop event (won't be used to stop externally)
            # Resilience is still applied, but it will block the caller.
            dummy_stop_event = threading.Event()
            self._process_swm_events_resiliently(topics, event_callback, agent_id, auto_update_lpa, requested_queue_size, dummy_stop_event)
            return None

    def stop_swm_event_handler(self, agent_id: str, timeout: float = 5.0) -> bool:
        """Stops a specific SWM event handler thread."""
        if agent_id in self._swm_event_handlers:
            thread, stop_event = self._swm_event_handlers[agent_id]
            logger.info(f"AgentSDK: Signaling SWM event handler for agent_id '{agent_id}' to stop...")
            stop_event.set()
            if thread.is_alive():
                thread.join(timeout=timeout)
                if thread.is_alive():
                    logger.warning(f"AgentSDK: SWM event handler thread for agent_id '{agent_id}' did not join in time.")
                    return False
            logger.info(f"AgentSDK: SWM event handler for agent_id '{agent_id}' stopped.")
            # Removal from dict is now handled in _process_swm_events_resiliently's finally block
            return True
        else:
            logger.info(f"AgentSDK: No active SWM event handler found for agent_id '{agent_id}'.")
            return False

    def acquire_distributed_lock(self, resource_id: str, agent_id: str, timeout_ms: int = 0,
                                 lease_duration_ms: int = 0, rpc_timeout: Optional[float] = None) -> Optional[common_swm_pb2.AcquireLockResponse]:
        return self.swm.acquire_lock(resource_id, agent_id, timeout_ms, lease_duration_ms, rpc_timeout=rpc_timeout)

    def release_distributed_lock(self, resource_id: str, agent_id: str, lock_id: Optional[str] = None,
                                 rpc_timeout: Optional[float] = None) -> Optional[common_swm_pb2.ReleaseLockResponse]:
        return self.swm.release_lock(resource_id, agent_id, lock_id, rpc_timeout=rpc_timeout)

    def get_distributed_lock_info(self, resource_id: str, rpc_timeout: Optional[float] = None) -> Optional[common_swm_pb2.LockInfo]:
        return self.swm.get_lock_info(resource_id, rpc_timeout=rpc_timeout)

    @contextlib.contextmanager
    def distributed_lock(self, resource_id: str, agent_id: str, acquire_timeout_ms: int = 10000,
                         lease_duration_ms: int = 60000, rpc_timeout: Optional[float] = None):
        actual_rpc_timeout = rpc_timeout if rpc_timeout is not None else self.config.swm_lock_rpc_timeout_s
        acquired_lock_response: Optional[common_swm_pb2.AcquireLockResponse] = None
        acquired_lock_id: Optional[str] = None
        lock_was_successfully_acquired_by_this_context = False
        try:
            logger.info(f"AgentSDK (distributed_lock): Attempting to acquire lock for '{resource_id}' by agent '{agent_id}'...")
            acquired_lock_response = self.acquire_distributed_lock(resource_id, agent_id, acquire_timeout_ms, lease_duration_ms, rpc_timeout=actual_rpc_timeout)
            if acquired_lock_response and \
               (acquired_lock_response.status == common_swm_pb2.LockStatusValue.ACQUIRED or \
                acquired_lock_response.status == common_swm_pb2.LockStatusValue.ALREADY_HELD_BY_YOU):
                status_name = common_swm_pb2.LockStatusValue.Name(acquired_lock_response.status)
                logger.info(f"AgentSDK (distributed_lock): Lock for '{resource_id}' status: {status_name}. Lock ID: {acquired_lock_response.lock_id}")
                acquired_lock_id = acquired_lock_response.lock_id
                lock_was_successfully_acquired_by_this_context = True
                yield acquired_lock_response
            else:
                status_name = common_swm_pb2.LockStatusValue.Name(acquired_lock_response.status) if acquired_lock_response else "N/A"
                message = acquired_lock_response.message if acquired_lock_response else "No response from server"
                logger.warning(f"AgentSDK (distributed_lock): Failed to acquire lock for '{resource_id}'. Status: {status_name}, Message: {message}")
                yield acquired_lock_response
        finally:
            if lock_was_successfully_acquired_by_this_context and acquired_lock_id:
                logger.info(f"AgentSDK (distributed_lock): Releasing lock for '{resource_id}', Lock ID: {acquired_lock_id} by agent '{agent_id}'.")
                self.release_distributed_lock(resource_id, agent_id, lock_id=acquired_lock_id, rpc_timeout=actual_rpc_timeout)
            elif acquired_lock_response:
                 logger.debug(f"AgentSDK (distributed_lock): Lock for '{resource_id}' was not in an acquired state by this context (status: {common_swm_pb2.LockStatusValue.Name(acquired_lock_response.status)}), no release needed by this context.")
            else:
                 logger.debug(f"AgentSDK (distributed_lock): No lock response received for '{resource_id}', no release needed.")

    def increment_distributed_counter(self, counter_id: str, increment_by: int = 1, rpc_timeout: Optional[float] = None) -> Optional[int]:
        response = self.swm.increment_counter(counter_id, increment_by, rpc_timeout=rpc_timeout)
        if response and (not response.status_message or "successfully updated" in response.status_message.lower() or not "error" in response.status_message.lower()):
            return response.current_value
        else: logger.error(f"AgentSDK: Error incrementing counter '{counter_id}'. Message: {response.status_message if response else 'No response'}"); return None

    def get_distributed_counter(self, counter_id: str, rpc_timeout: Optional[float] = None) -> Optional[int]:
        response = self.swm.get_counter(counter_id, rpc_timeout=rpc_timeout)
        if response and (not response.status_message or
                         "successfully retrieved" in response.status_message.lower() or
                         "not found, returned default value 0" in response.status_message.lower() or
                         not "error" in response.status_message.lower()):
            return response.current_value
        else: logger.error(f"AgentSDK: Error getting counter '{counter_id}'. Message: {response.status_message if response else 'No response'}"); return None

if __name__ == '__main__':
    logger.info("Running AgentSDK example (requires GLM, SWM, and potentially KPS servers)")
    import os
    import time

    if os.getenv("RUN_AGENT_SDK_EXAMPLE") == "true":
        try:
            sdk_config = DCSMClientSDKConfig()
            logger.info(f"SDK Config loaded: GLM @ {sdk_config.glm_address}, SWM @ {sdk_config.swm_address}, KPS @ {sdk_config.kps_address}")
            logger.info(f"SDK GLM retrieve KEMs timeout from config: {sdk_config.glm_retrieve_kems_timeout_s}s")
            logger.info(f"SDK SWM lock RPC timeout from config: {sdk_config.swm_lock_rpc_timeout_s}s")
            logger.info(f"SDK SWM Event Handler Enabled: {sdk_config.swm_event_handler_enabled}")


            with AgentSDK(config=sdk_config) as sdk:
                # Example of starting a resilient SWM event handler
                def my_event_callback(event: common_swm_pb2.SWMMemoryEvent):
                    logger.info(f"[Callback] Received SWM Event: Type={common_swm_pb2.SWMMemoryEvent.EventType.Name(event.event_type)}, KEM_ID={event.kem_payload.id if event.HasField('kem_payload') else 'N/A'}")

                event_handler_thread = sdk.handle_swm_events(
                    agent_id="my_resilient_agent_123",
                    event_callback=my_event_callback,
                    auto_update_lpa=True,
                    run_in_background=True
                )
                if event_handler_thread:
                    logger.info(f"SWM event handler started in background thread: {event_handler_thread.name}")

                logger.info("\n--- Testing SWM functions (via sdk.swm) ---")
                kems_to_pub_swm = [
                    {"id": "sdk_swm_pub_001", "content_type": "text/plain", "content": "SWM pub 1", "metadata": {"tag": "swm_batch"}},
                    {"id": "sdk_swm_pub_002", "content_type": "text/plain", "content": "SWM pub 2", "metadata": {"tag": "swm_batch", "color": "blue"}}
                ]
                s_pubs, f_pubs = sdk.publish_kems_to_swm_batch(kems_to_pub_swm, persist_to_glm=False)
                logger.info(f"SWM Batch Publish: Success {len(s_pubs)}, Failed {len(f_pubs)}")

                time.sleep(0.5) # Give time for events to potentially propagate

                # Publish one more to see events if SWM is up
                sdk.swm.publish_kem_to_swm({"id": "sdk_swm_pub_003", "content": "SWM pub 3", "metadata": {"color":"red"}})
                time.sleep(0.5)


                loaded_kems = sdk.load_kems_to_lpa_from_swm({"metadata_filters": {"tag": "swm_batch"}}, max_kems_to_load=5, rpc_timeout=15.0)
                logger.info(f"Loaded from SWM to LAM: {len(loaded_kems)} KEMs.")
                for lk in loaded_kems: logger.info(f"  LAM: {lk.get('id')} - {lk.get('content')}")

                counter_id = "my_test_counter_sdk"
                logger.info(f"Initial value of counter '{counter_id}': {sdk.get_distributed_counter(counter_id)}")
                sdk.increment_distributed_counter(counter_id, 5, rpc_timeout=3.0)
                logger.info(f"Value of counter '{counter_id}' after +5: {sdk.get_distributed_counter(counter_id)}")
                sdk.increment_distributed_counter(counter_id, -2)
                logger.info(f"Value of counter '{counter_id}' after -2: {sdk.get_distributed_counter(counter_id)}")

                res_id = "my_shared_resource_sdk_example"
                agent1_id = "sdk_example_agent_1"
                logger.info(f"Attempting lock on '{res_id}' by agent '{agent1_id}'...")
                with sdk.distributed_lock(res_id, agent1_id, acquire_timeout_ms=1000, lease_duration_ms=10000) as lock_resp:
                    if lock_resp and (lock_resp.status == common_swm_pb2.LockStatusValue.ACQUIRED or lock_resp.status == common_swm_pb2.LockStatusValue.ALREADY_HELD_BY_YOU) :
                        logger.info(f"Lock '{res_id}' acquired/already_held by '{agent1_id}'. Lock ID: {lock_resp.lock_id}")
                        time.sleep(1)
                        logger.info(f"Work on resource '{res_id}' finished.")
                    else:
                        status_name = common_swm_pb2.LockStatusValue.Name(lock_resp.status) if lock_resp else "N/A"
                        logger.warning(f"Failed to acquire lock '{res_id}'. Status: {status_name}")
                logger.info(f"Lock '{res_id}' should be released.")

                logger.info("\n--- Testing KPS function (via sdk.kps) ---")
                kps_data_id = "sdk_kps_test_001"
                kps_content = "This is a test document to be processed by KPS via SDK."
                kps_metadata = {"source": "sdk_example", "type": "test_doc"}
                kps_response = sdk.process_data_for_kps(kps_data_id, "text/plain", kps_content.encode('utf-8'), kps_metadata)
                if kps_response:
                    logger.info(f"KPS ProcessRawData response: KEM_ID='{kps_response.kem_id}', Success={kps_response.success}, Msg='{kps_response.status_message}'")

                # Wait a bit or simulate SWM server down to test resilience
                logger.info("Example run: Sleeping for 5s to observe event handler (if SWM is active)...")
                time.sleep(5)

                if event_handler_thread:
                    logger.info("Example run: Stopping SWM event handler...")
                    sdk.stop_swm_event_handler("my_resilient_agent_123")
                    logger.info("Example run: SWM event handler stop requested.")


        except grpc.RpcError as e:
            logger.error(f"gRPC ERROR in SDK example: {e.code()} - {e.details()}", exc_info=True)
        except Exception as e_main:
            logger.error(f"Unexpected error in AgentSDK example: {e_main}", exc_info=True)
        finally:
            logger.info("AgentSDK example finished.")
    else:
        logger.info("Environment variable RUN_AGENT_SDK_EXAMPLE not set to 'true', main SDK example will not run.")
