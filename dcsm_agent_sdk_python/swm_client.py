import grpc
import logging
import random
import time # For sleep in retry decorator, though decorator itself is now in grpc_utils
from typing import Optional, List, Tuple, Generator # Added Generator
import typing # Ensure typing is imported for older Python versions if needed by dataclasses

from .generated_grpc_code import kem_pb2
from .generated_grpc_code import glm_service_pb2 # For KEMQuery type hint
from .generated_grpc_code import swm_service_pb2
from .generated_grpc_code import swm_service_pb2_grpc
# from google.protobuf.json_format import MessageToDict, ParseDict # Replaced by utils
from .proto_utils import kem_dict_to_proto, kem_proto_to_dict

from dcs_memory.common.grpc_utils import retry_grpc_call, DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_DELAY_S, DEFAULT_BACKOFF_FACTOR, DEFAULT_JITTER_FRACTION # RETRYABLE_ERROR_CODES is also there

logger = logging.getLogger(__name__)

class SWMClient:
    def __init__(self,
                 server_address: str = 'localhost:50053',
                 retry_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 retry_initial_delay_s: float = DEFAULT_INITIAL_DELAY_S,
                 retry_backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
                 retry_jitter_fraction: float = DEFAULT_JITTER_FRACTION):
        self.server_address = server_address
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[swm_service_pb2_grpc.SharedWorkingMemoryServiceStub] = None
        self.retry_max_attempts = retry_max_attempts
        self.retry_initial_delay_s = retry_initial_delay_s
        self.retry_backoff_factor = retry_backoff_factor
        self.retry_jitter_fraction = retry_jitter_fraction

    def connect(self):
        if not self.channel:
            try:
                self.channel = grpc.insecure_channel(self.server_address)
                # Optional: Check connection readiness with a timeout
                # grpc.channel_ready_future(self.channel).result(timeout=5) # 5 seconds to connect
                self.stub = swm_service_pb2_grpc.SharedWorkingMemoryServiceStub(self.channel)
                logger.info(f"SWMClient: Successfully connected to SWM service at: {self.server_address}")
            except grpc.FutureTimeoutError: # type: ignore # grpc.FutureTimeoutError is valid
                logger.error(f"SWMClient: Failed to connect to SWM service at {self.server_address} within the timeout.")
                self.channel = None
                self.stub = None
            except Exception as e:
                logger.error(f"SWMClient: Error connecting to SWM service {self.server_address}: {e}", exc_info=True)
                self.channel = None
                self.stub = None

    def _ensure_connected(self):
        if not self.stub:
            self.connect()
        if not self.stub: # If still no stub after connect(), an error occurred
            raise ConnectionError(f"SWMClient: Failed to establish connection with SWM service at {self.server_address}")

    # _kem_dict_to_proto and _kem_proto_to_dict are now imported from proto_utils

    @retry_grpc_call
    def publish_kem_to_swm(self, kem_data: dict, persist_to_glm_if_new_or_updated: bool = False, timeout: int = 10) -> Optional[dict]:
        self._ensure_connected()
        kem_proto = kem_dict_to_proto(kem_data)
        request = swm_service_pb2.PublishKEMToSWMRequest(
            kem_to_publish=kem_proto,
            persist_to_glm_if_new_or_updated=persist_to_glm_if_new_or_updated
        )
        logger.debug(f"SWMClient: PublishKEMToSWM request: {request}")
        response: swm_service_pb2.PublishKEMToSWMResponse = self.stub.PublishKEMToSWM(request, timeout=timeout) # type: ignore

        if response and response.published_to_swm:
            logger.info(f"SWMClient: KEM ID '{response.kem_id}' published to SWM. Status: {response.status_message}")
            return {
                "kem_id": response.kem_id,
                "published_to_swm": response.published_to_swm,
                "persistence_triggered_to_glm": response.persistence_triggered_to_glm,
                "status_message": response.status_message
            }
        elif response:
            logger.warning(f"SWMClient: Failed to publish KEM to SWM. kem_id='{response.kem_id}', msg='{response.status_message}'")
            return { # Return failure status
                "kem_id": response.kem_id,
                "published_to_swm": False,
                "persistence_triggered_to_glm": response.persistence_triggered_to_glm,
                "status_message": response.status_message
            }
        return None # Should not happen if server behaves, implies RPC error handled by decorator

    @retry_grpc_call
    def query_swm(self, kem_query: glm_service_pb2.KEMQuery,
                  page_size: int = 0, page_token: str = "", timeout: int = 10) -> Tuple[Optional[List[dict]], Optional[str]]:
        self._ensure_connected()
        request = swm_service_pb2.QuerySWMRequest(
            query=kem_query,
            page_size=page_size, # Server uses its default if 0
            page_token=page_token
        )
        logger.debug(f"SWMClient: QuerySWM request: {request}")
        response: swm_service_pb2.QuerySWMResponse = self.stub.QuerySWM(request, timeout=timeout) # type: ignore

        if response:
            kems_as_dicts = [kem_proto_to_dict(kem) for kem in response.kems]
            logger.info(f"SWMClient: QuerySWM returned {len(kems_as_dicts)} KEMs. Next page token: '{response.next_page_token}'.")
            return kems_as_dicts, response.next_page_token
        return None, None # Should be handled by retry decorator for RpcError

    @retry_grpc_call
    def load_kems_from_glm(self, query_for_glm: glm_service_pb2.KEMQuery, timeout: int = 20) -> Optional[dict]:
        self._ensure_connected()
        request = swm_service_pb2.LoadKEMsFromGLMRequest(query_for_glm=query_for_glm)
        logger.debug(f"SWMClient: LoadKEMsFromGLM request: {request}")
        response: swm_service_pb2.LoadKEMsFromGLMResponse = self.stub.LoadKEMsFromGLM(request, timeout=timeout) # type: ignore

        if response:
            logger.info(f"SWMClient: LoadKEMsFromGLM response: Queried GLM: {response.kems_queried_in_glm_count}, Loaded to SWM: {response.kems_loaded_to_swm_count}, IDs: {list(response.loaded_kem_ids)}. Msg: {response.status_message}")
            return {
                "kems_queried_in_glm_count": response.kems_queried_in_glm_count,
                "kems_loaded_to_swm_count": response.kems_loaded_to_swm_count,
                "loaded_kem_ids": list(response.loaded_kem_ids),
                "status_message": response.status_message
            }
        return None

    def subscribe_to_swm_events(self, topics: Optional[List[swm_service_pb2.SubscriptionTopic]] = None,
                                agent_id: str = "default_sdk_agent",
                                requested_queue_size: int = 0) -> Optional[Generator[swm_service_pb2.SWMMemoryEvent, None, None]]:
        self._ensure_connected()
        request = swm_service_pb2.SubscribeToSWMEventsRequest(
            agent_id=agent_id,
            requested_queue_size=requested_queue_size
        )
        if topics:
            request.topics.extend(topics)
        logger.info(f"SWMClient: Subscribing to SWM events with request: {request}")
        try:
            event_stream = self.stub.SubscribeToSWMEvents(request) # type: ignore
            def event_generator():
                try:
                    for event in event_stream:
                        logger.debug(f"SWMClient: Received SWM event: {swm_service_pb2.SWMMemoryEvent.EventType.Name(event.event_type)} for KEM ID {event.kem_payload.id if event.HasField('kem_payload') else 'N/A'}")
                        yield event
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.CANCELLED:
                        logger.info(f"SWMClient: SWM event stream was cancelled by the client (agent_id: {agent_id}).")
                    elif e.code() == grpc.StatusCode.UNAVAILABLE:
                         logger.warning(f"SWMClient: SWM event stream unavailable (server might have stopped). Agent_id: {agent_id}. Code: {e.code()}, Details: {e.details()}")
                    else:
                        logger.error(f"SWMClient: gRPC error in SWM event stream (agent_id: {agent_id}). Code: {e.code()}, Details: {e.details()}", exc_info=True)
                except Exception as e_gen:
                    logger.error(f"SWMClient: Unexpected error in SWM event generator (agent_id: {agent_id}): {e_gen}", exc_info=True)
                finally:
                    logger.info(f"SWMClient: SWM event generator for agent_id '{agent_id}' finished.")
            return event_generator()
        except grpc.RpcError as e:
            logger.error(f"SWMClient: Failed to initiate SWM event subscription. Code: {e.code()}, Details: {e.details()}", exc_info=True)
            return None
        except Exception as e_init:
            logger.error(f"SWMClient: Unexpected error initiating SWM event subscription: {e_init}", exc_info=True)
            return None

    # --- Lock methods ---
    @retry_grpc_call
    def acquire_lock(self, resource_id: str, agent_id: str, timeout_ms: int = 0, lease_duration_ms: int = 0, rpc_timeout: int = 5) -> Optional[swm_service_pb2.AcquireLockResponse]:
        self._ensure_connected()
        request = swm_service_pb2.AcquireLockRequest(resource_id=resource_id, agent_id=agent_id, timeout_ms=timeout_ms, lease_duration_ms=lease_duration_ms)
        logger.debug(f"SWMClient: AcquireLock request: {request}")
        try:
            response = self.stub.AcquireLock(request, timeout=rpc_timeout) # type: ignore
            logger.info(f"SWMClient: AcquireLock for resource '{resource_id}' by agent '{agent_id}' status: {swm_service_pb2.LockStatusValue.Name(response.status)}. Lock ID: {response.lock_id}")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on AcquireLock for resource '{resource_id}': {e.code()} - {e.details()}", exc_info=False)
            return swm_service_pb2.AcquireLockResponse(resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ERROR, message=f"gRPC Error: {e.details()}")
        except Exception as e_acq:
            logger.error(f"SWMClient: Unexpected error on AcquireLock for resource '{resource_id}': {e_acq}", exc_info=True)
            return swm_service_pb2.AcquireLockResponse(resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ERROR, message=f"Unexpected SDK error: {e_acq}")

    @retry_grpc_call
    def release_lock(self, resource_id: str, agent_id: str, lock_id: Optional[str] = None, rpc_timeout: int = 5) -> Optional[swm_service_pb2.ReleaseLockResponse]:
        self._ensure_connected()
        request = swm_service_pb2.ReleaseLockRequest(resource_id=resource_id, agent_id=agent_id, lock_id=lock_id if lock_id else "")
        logger.debug(f"SWMClient: ReleaseLock request: {request}")
        try:
            response = self.stub.ReleaseLock(request, timeout=rpc_timeout) # type: ignore
            logger.info(f"SWMClient: ReleaseLock for resource '{resource_id}' by agent '{agent_id}' status: {swm_service_pb2.ReleaseStatusValue.Name(response.status)}.")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on ReleaseLock for resource '{resource_id}': {e.code()} - {e.details()}", exc_info=False)
            return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING, message=f"gRPC Error: {e.details()}")
        except Exception as e_rel:
            logger.error(f"SWMClient: Unexpected error on ReleaseLock for resource '{resource_id}': {e_rel}", exc_info=True)
            return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING, message=f"Unexpected SDK error: {e_rel}")

    @retry_grpc_call
    def get_lock_info(self, resource_id: str, rpc_timeout: int = 5) -> Optional[swm_service_pb2.LockInfo]:
        self._ensure_connected()
        request = swm_service_pb2.GetLockInfoRequest(resource_id=resource_id)
        logger.debug(f"SWMClient: GetLockInfo request for resource '{resource_id}'")
        try:
            response = self.stub.GetLockInfo(request, timeout=rpc_timeout) # type: ignore
            logger.info(f"SWMClient: GetLockInfo for resource '{resource_id}': is_locked={response.is_locked}, holder='{response.current_holder_agent_id}'.")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on GetLockInfo for resource '{resource_id}': {e.code()} - {e.details()}", exc_info=False)
            return None
        except Exception as e_info:
            logger.error(f"SWMClient: Unexpected error on GetLockInfo for resource '{resource_id}': {e_info}", exc_info=True)
            return None

    # --- Distributed Counter methods ---
    @retry_grpc_call
    def increment_counter(self, counter_id: str, increment_by: int = 1, rpc_timeout: int = 5) -> Optional[swm_service_pb2.CounterValueResponse]:
        self._ensure_connected()
        request = swm_service_pb2.IncrementCounterRequest(counter_id=counter_id, increment_by=increment_by)
        logger.debug(f"SWMClient: IncrementCounter request: {request}")
        try:
            response = self.stub.IncrementCounter(request, timeout=rpc_timeout) # type: ignore
            logger.info(f"SWMClient: IncrementCounter for counter_id '{counter_id}' new value: {response.current_value}")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on IncrementCounter for counter_id '{counter_id}': {e.code()} - {e.details()}", exc_info=False)
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"gRPC Error: {e.details()}")
        except Exception as e_inc:
            logger.error(f"SWMClient: Unexpected error on IncrementCounter for counter_id '{counter_id}': {e_inc}", exc_info=True)
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"Unexpected SDK error: {e_inc}")

    @retry_grpc_call
    def get_counter(self, counter_id: str, rpc_timeout: int = 5) -> Optional[swm_service_pb2.CounterValueResponse]:
        self._ensure_connected()
        request = swm_service_pb2.DistributedCounterRequest(counter_id=counter_id)
        logger.debug(f"SWMClient: GetCounter request for counter_id '{counter_id}'")
        try:
            response = self.stub.GetCounter(request, timeout=rpc_timeout) # type: ignore
            logger.info(f"SWMClient: GetCounter for counter_id '{counter_id}' value: {response.current_value}")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on GetCounter for counter_id '{counter_id}': {e.code()} - {e.details()}", exc_info=False)
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"gRPC Error: {e.details()}")
        except Exception as e_getc:
            logger.error(f"SWMClient: Unexpected error on GetCounter for counter_id '{counter_id}': {e_getc}", exc_info=True)
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"Unexpected SDK error: {e_getc}")

    def close(self):
        if self.channel:
            self.channel.close()
            self.channel = None
            self.stub = None
            logger.info("SWMClient: SWMClient channel closed.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == '__main__':
    logger.info("SWMClient Example (requires SWM server on localhost:50053)")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    example_kem_data = {
        "id": "swm_client_test_001", "content_type": "text/plain",
        "content": "This is a test KEM for the SWM client.",
        "metadata": {"source": "swm_client_example", "version": "1.0"},
        "embeddings": [i*0.01 for i in range(384)]
    }
    try:
        with SWMClient(server_address='localhost:50053') as client:
            logger.info("\n--- Publishing KEM to SWM ---")
            publish_result = client.publish_kem_to_swm(example_kem_data, persist_to_glm_if_new_or_updated=True)
            if publish_result and publish_result.get("published_to_swm"):
                logger.info(f"KEM published: ID='{publish_result.get('kem_id')}', GLM Status: {publish_result.get('persistence_triggered_to_glm')}")
                published_kem_id = publish_result.get('kem_id')
                if published_kem_id:
                    logger.info(f"\n--- Querying KEM ID '{published_kem_id}' from SWM ---")
                    query_by_id = glm_service_pb2.KEMQuery()
                    query_by_id.ids.extend([published_kem_id])
                    kems_tuple_id, _ = client.query_swm(kem_query=query_by_id, page_size=1)
                    if kems_tuple_id and kems_tuple_id[0]: logger.info(f"Found KEM: {kems_tuple_id[0]}")
                    else: logger.warning(f"KEM ID '{published_kem_id}' not found in SWM after publishing.")
            else: logger.error(f"Failed to publish KEM. Result: {publish_result}")
            logger.info("\n--- Querying KEMs from SWM by metadata {'source': 'swm_client_example'} ---")
            query_by_meta = glm_service_pb2.KEMQuery()
            query_by_meta.metadata_filters["source"] = "swm_client_example"
            kems_tuple_meta, _ = client.query_swm(kem_query=query_by_meta, page_size=5)
            if kems_tuple_meta:
                logger.info(f"Found {len(kems_tuple_meta)} KEMs by metadata:")
                for k_dict in kems_tuple_meta: logger.info(f"  ID: {k_dict.get('id')}, Content: {k_dict.get('content', '')[:30]}...")
            else: logger.info("No KEMs found by metadata.")
            logger.info("\n--- Requesting to load KEMs from GLM to SWM (by metadata {'topic': 'AI'}) ---")
            load_query = glm_service_pb2.KEMQuery()
            load_query.metadata_filters["topic"] = "AI"
            load_result = client.load_kems_from_glm(query_for_glm=load_query)
            if load_result: logger.info(f"Result of loading from GLM: {load_result}")
            else: logger.error("Failed to execute request to load from GLM.")
    except ConnectionError as e: logger.error(f"CONNECTION ERROR: {e}")
    except grpc.RpcError as e: logger.error(f"gRPC ERROR: code={e.code()}, details={e.details()}")
    except Exception as e: logger.error(f"UNEXPECTED ERROR: {e}", exc_info=True)
