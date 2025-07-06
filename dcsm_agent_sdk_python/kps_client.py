import grpc
import logging
from typing import Optional, List, Tuple

from .generated_grpc_code import kem_pb2
from .generated_grpc_code import kps_service_pb2
from .generated_grpc_code import kps_service_pb2_grpc
from dcs_memory.common.grpc_utils import retry_grpc_call, DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_DELAY_S, DEFAULT_BACKOFF_FACTOR, DEFAULT_JITTER_FRACTION

logger = logging.getLogger(__name__)

class KPSClient:
    def __init__(self,
                 server_address: str = 'localhost:50053',
                 retry_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 retry_initial_delay_s: float = DEFAULT_INITIAL_DELAY_S,
                 retry_backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
                 retry_jitter_fraction: float = DEFAULT_JITTER_FRACTION,
                 tls_enabled: bool = False,
                 tls_ca_cert_path: Optional[str] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key_path: Optional[str] = None,
                 tls_server_override_authority: Optional[str] = None,
                 # Default timeout for KPS RPCs
                 process_raw_data_timeout_s: float = 30.0,
                 # Adding default timeouts for other hypothetical KPS methods for completeness
                 add_memory_timeout_s: float = 10.0,
                 batch_add_memories_timeout_s: float = 30.0,
                 retrieve_memory_timeout_s: float = 10.0,
                 query_nn_timeout_s: float = 10.0,
                 remove_memory_timeout_s: float = 10.0):
        self.server_address = server_address
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[kps_service_pb2_grpc.KnowledgeProcessingServiceStub] = None

        # Retry policy
        self.retry_max_attempts = retry_max_attempts
        self.retry_initial_delay_s = retry_initial_delay_s
        self.retry_backoff_factor = retry_backoff_factor
        self.retry_jitter_fraction = retry_jitter_fraction

        # TLS settings
        self.tls_enabled = tls_enabled
        self.tls_ca_cert_path = tls_ca_cert_path
        self.tls_client_cert_path = tls_client_cert_path
        self.tls_client_key_path = tls_client_key_path
        self.tls_server_override_authority = tls_server_override_authority

        # RPC Timeouts
        self.process_raw_data_timeout_s = process_raw_data_timeout_s
        self.add_memory_timeout_s = add_memory_timeout_s
        self.batch_add_memories_timeout_s = batch_add_memories_timeout_s
        self.retrieve_memory_timeout_s = retrieve_memory_timeout_s
        self.query_nn_timeout_s = query_nn_timeout_s
        self.remove_memory_timeout_s = remove_memory_timeout_s


    def connect(self):
        if not self.channel:
            try:
                if self.tls_enabled:
                    credentials_list = []
                    if self.tls_ca_cert_path:
                        with open(self.tls_ca_cert_path, 'rb') as f:
                            root_certificates = f.read()
                        credentials_list.append(root_certificates)

                    client_cert_chain = None
                    if self.tls_client_cert_path and self.tls_client_key_path:
                        with open(self.tls_client_cert_path, 'rb') as f:
                            client_cert = f.read()
                        with open(self.tls_client_key_path, 'rb') as f:
                            client_key = f.read()
                        client_cert_chain = (client_key, client_cert)

                    channel_credentials = grpc.ssl_channel_credentials(
                        root_certificates=credentials_list[0] if credentials_list else None,
                        private_key=client_cert_chain[0] if client_cert_chain else None,
                        certificate_chain=client_cert_chain[1] if client_cert_chain else None
                    )

                    options = None
                    if self.tls_server_override_authority:
                        options = (('grpc.ssl_target_name_override', self.tls_server_override_authority),)

                    self.channel = grpc.secure_channel(self.server_address, channel_credentials, options=options)
                    logger.info(f"KPSClient: Secure channel created for {self.server_address}.")
                else:
                    self.channel = grpc.insecure_channel(self.server_address)
                    logger.info(f"KPSClient: Insecure channel created for {self.server_address}.")

                self.stub = kps_service_pb2_grpc.KnowledgeProcessingServiceStub(self.channel)
                logger.info(f"KPSClient: Successfully connected to KPS service at: {self.server_address} (TLS: {self.tls_enabled})")

            except grpc.FutureTimeoutError:
                logger.error(f"KPSClient: Failed to connect to KPS service at {self.server_address} within the timeout (TLS: {self.tls_enabled}).")
                self._reset_connection_state()
            except FileNotFoundError as e_fnf:
                logger.error(f"KPSClient: TLS certificate/key file not found: {e_fnf} (TLS: {self.tls_enabled}). Connection failed.")
                self._reset_connection_state()
            except Exception as e:
                logger.error(f"KPSClient: Error connecting to KPS service {self.server_address} (TLS: {self.tls_enabled}): {e}", exc_info=True)
                self._reset_connection_state()

    def _reset_connection_state(self):
        self.channel = None
        self.stub = None

    def _ensure_connected(self):
        if not self.stub:
            self.connect()
        if not self.stub:
            raise ConnectionError(f"KPSClient: Failed to establish connection with KPS service at {self.server_address}")

    @retry_grpc_call
    def process_raw_data(self, data_id: str, content_type: str, raw_content: bytes,
                         initial_metadata: Optional[dict] = None, timeout: Optional[float] = None) -> Optional[kps_service_pb2.ProcessRawDataResponse]:
        self._ensure_connected()
        actual_timeout = timeout if timeout is not None else self.process_raw_data_timeout_s

        metadata_proto = {}
        if initial_metadata:
            for k, v in initial_metadata.items():
                metadata_proto[k] = str(v)

        request = kps_service_pb2.ProcessRawDataRequest(
            data_id=data_id,
            content_type=content_type,
            raw_content=raw_content,
            initial_metadata=metadata_proto
        )
        logger.debug(f"KPSClient: ProcessRawData request for data_id='{data_id}' with timeout {actual_timeout}s")
        response = self.stub.ProcessRawData(request, timeout=actual_timeout) # type: ignore
        logger.info(f"KPSClient: ProcessRawData response for data_id='{data_id}': kem_id='{response.kem_id}', success={response.success}, msg='{response.status_message}'")
        return response

    @retry_grpc_call
    def add_memory(self, kem_uri: str, content: str, timeout: Optional[float] = None) -> Optional[kps_service_pb2.AddMemoryResponse]:
        self._ensure_connected()
        actual_timeout = timeout if timeout is not None else self.add_memory_timeout_s
        request = kps_service_pb2.AddMemoryRequest(kem_uri=kem_uri, content=content)
        logger.debug(f"KPSClient: AddMemory request: kem_uri='{kem_uri}', content_len={len(content)}, timeout={actual_timeout}s")
        response = self.stub.AddMemory(request, timeout=actual_timeout) # type: ignore
        logger.info(f"KPSClient: AddMemory response for kem_uri='{kem_uri}': id='{response.id}', status='{response.status_message}'")
        return response

    @retry_grpc_call
    def batch_add_memories(self, memories: List[kps_service_pb2.MemoryContent], timeout: Optional[float] = None) -> Optional[kps_service_pb2.BatchAddMemoriesResponse]:
        self._ensure_connected()
        actual_timeout = timeout if timeout is not None else self.batch_add_memories_timeout_s
        request = kps_service_pb2.BatchAddMemoriesRequest(memories=memories)
        logger.debug(f"KPSClient: BatchAddMemories request with {len(memories)} items, timeout={actual_timeout}s")
        response = self.stub.BatchAddMemories(request, timeout=actual_timeout) # type: ignore
        logger.info(f"KPSClient: BatchAddMemories response: Success={len(response.successfully_added_ids)}, Failed={len(response.failed_kem_references)}, Msg='{response.overall_error_message}'")
        return response

    @retry_grpc_call
    def retrieve_memory_content(self, kem_uri: str, timeout: Optional[float] = None) -> Optional[kps_service_pb2.MemoryContent]:
        self._ensure_connected()
        actual_timeout = timeout if timeout is not None else self.retrieve_memory_timeout_s
        request = kps_service_pb2.RetrieveMemoryRequest(kem_uri=kem_uri)
        logger.debug(f"KPSClient: RetrieveMemory request for kem_uri='{kem_uri}', timeout={actual_timeout}s")
        response = self.stub.RetrieveMemory(request, timeout=actual_timeout) # type: ignore
        if response and response.kem_uri == kem_uri :
             logger.info(f"KPSClient: RetrieveMemory found content for kem_uri='{kem_uri}'.")
             return response
        logger.warning(f"KPSClient: RetrieveMemory did not find content or unexpected response for kem_uri='{kem_uri}'.")
        return None

    @retry_grpc_call
    def query_nearest_neighbors(self, kem_uri: str, k: int, timeout: Optional[float] = None) -> Optional[kps_service_pb2.QueryResults]:
        self._ensure_connected()
        actual_timeout = timeout if timeout is not None else self.query_nn_timeout_s
        request = kps_service_pb2.QueryNearestNeighborsRequest(kem_uri=kem_uri, k=k)
        logger.debug(f"KPSClient: QueryNearestNeighbors request: kem_uri='{kem_uri}', k={k}, timeout={actual_timeout}s")
        response = self.stub.QueryNearestNeighbors(request, timeout=actual_timeout) # type: ignore
        logger.info(f"KPSClient: QueryNearestNeighbors for kem_uri='{kem_uri}' found {len(response.results)} neighbors.")
        return response

    @retry_grpc_call
    def query_nearest_neighbors_by_vector(self, vector: List[float], k: int, timeout: Optional[float] = None) -> Optional[kps_service_pb2.QueryResults]:
        self._ensure_connected()
        actual_timeout = timeout if timeout is not None else self.query_nn_timeout_s
        request = kps_service_pb2.QueryNearestNeighborsByVectorRequest(vector=vector, k=k)
        logger.debug(f"KPSClient: QueryNearestNeighborsByVector request: vector_len={len(vector)}, k={k}, timeout={actual_timeout}s")
        response = self.stub.QueryNearestNeighborsByVector(request, timeout=actual_timeout) # type: ignore
        logger.info(f"KPSClient: QueryNearestNeighborsByVector found {len(response.results)} neighbors.")
        return response

    @retry_grpc_call
    def remove_memory(self, kem_uri: str, timeout: Optional[float] = None) -> Optional[kps_service_pb2.RemoveMemoryResponse]:
        self._ensure_connected()
        actual_timeout = timeout if timeout is not None else self.remove_memory_timeout_s
        request = kps_service_pb2.RemoveMemoryRequest(kem_uri=kem_uri)
        logger.debug(f"KPSClient: RemoveMemory request for kem_uri='{kem_uri}', timeout={actual_timeout}s")
        response = self.stub.RemoveMemory(request, timeout=actual_timeout) # type: ignore
        logger.info(f"KPSClient: RemoveMemory response for kem_uri='{kem_uri}': status='{response.status_message}'")
        return response

    def close(self):
        if self.channel:
            self.channel.close()
            self._reset_connection_state()
            logger.info("KPSClient: KPSClient channel closed.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Running KPSClient example (requires KPS server on localhost:50053)")

    import os
    if os.getenv("RUN_KPS_CLIENT_EXAMPLE") == "true":
        try:
            with KPSClient() as client:

                data_id_test = "kps_client_raw_data_001"
                content_type_test = "text/plain"
                raw_content_test = b"This is a test for KPS ProcessRawData via SDK."
                metadata_test = {"source": "kps_client_example", "version": "1.0"}

                logger.info(f"\n--- Processing raw data: {data_id_test} ---")
                process_resp = client.process_raw_data(data_id_test, content_type_test, raw_content_test, metadata_test)

                if process_resp:
                    logger.info(f"ProcessRawData Status: {process_resp.status_message}, KEM_ID: {process_resp.kem_id}, Success: {process_resp.success}")

        except ConnectionError as e: logger.error(f"KPSClient Example: CONNECTION ERROR - {e}")
        except grpc.RpcError as e: logger.error(f"KPSClient Example: gRPC ERROR - code={e.code()}, details={e.details()}")
        except Exception as e_main: logger.error(f"KPSClient Example: UNEXPECTED ERROR - {e_main}", exc_info=True)
    else:
        logger.info("Environment variable RUN_KPS_CLIENT_EXAMPLE not set to 'true', example will not run.")
