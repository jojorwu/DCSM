import grpc
import logging
from typing import Optional, List, Tuple

from .generated_grpc_code import kem_pb2 # KEM is used in some KPS messages, but KPS mostly deals with its own specific types
from .generated_grpc_code import kps_service_pb2
from .generated_grpc_code import kps_service_pb2_grpc
from .proto_utils import kem_dict_to_proto, kem_proto_to_dict # May not be directly needed if KPS deals with content strings + KEM URIs
from dcs_memory.common.grpc_utils import retry_grpc_call, DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_DELAY_S, DEFAULT_BACKOFF_FACTOR, DEFAULT_JITTER_FRACTION

logger = logging.getLogger(__name__)

class KPSClient:
    def __init__(self,
                 server_address: str = 'localhost:50053', # Default KPS port
                 retry_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 retry_initial_delay_s: float = DEFAULT_INITIAL_DELAY_S,
                 retry_backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
                 retry_jitter_fraction: float = DEFAULT_JITTER_FRACTION,
                 tls_enabled: bool = False,
                 tls_ca_cert_path: Optional[str] = None,
                 tls_client_cert_path: Optional[str] = None,
                 tls_client_key_path: Optional[str] = None,
                 tls_server_override_authority: Optional[str] = None):
        self.server_address = server_address
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[kps_service_pb2_grpc.KnowledgeProcessingServiceStub] = None

        self.retry_max_attempts = retry_max_attempts
        self.retry_initial_delay_s = retry_initial_delay_s
        self.retry_backoff_factor = retry_backoff_factor
        self.retry_jitter_fraction = retry_jitter_fraction

        self.tls_enabled = tls_enabled
        self.tls_ca_cert_path = tls_ca_cert_path
        self.tls_client_cert_path = tls_client_cert_path
        self.tls_client_key_path = tls_client_key_path
        self.tls_server_override_authority = tls_server_override_authority

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
                        client_cert_chain = (client_key, client_cert) # private_key, certificate_chain

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

                # Optional: Check connection readiness with a timeout
                # grpc.channel_ready_future(self.channel).result(timeout=5)
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
    def add_memory(self, kem_uri: str, content: str, timeout: int = 10) -> Optional[kps_service_pb2.AddMemoryResponse]:
        self._ensure_connected()
        request = kps_service_pb2.AddMemoryRequest(kem_uri=kem_uri, content=content)
        logger.debug(f"KPSClient: AddMemory request: kem_uri='{kem_uri}', content_len={len(content)}")
        response = self.stub.AddMemory(request, timeout=timeout) # type: ignore
        logger.info(f"KPSClient: AddMemory response for kem_uri='{kem_uri}': id='{response.id}', status='{response.status_message}'")
        return response

    @retry_grpc_call
    def batch_add_memories(self, memories: List[kps_service_pb2.MemoryContent], timeout: int = 30) -> Optional[kps_service_pb2.BatchAddMemoriesResponse]:
        self._ensure_connected()
        request = kps_service_pb2.BatchAddMemoriesRequest(memories=memories)
        logger.debug(f"KPSClient: BatchAddMemories request with {len(memories)} items.")
        response = self.stub.BatchAddMemories(request, timeout=timeout) # type: ignore
        logger.info(f"KPSClient: BatchAddMemories response: Success={len(response.successfully_added_ids)}, Failed={len(response.failed_kem_references)}, Msg='{response.overall_error_message}'")
        return response

    @retry_grpc_call
    def retrieve_memory_content(self, kem_uri: str, timeout: int = 10) -> Optional[kps_service_pb2.MemoryContent]: # KPS RetrieveMemory returns MemoryContent
        self._ensure_connected()
        request = kps_service_pb2.RetrieveMemoryRequest(kem_uri=kem_uri)
        logger.debug(f"KPSClient: RetrieveMemory request for kem_uri='{kem_uri}'")
        response = self.stub.RetrieveMemory(request, timeout=timeout) # type: ignore
        if response and response.kem_uri == kem_uri : # Basic check for validity
             logger.info(f"KPSClient: RetrieveMemory found content for kem_uri='{kem_uri}'.")
             return response
        logger.warning(f"KPSClient: RetrieveMemory did not find content or unexpected response for kem_uri='{kem_uri}'.")
        return None


    @retry_grpc_call
    def query_nearest_neighbors(self, kem_uri: str, k: int, timeout: int = 10) -> Optional[kps_service_pb2.QueryResults]:
        self._ensure_connected()
        request = kps_service_pb2.QueryNearestNeighborsRequest(kem_uri=kem_uri, k=k)
        logger.debug(f"KPSClient: QueryNearestNeighbors request: kem_uri='{kem_uri}', k={k}")
        response = self.stub.QueryNearestNeighbors(request, timeout=timeout) # type: ignore
        logger.info(f"KPSClient: QueryNearestNeighbors for kem_uri='{kem_uri}' found {len(response.results)} neighbors.")
        return response

    @retry_grpc_call
    def query_nearest_neighbors_by_vector(self, vector: List[float], k: int, timeout: int = 10) -> Optional[kps_service_pb2.QueryResults]:
        self._ensure_connected()
        request = kps_service_pb2.QueryNearestNeighborsByVectorRequest(vector=vector, k=k)
        logger.debug(f"KPSClient: QueryNearestNeighborsByVector request: vector_len={len(vector)}, k={k}")
        response = self.stub.QueryNearestNeighborsByVector(request, timeout=timeout) # type: ignore
        logger.info(f"KPSClient: QueryNearestNeighborsByVector found {len(response.results)} neighbors.")
        return response

    @retry_grpc_call
    def remove_memory(self, kem_uri: str, timeout: int = 10) -> Optional[kps_service_pb2.RemoveMemoryResponse]:
        self._ensure_connected()
        request = kps_service_pb2.RemoveMemoryRequest(kem_uri=kem_uri)
        logger.debug(f"KPSClient: RemoveMemory request for kem_uri='{kem_uri}'")
        response = self.stub.RemoveMemory(request, timeout=timeout) # type: ignore
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
                kem_uri_test = "kem:example:doc:kps_client_test_001"
                content_test = "This is a test document for KPSClient."

                logger.info(f"\n--- Adding memory: {kem_uri_test} ---")
                add_resp = client.add_memory(kem_uri_test, content_test)
                if add_resp: logger.info(f"AddMemory Status: {add_resp.status_message}")

                logger.info(f"\n--- Retrieving memory content: {kem_uri_test} ---")
                ret_mem = client.retrieve_memory_content(kem_uri_test)
                if ret_mem: logger.info(f"Retrieved content: '{ret_mem.content[:50]}...' for KEM URI: {ret_mem.kem_uri}")

                logger.info(f"\n--- Querying nearest neighbors for: {kem_uri_test} (k=1) ---")
                nn_resp = client.query_nearest_neighbors(kem_uri_test, k=1)
                if nn_resp and nn_resp.results:
                    logger.info(f"Found {len(nn_resp.results)} neighbor(s). Top result: ID='{nn_resp.results[0].kem_uri}', Score={nn_resp.results[0].distance:.4f}")

                logger.info(f"\n--- Removing memory: {kem_uri_test} ---")
                remove_resp = client.remove_memory(kem_uri_test)
                if remove_resp: logger.info(f"RemoveMemory Status: {remove_resp.status_message}")

        except ConnectionError as e: logger.error(f"KPSClient Example: CONNECTION ERROR - {e}")
        except grpc.RpcError as e: logger.error(f"KPSClient Example: gRPC ERROR - code={e.code()}, details={e.details()}")
        except Exception as e_main: logger.error(f"KPSClient Example: UNEXPECTED ERROR - {e_main}", exc_info=True)
    else:
        logger.info("Environment variable RUN_KPS_CLIENT_EXAMPLE not set to 'true', example will not run.")

```
