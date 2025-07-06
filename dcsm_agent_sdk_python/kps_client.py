import grpc
import logging
from typing import Optional, List, Dict # Simplified imports

# KEM type might be needed if ProcessRawDataResponse includes it, or for constructing requests if KPS API changes.
# For now, assuming kps_service_pb2 is self-contained for ProcessRawData.
# from .generated_grpc_code import kem_pb2
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
                 process_raw_data_timeout_s: float = 30.0): # Only timeout for ProcessRawData
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

        self.process_raw_data_timeout_s = process_raw_data_timeout_s

    def connect(self):
        if not self.channel:
            try:
                if self.tls_enabled:
                    root_certificates = None
                    if self.tls_ca_cert_path:
                        with open(self.tls_ca_cert_path, 'rb') as f:
                            root_certificates = f.read()

                    client_key = None
                    client_cert = None
                    if self.tls_client_cert_path and self.tls_client_key_path:
                        with open(self.tls_client_key_path, 'rb') as f:
                            client_key = f.read()
                        with open(self.tls_client_cert_path, 'rb') as f:
                            client_cert = f.read()

                    channel_credentials = grpc.ssl_channel_credentials(
                        root_certificates=root_certificates,
                        private_key=client_key,
                        certificate_chain=client_cert
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

            except grpc.FutureTimeoutError: # This error is less common for channel creation itself
                logger.error(f"KPSClient: Failed to connect to KPS service at {self.server_address} within timeout (TLS: {self.tls_enabled}).")
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
                         initial_metadata: Optional[Dict[str, str]] = None, # Changed type hint for clarity
                         timeout: Optional[float] = None) -> Optional[kps_service_pb2.ProcessRawDataResponse]:
        self._ensure_connected()
        actual_timeout = timeout if timeout is not None else self.process_raw_data_timeout_s

        # Ensure initial_metadata values are strings if provided
        processed_metadata: Optional[Dict[str, str]] = None
        if initial_metadata:
            processed_metadata = {k: str(v) for k, v in initial_metadata.items()}

        request = kps_service_pb2.ProcessRawDataRequest(
            data_id=data_id,
            content_type=content_type,
            raw_content=raw_content,
            initial_metadata=processed_metadata
        )
        logger.debug(f"KPSClient: ProcessRawData request for data_id='{data_id}' with timeout {actual_timeout}s")
        response = self.stub.ProcessRawData(request, timeout=actual_timeout) # type: ignore
        logger.info(f"KPSClient: ProcessRawData response for data_id='{data_id}': kem_id='{response.kem_id}', success={response.success}, msg='{response.status_message}'")
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
            # Example: Use default timeout from KPSClient's __init__
            with KPSClient() as client:
                data_id_test = "kps_client_raw_data_001"
                content_type_test = "text/plain"
                raw_content_test = b"This is a test for KPS ProcessRawData via SDK's KPSClient."
                metadata_test = {"source": "kps_client_example", "version": "1.0"}

                logger.info(f"\n--- Processing raw data: {data_id_test} ---")
                # Example of overriding timeout for a specific call:
                # process_resp = client.process_raw_data(data_id_test, content_type_test, raw_content_test, metadata_test, timeout=45.0)
                process_resp = client.process_raw_data(data_id_test, content_type_test, raw_content_test, metadata_test)

                if process_resp:
                    logger.info(f"ProcessRawData Status: {process_resp.status_message}, KEM_ID: {process_resp.kem_id}, Success: {process_resp.success}")
                else:
                    logger.error("Failed to process data via KPS or received no response.")

        except ConnectionError as e: logger.error(f"KPSClient Example: CONNECTION ERROR - {e}")
        except grpc.RpcError as e: logger.error(f"KPSClient Example: gRPC ERROR - code={e.code()}, details={e.details()}")
        except Exception as e_main: logger.error(f"KPSClient Example: UNEXPECTED ERROR - {e_main}", exc_info=True)
    else:
        logger.info("Environment variable RUN_KPS_CLIENT_EXAMPLE not set to 'true', KPSClient example will not run.")
