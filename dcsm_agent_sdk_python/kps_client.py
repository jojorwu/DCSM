import grpc
import logging
from typing import Optional, List, Tuple, Dict

from dcs_memory.generated_grpc import kps_service_pb2
from dcs_memory.generated_grpc import kps_service_pb2_grpc
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
        self.stub: Optional[kps_service_pb2_grpc.KnowledgeProcessorServiceStub] = None

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

                self.stub = kps_service_pb2_grpc.KnowledgeProcessorServiceStub(self.channel)
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
    def process_raw_data(self, data_id: str, content_type: str, raw_content: bytes, initial_metadata: Dict[str, str], timeout: int = 30) -> Optional[kps_service_pb2.ProcessRawDataResponse]:
        self._ensure_connected()
        request = kps_service_pb2.ProcessRawDataRequest(
            data_id=data_id,
            content_type=content_type,
            raw_content=raw_content,
            initial_metadata=initial_metadata
        )
        logger.debug(f"KPSClient: ProcessRawData request: data_id='{data_id}', content_type='{content_type}'")
        response = self.stub.ProcessRawData(request, timeout=timeout)
        logger.info(f"KPSClient: ProcessRawData response for data_id='{data_id}': success='{response.success}', kem_id='{response.kem_id}', status='{response.status_message}'")
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
