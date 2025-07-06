from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional, List, Dict, Any

class DCSMClientSDKConfig(BaseSettings):
    """
    Configuration for the DCSM Agent SDK.
    Can be loaded from environment variables (prefixed with DCSM_SDK_) or a .env file.
    """
    glm_host: str = Field(default="localhost", alias="DCSM_GLM_HOST")
    glm_port: int = Field(default=50051, alias="DCSM_GLM_PORT")

    swm_host: str = Field(default="localhost", alias="DCSM_SWM_HOST")
    swm_port: int = Field(default=50052, alias="DCSM_SWM_PORT")

    kps_host: str = Field(default="localhost", alias="DCSM_KPS_HOST")
    kps_port: int = Field(default=50053, alias="DCSM_KPS_PORT")

    lpa_max_size: int = Field(default=100)
    lpa_indexed_keys: List[str] = Field(default_factory=list)

    connect_on_init: bool = Field(default=True)

    retry_max_attempts: int = Field(default=3)
    retry_initial_delay_s: float = Field(default=1.0)
    retry_backoff_factor: float = Field(default=2.0)
    retry_jitter_fraction: float = Field(default=0.1)

    tls_enabled: bool = Field(default=False, description="Enable TLS for all client connections.")
    tls_ca_cert_path: Optional[str] = Field(default=None, description="Path to the CA certificate file for verifying server certs.")
    tls_client_cert_path: Optional[str] = Field(default=None, description="Path to client's TLS certificate file.")
    tls_client_key_path: Optional[str] = Field(default=None, description="Path to client's TLS key file.")
    tls_server_override_authority: Optional[str] = Field(default=None, description="Server name override for SSL/TLS hostname verification.")

    # Default RPC Timeouts (in seconds)
    glm_batch_store_kems_timeout_s: float = Field(default=20.0, description="Default timeout for GLM BatchStoreKEMs RPC.")
    glm_retrieve_kems_timeout_s: float = Field(default=10.0, description="Default timeout for GLM RetrieveKEMs RPC.")
    glm_update_kem_timeout_s: float = Field(default=10.0, description="Default timeout for GLM UpdateKEM RPC.")
    glm_delete_kem_timeout_s: float = Field(default=10.0, description="Default timeout for GLM DeleteKEM RPC.")

    swm_publish_kem_timeout_s: float = Field(default=10.0, description="Default timeout for SWM PublishKEMToSWM RPC.")
    swm_query_swm_timeout_s: float = Field(default=10.0, description="Default timeout for SWM QuerySWM RPC.")
    swm_load_kems_timeout_s: float = Field(default=20.0, description="Default timeout for SWM LoadKEMsFromGLM RPC.")
    swm_lock_rpc_timeout_s: float = Field(default=5.0, description="Default timeout for SWM Lock RPCs.")
    swm_counter_rpc_timeout_s: float = Field(default=5.0, description="Default timeout for SWM Counter RPCs.")
    swm_query_swm_page_size_default: int = Field(default=50, description="Default page size for SWM QuerySWM when loading into LPA.")


    kps_process_raw_data_timeout_s: float = Field(default=30.0, description="Default timeout for KPS ProcessRawData RPC.")

    model_config = SettingsConfigDict(
        env_prefix='DCSM_SDK_',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    @property
    def glm_address(self) -> Optional[str]:
        return f"{self.glm_host}:{self.glm_port}" if self.glm_host and self.glm_port is not None else None

    @property
    def swm_address(self) -> Optional[str]:
        return f"{self.swm_host}:{self.swm_port}" if self.swm_host and self.swm_port is not None else None

    @property
    def kps_address(self) -> Optional[str]:
        return f"{self.kps_host}:{self.kps_port}" if self.kps_host and self.kps_port is not None else None

if __name__ == '__main__':
    config = DCSMClientSDKConfig()
    print("DCSM SDK Configuration Loaded:")
    print(f"  GLM Address: {config.glm_address}")
    print(f"  SWM Address: {config.swm_address}")
    print(f"  KPS Address: {config.kps_address}")
    print(f"  LPA Max Size: {config.lpa_max_size}")
    print(f"  LPA Indexed Keys: {config.lpa_indexed_keys}")
    print(f"  Connect on Init: {config.connect_on_init}")
    print(f"  Retry Max Attempts: {config.retry_max_attempts}")
    print(f"  TLS Enabled: {config.tls_enabled}")
    print(f"  TLS CA Cert Path: {config.tls_ca_cert_path}")
    print(f"  GLM Retrieve KEMs Timeout: {config.glm_retrieve_kems_timeout_s}s")
    print(f"  SWM Query Page Size Default: {config.swm_query_swm_page_size_default}")
    # ... print other new timeout fields ...
