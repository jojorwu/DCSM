from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field # Field can be used for more detailed model field configuration
from typing import Optional, List, Dict, Any

class ServiceEndpointConfig(BaseSettings):
    """Configuration for a single gRPC service endpoint."""
    host: str = "localhost"
    port: int

    # Future considerations for TLS:
    # secure: bool = False
    # ca_cert_path: Optional[str] = None
    # client_cert_path: Optional[str] = None
    # client_key_path: Optional[str] = None

    # Pydantic model config
    model_config = SettingsConfigDict(extra='ignore')

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

class DCSMClientSDKConfig(BaseSettings):
    """
    Configuration for the DCSM Agent SDK.
    Can be loaded from environment variables (prefixed with DCSM_SDK_) or a .env file.
    """
    # Service endpoint configurations
    # These are structured to allow nested environment variable loading, e.g.,
    # DCSM_SDK_GLM__HOST, DCSM_SDK_GLM__PORT
    # DCSM_SDK_SWM__HOST, DCSM_SDK_SWM__PORT
    # DCSM_SDK_KPS__HOST, DCSM_SDK_KPS__PORT
    # Note the double underscore for Pydantic's nested model env var parsing.
    # However, Pydantic v2 might handle direct glm_host, glm_port better with Field(alias=...).
    # For simplicity here with BaseSettings, we might need to adjust or use custom parsing if
    # direct GLM_HOST type env vars are desired without the SDK_ prefix for sub-models.
    # Let's use Field to make them optional and provide defaults directly.

    glm_host: Optional[str] = Field(default="localhost")
    glm_port: int = Field(default=50051)

    swm_host: Optional[str] = Field(default="localhost")
    swm_port: int = Field(default=50052) # Corrected from sdk.py's default of 50053 for swm

    kps_host: Optional[str] = Field(default="localhost")
    kps_port: int = Field(default=50053)

    # Local Agent Memory (LAM) / Local Persistent Array (LPA) settings
    lpa_max_size: int = Field(default=100)
    lpa_indexed_keys: List[str] = Field(default_factory=list)

    connect_on_init: bool = Field(default=True)

    # Client-side retry policy defaults
    retry_max_attempts: int = Field(default=3)
    retry_initial_delay_s: float = Field(default=1.0)
    retry_backoff_factor: float = Field(default=2.0)
    retry_jitter_fraction: float = Field(default=0.1) # Added jitter

    # Global TLS settings (optional, can be overridden per service if needed later)
    # If these are set, SDK will attempt to use secure channels.
    # For mTLS, client_cert and client_key would also be needed.
    tls_enabled: bool = Field(default=False, description="Enable TLS for all client connections.")
    tls_ca_cert_path: Optional[str] = Field(default=None, description="Path to the CA certificate file for verifying server certs.")
    # For mTLS:
    tls_client_cert_path: Optional[str] = Field(default=None, description="Path to client's TLS certificate file.")
    tls_client_key_path: Optional[str] = Field(default=None, description="Path to client's TLS key file.")
    tls_server_override_authority: Optional[str] = Field(default=None, description="The server name to use for SSL verification.")


    # Pydantic model config
    model_config = SettingsConfigDict(
        env_prefix='DCSM_SDK_', # e.g. DCSM_SDK_GLM_HOST (Pydantic will look for this)
                               # This prefix applies to top-level fields.
                               # For aliased fields like glm_host, it respects the alias.
        case_sensitive=False,
        env_file='.env',       # Load .env file if present
        env_file_encoding='utf-8',
        extra='ignore'         # Ignore extra fields from env or file
    )

    # Helper properties to get full addresses
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
    # Example usage:
    # Create a .env file with lines like:
    # DCSM_SDK_GLM_HOST=remote_glm_server
    # DCSM_SDK_GLM_PORT=50001
    # DCSM_SDK_LPA_MAX_SIZE=200
    # DCSM_SDK_TLS_ENABLED=true
    # DCSM_SDK_TLS_CA_CERT_PATH=/path/to/ca.crt

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

    # Example of overriding with specific env vars (if you were to run this with them set)
    # os.environ["DCSM_SDK_GLM_HOST"] = "override.host.com"
    # config_override = DCSMClientSDKConfig()
    # print(f"\n  Overridden GLM Host: {config_override.glm_host}")

