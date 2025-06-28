from pydantic import Field, field_validator, model_validator, PositiveInt, NonNegativeFloat, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Union, Literal, Dict, Any
import logging # For log level conversion

# Common base configuration for all services
class BaseServiceConfig(BaseSettings):
    # Pydantic settings configuration:
    # env_prefix: Prefix for environment variables (e.g., 'APP_' would mean APP_LOG_LEVEL)
    # extra: 'ignore' - ignore extra fields not defined in the model
    # env_file: Path to .env file to load variables from
    # env_file_encoding: Encoding of the .env file
    model_config = SettingsConfigDict(env_prefix='', extra='ignore', env_file='.env', env_file_encoding='utf-8')

    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level for the service."
    )
    GRPC_LISTEN_ADDRESS: str = Field(
        default="[::]:50050", # Generic default, should be overridden by specific services
        description="Address and port for the gRPC server to listen on."
    )
    # Common parameters for gRPC client retry logic, if used by the service.
    # These can be overridden or supplemented in specific service configs
    # for different clients (e.g., GLM_RETRY_MAX_ATTEMPTS for a GLM client).
    RETRY_MAX_ATTEMPTS: NonNegativeInt = Field(default=3, description="Maximum number of attempts for gRPC calls.")
    RETRY_INITIAL_DELAY_S: NonNegativeFloat = Field(default=1.0, description="Initial delay before retrying in seconds.")
    RETRY_BACKOFF_FACTOR: float = Field(default=2.0, ge=1.0, description="Multiplier for exponential backoff of delay (must be >= 1.0).")
    RETRY_JITTER_FRACTION: float = Field(default=0.1, ge=0.0, le=1.0, description="Jitter fraction to randomize delays (must be between 0.0 and 1.0).")

    @field_validator("GRPC_LISTEN_ADDRESS")
    @classmethod
    def validate_listen_address(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("GRPC_LISTEN_ADDRESS must be a string")
        try:
            host, port_str = v.rsplit(":", 1)
            port = int(port_str)
            if not (0 <= port <= 65535): # 0 can be valid for OS to pick a port
                raise ValueError("Port number must be between 0 and 65535")
        except ValueError:
            raise ValueError("GRPC_LISTEN_ADDRESS format error. Expected 'host:port' or '[ipv6_addr]:port'")
        return v

    def get_log_level_int(self) -> int:
        # Converts string LOG_LEVEL to the corresponding value from the logging module
        return getattr(logging, self.LOG_LEVEL.upper(), logging.INFO)


# Configuration for GLM service
class GLMConfig(BaseServiceConfig):
    model_config = SettingsConfigDict(env_prefix='GLM_', extra='ignore', env_file='.env', env_file_encoding='utf-8')

    DB_FILENAME: str = Field(default="glm_metadata.sqlite3", description="SQLite database filename for GLM metadata.")
    QDRANT_HOST: str = Field(default="localhost", description="Qdrant host.")
    QDRANT_PORT: int = Field(default=6333, ge=1, le=65535, description="Qdrant gRPC port.")
    QDRANT_COLLECTION: str = Field(default="glm_kems_default_collection", min_length=1, description="Qdrant collection name for GLM KEMs.")
    DEFAULT_VECTOR_SIZE: PositiveInt = Field(default=384, description="Default vector dimension for Qdrant (must match embedding model).")
    DEFAULT_PAGE_SIZE: PositiveInt = Field(default=10, description="Default page size for RetrieveKEMs.")
    GRPC_LISTEN_ADDRESS: str = Field(default="[::]:50051", description="GLM gRPC listen address.")
    # Note: GRPC_LISTEN_ADDRESS validation is handled by BaseServiceConfig


# Configuration for KPS service
class KPSConfig(BaseServiceConfig):
    model_config = SettingsConfigDict(env_prefix='KPS_', extra='ignore', env_file='.env', env_file_encoding='utf-8')

    GLM_SERVICE_ADDRESS: str = Field(default="localhost:50051", description="Address of the GLM service KPS connects to.")
    SENTENCE_TRANSFORMER_MODEL: str = Field(default="all-MiniLM-L6-v2", min_length=1, description="Name or path to the sentence-transformer model.")
    DEFAULT_VECTOR_SIZE: PositiveInt = Field(default=384, description="Expected/generated embedding dimension (must match GLM.DEFAULT_VECTOR_SIZE).")
    GRPC_LISTEN_ADDRESS: str = Field(default="[::]:50052", description="KPS gRPC listen address.")
    # Note: GRPC_LISTEN_ADDRESS validation is handled by BaseServiceConfig
    # KPS-specific retry parameters for its GLM client could be added here if needed
    # e.g., KPS_GLM_RETRY_MAX_ATTEMPTS: int = Field(default=3) ...


# Configuration for SWM service
class SWMConfig(BaseServiceConfig):
    model_config = SettingsConfigDict(env_prefix='SWM_', extra='ignore', env_file='.env', env_file_encoding='utf-8')

    GLM_SERVICE_ADDRESS: str = Field(default="localhost:50051", description="Address of the GLM service for SWM.")
    CACHE_MAX_SIZE: NonNegativeInt = Field(default=200, description="Maximum number of KEMs in SWM's LRU cache (0 for unlimited, though not typical for LRU).")
    DEFAULT_PAGE_SIZE: PositiveInt = Field(default=20, description="Default page size for QuerySWM.")
    GRPC_LISTEN_ADDRESS: str = Field(default="[::]:50053", description="SWM gRPC listen address.")
    # Note: GRPC_LISTEN_ADDRESS validation is handled by BaseServiceConfig

    SWM_INDEXED_METADATA_KEYS: List[str] = Field(
        default_factory=list,
        description="Comma-separated list of KEM metadata keys to be indexed by SWM's internal cache. Example: 'type,source,project_id'"
    )
    LOCK_CLEANUP_INTERVAL_S: PositiveInt = Field(
        default=60,
        description="Interval in seconds for the background task that cleans up expired distributed locks."
    )
    # SWM-specific retry parameters for its GLM client (inherits structure from BaseServiceConfig, could be overridden if needed)
    # GLM_RETRY_MAX_ATTEMPTS: NonNegativeInt = Field(default=3, description="Max retry attempts for SWM's GLM client.")
    # GLM_RETRY_INITIAL_DELAY_S: NonNegativeFloat = Field(default=1.0, description="Initial retry delay for SWM's GLM client.")
    # GLM_RETRY_BACKOFF_FACTOR: float = Field(default=2.0, ge=1.0, description="Retry backoff factor for SWM's GLM client.")

    SWM_SUBSCRIBER_DEFAULT_QUEUE_SIZE: PositiveInt = Field(default=100, description="Default queue size for SWM event subscribers.")
    SWM_SUBSCRIBER_MIN_QUEUE_SIZE: PositiveInt = Field(default=10, description="Minimum allowed queue size for SWM event subscribers.")
    SWM_SUBSCRIBER_MAX_QUEUE_SIZE: PositiveInt = Field(default=1000, description="Maximum allowed queue size for SWM event subscribers.")

    @model_validator(mode='after')
    def validate_queue_sizes(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        min_q = values.get('SWM_SUBSCRIBER_MIN_QUEUE_SIZE')
        def_q = values.get('SWM_SUBSCRIBER_DEFAULT_QUEUE_SIZE')
        max_q = values.get('SWM_SUBSCRIBER_MAX_QUEUE_SIZE')

        if not (min_q is not None and def_q is not None and max_q is not None):
            # This should not happen if defaults are set and types are correct, but as a safeguard
            return values # Or raise error

        if not (min_q <= def_q <= max_q):
            raise ValueError(
                "SWM subscriber queue sizes must satisfy: MIN_QUEUE_SIZE <= DEFAULT_QUEUE_SIZE <= MAX_QUEUE_SIZE. "
                f"Got MIN={min_q}, DEFAULT={def_q}, MAX={max_q}"
            )
        return values

    @field_validator("SWM_INDEXED_METADATA_KEYS", mode="before")
    @classmethod
    def _parse_comma_separated_list(cls, value: Union[str, List[str]]) -> List[str]:
        if isinstance(value, str):
            return [key.strip() for key in value.split(',') if key.strip()]
        elif isinstance(value, list):
            return [str(key).strip() for key in value if str(key).strip()]
        return []

if __name__ == '__main__':
    # Example usage of configurations (for debugging)
    print("--- GLM Config ---")
    glm_conf = GLMConfig()
    print(f"GLM Log Level: {glm_conf.LOG_LEVEL} ({glm_conf.get_log_level_int()})")
    print(f"GLM DB Filename: {glm_conf.DB_FILENAME}")
    print(f"GLM Qdrant Host: {glm_conf.QDRANT_HOST}")
    print(f"GLM gRPC Address: {glm_conf.GRPC_LISTEN_ADDRESS}")

    print("\n--- KPS Config ---")
    kps_conf = KPSConfig()
    print(f"KPS Log Level: {kps_conf.LOG_LEVEL}")
    print(f"KPS GLM Address: {kps_conf.GLM_SERVICE_ADDRESS}")
    print(f"KPS Model: {kps_conf.SENTENCE_TRANSFORMER_MODEL}")
    print(f"KPS Vector Size: {kps_conf.DEFAULT_VECTOR_SIZE}")

    print("\n--- SWM Config ---")
    # To test SWM_INDEXED_METADATA_KEYS via environment variables, e.g.:
    # export SWM_INDEXED_METADATA_KEYS="type,source"
    swm_conf = SWMConfig()
    print(f"SWM Log Level: {swm_conf.LOG_LEVEL}")
    print(f"SWM GLM Address: {swm_conf.GLM_SERVICE_ADDRESS}")
    print(f"SWM Cache Max Size: {swm_conf.CACHE_MAX_SIZE}")
    print(f"SWM Indexed Keys: {swm_conf.SWM_INDEXED_METADATA_KEYS} (type: {type(swm_conf.SWM_INDEXED_METADATA_KEYS)})")
    print(f"SWM Lock Cleanup Interval: {swm_conf.LOCK_CLEANUP_INTERVAL_S}s")
    print(f"SWM GLM Retry Max Attempts: {swm_conf.GLM_RETRY_MAX_ATTEMPTS}")


    print("\n--- Base Config Retry Defaults (loaded if no prefix matches) ---")
    base_conf = BaseServiceConfig() # Will load from .env if fields are not prefixed by GLM_, KPS_, SWM_
    print(f"Default Max Attempts (from Base): {base_conf.RETRY_MAX_ATTEMPTS}")
    print(f"Default Initial Delay (from Base): {base_conf.RETRY_INITIAL_DELAY_S}")
    print(f"Default Backoff Factor (from Base): {base_conf.RETRY_BACKOFF_FACTOR}")
    print(f"Default Jitter Fraction (from Base): {base_conf.RETRY_JITTER_FRACTION}")

```
