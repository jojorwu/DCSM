import os
import yaml # PyYAML
import logging
from typing import List, Union, Literal, Dict, Any, Tuple, ClassVar, Optional, Type

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict, PydanticBaseSettingsSource
from pydantic.fields import FieldInfo


logger = logging.getLogger(__name__)

# Common base configuration for all services
class BaseServiceConfig(BaseSettings):
    # This key will be used to find the relevant section in the YAML config file.
    # Subclasses (GLMConfig, KPSConfig, SWMConfig) should override this.
    _service_config_key: ClassVar[Optional[str]] = None

    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", # This default is used if not found in YAML, .env, or env var
        description="Logging level for the service."
    )
    GRPC_LISTEN_ADDRESS: str = Field(
        default="[::]:50050", # Generic default, overridden by specific services or YAML
        description="Address and port for the gRPC server to listen on."
    )
    RETRY_MAX_ATTEMPTS: int = Field(default=3, description="Maximum number of attempts for gRPC calls.")
    RETRY_INITIAL_DELAY_S: float = Field(default=1.0, description="Initial delay before retrying in seconds.")
    RETRY_BACKOFF_FACTOR: float = Field(default=2.0, description="Multiplier for exponential backoff of delay.")
    RETRY_JITTER_FRACTION: float = Field(default=0.1, description="Jitter fraction to randomize delays.")
    # General gRPC server settings - can be overridden by service-specific if needed
    GRPC_SERVER_MAX_WORKERS: int = Field(default=10, description="Default max_workers for synchronous gRPC servers.")
    GRPC_SERVER_SHUTDOWN_GRACE_S: int = Field(default=5, description="Default shutdown grace period for gRPC servers in seconds.")


    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:

        yaml_data_for_cls: Dict[str, Any] = {}
        config_file_path = os.getenv("DCSM_CONFIG_FILE", "config.yml") # Default path relative to CWD

        if os.path.exists(config_file_path):
            logger.info(f"Attempting to load configuration from YAML file: {config_file_path}")
            try:
                with open(config_file_path, 'r') as f:
                    full_yaml_data = yaml.safe_load(f) or {}

                # 1. Get 'shared' settings from YAML (if any)
                shared_yaml_data = full_yaml_data.get("shared", {}).copy()
                yaml_data_for_cls.update(shared_yaml_data)

                # 2. Get service-specific settings from YAML
                #    The `_service_config_key` is defined in subclasses like GLMConfig.
                service_key = getattr(settings_cls, '_service_config_key', None)
                if service_key and service_key in full_yaml_data:
                    service_yaml_data = full_yaml_data.get(service_key, {})
                    yaml_data_for_cls.update(service_yaml_data) # Service-specific overrides shared

                # 3. Allow top-level keys in YAML to map to BaseServiceConfig fields
                #    if this is the BaseServiceConfig class itself being instantiated,
                #    and only if they are not service-specific keys.
                #    These are overridden by 'shared' or service-specific sections if names clash.
                if settings_cls == BaseServiceConfig:
                    base_specific_yaml_data = {
                        k: v for k, v in full_yaml_data.items()
                        if k not in ["shared", "glm", "kps", "swm"] # Example service keys
                    }
                    # Merge base_specific_yaml_data first, so it's overridden
                    temp_merged = base_specific_yaml_data
                    temp_merged.update(yaml_data_for_cls)
                    yaml_data_for_cls = temp_merged

                if yaml_data_for_cls:
                    logger.info(f"Successfully loaded and processed YAML data for {settings_cls.__name__} from {config_file_path}")
                else:
                    logger.info(f"No specific or shared YAML data found for {settings_cls.__name__} in {config_file_path}, or file was empty.")

            except yaml.YAMLError as e:
                logger.error(f"Error parsing YAML configuration file {config_file_path}: {e}", exc_info=True)
            except IOError as e:
                logger.error(f"Error reading YAML configuration file {config_file_path}: {e}", exc_info=True)
        else:
            # Changed to debug level as it's common for this file not to exist in some environments
            logger.debug(f"YAML configuration file not found at {config_file_path}. Using other sources (env, defaults).")

        # Custom source class that provides the loaded YAML data
        class YamlFileSettingsSource(PydanticBaseSettingsSource):
            def __init__(self, settings_cls: Type[BaseSettings], data: Dict[str, Any]):
                super().__init__(settings_cls)
                self.data = data

            def get_field_value(self, field: FieldInfo, field_name: str) -> Tuple[Any, str, bool]:
                # Pydantic expects field_value, field_key, value_is_complex
                # field_key is the original key in the source, field_name is the model field name
                # For simple dicts like ours, field_name is usually the key.
                field_value = self.data.get(field_name) # Use field_name which respects aliases
                return field_value, field_name, field.is_complex_field()


            def prepare_field_value(self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool) -> Any:
                return value # No special preparation needed for values from YAML

            def __call__(self) -> Dict[str, Any]:
                return self.data

        # Order of sources: First in tuple has HIGHEST priority.
        # We want: init_settings > env_settings > dotenv_settings > yaml_settings > file_secret_settings
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlFileSettingsSource(settings_cls, yaml_data_for_cls), # Our YAML data
            file_secret_settings,
        )

    model_config = SettingsConfigDict(
        env_prefix='', # Base fields usually don't have a prefix unless specified by `Field(validation_alias=...)`
        extra='ignore',
        env_file='.env',
        env_file_encoding='utf-8',
        # settings_customise_sources is implicitly used by Pydantic if defined as a @classmethod
    )

    def get_log_level_int(self) -> int:
        return getattr(logging, self.LOG_LEVEL.upper(), logging.INFO)


# Configuration for GLM service
class GLMConfig(BaseServiceConfig):
    _service_config_key: ClassVar[str] = "glm" # Key for YAML lookup
    model_config = SettingsConfigDict(env_prefix='GLM_', extra='ignore') # .env handled by Base

    DB_FILENAME: str = Field(description="SQLite database filename for GLM metadata.")
    QDRANT_HOST: str = Field(description="Qdrant host.")
    QDRANT_PORT: int = Field(description="Qdrant gRPC port.")
    QDRANT_COLLECTION: str = Field(description="Qdrant collection name for GLM KEMs.")
    DEFAULT_VECTOR_SIZE: int = Field(description="Default vector dimension for Qdrant.")
    DEFAULT_PAGE_SIZE: int = Field(description="Default page size for RetrieveKEMs.")
    SQLITE_BUSY_TIMEOUT: int = Field(default=7500, description="SQLite busy timeout in milliseconds.")
    SQLITE_CONNECT_TIMEOUT_S: int = Field(default=10, description="SQLite connection timeout in seconds.")

    QDRANT_CLIENT_TIMEOUT_S: int = Field(default=10, description="Timeout for Qdrant client operations in seconds.")
    QDRANT_DEFAULT_DISTANCE_METRIC: Literal["COSINE", "DOT", "EUCLID"] = Field(default="COSINE", description="Default distance metric for Qdrant collections.")
    QDRANT_PREFLIGHT_CHECK_TIMEOUT_S: int = Field(default=2, description="Timeout for Qdrant pre-flight check in seconds.")
    # GRPC_SERVER_MAX_WORKERS can be inherited from Base or overridden here if GLM needs specific value
    # GRPC_SERVER_SHUTDOWN_GRACE_S can be inherited or overridden


# Configuration for KPS service
class KPSConfig(BaseServiceConfig):
    _service_config_key: ClassVar[str] = "kps"
    model_config = SettingsConfigDict(env_prefix='KPS_', extra='ignore')

    GLM_SERVICE_ADDRESS: str = Field(description="Address of the GLM service KPS connects to.")
    EMBEDDING_MODEL_NAME: str = Field(description="Name or path to the sentence-transformer model.")
    DEFAULT_VECTOR_SIZE: int = Field(description="Expected/generated embedding dimension.")
    DEFAULT_PROCESSING_BATCH_SIZE: int = Field(default=32, description="Default batch size for processing documents.")
    GLM_STORE_KEM_TIMEOUT_S: int = Field(default=15, description="Timeout for KPS calling GLM's StoreKEM, in seconds.")
    # GRPC_SERVER_MAX_WORKERS can be inherited or overridden
    # GRPC_SERVER_SHUTDOWN_GRACE_S can be inherited or overridden


# Configuration for SWM service
class SWMConfig(BaseServiceConfig):
    _service_config_key: ClassVar[str] = "swm"
    model_config = SettingsConfigDict(env_prefix='SWM_', extra='ignore')

    GLM_SERVICE_ADDRESS: str = Field(description="Address of the GLM service for SWM.")
    MAX_CACHE_SIZE_KEMS: int = Field(description="Maximum number of KEMs in SWM's LRU cache (conceptual for Redis, actual eviction by Redis policies).")
    DEFAULT_KEM_TTL_SECONDS: int = Field(default=3600, description="Default TTL for KEMs in cache in seconds (used by RedisKemCache).")

    REDIS_HOST: str = Field(description="Redis host for SWM.")
    REDIS_PORT: int = Field(default=6379, description="Redis port for SWM.")
    SWM_REDIS_PASSWORD: Optional[str] = Field(default=None, description="Password for Redis connection, if any.") # Added for completeness
    SWM_REDIS_DB: int = Field(default=0, description="Default Redis DB number for SWM main data (used by RedisKemCache if specific DBs not set).") # Placeholder, specific DBs below are better
    REDIS_DB_PUBSUB: int = Field(default=0, description="Redis DB number for Pub/Sub.") # Already exists, kept for clarity
    REDIS_DB_CACHE_INDEX: int = Field(default=1, description="Redis DB number for Cache Index & Data.") # Already exists
    REDIS_DB_LOCKS_COUNTERS: int = Field(default=2, description="Redis DB number for Locks and Counters.") # Already exists

    # GLM Client specific timeouts for SWM
    GLM_RETRIEVE_TIMEOUT_S: int = Field(default=20, description="Timeout for SWM calling GLM RetrieveKEMs (async).")
    GLM_STORE_TIMEOUT_S: int = Field(default=10, description="Timeout for SWM calling GLM StoreKEM (async).")
    GLM_BATCH_STORE_TIMEOUT_S: int = Field(default=30, description="Timeout for SWM calling GLM BatchStoreKEMs (async).")

    # GLM Persistence Worker settings
    GLM_PERSISTENCE_QUEUE_MAX_SIZE: int = Field(default=1000, description="Max size of the GLM persistence queue.")
    GLM_PERSISTENCE_FLUSH_INTERVAL_S: float = Field(default=5.0, description="Interval to check/flush persistence queue.")
    GLM_PERSISTENCE_BATCH_SIZE: int = Field(default=50, description="Batch size for sending KEMs to GLM from persistence queue.")
    GLM_PERSISTENCE_BATCH_MAX_RETRIES: int = Field(default=3, description="Max retries for a failed batch in persistence worker.")
    GLM_PERSISTENCE_WORKER_SHUTDOWN_GRACE_S: int = Field(default=5, description="Grace period for persistence worker shutdown.")

    # Redis Pub/Sub Listener settings
    REDIS_PUBSUB_GET_MESSAGE_TIMEOUT_S: float = Field(default=1.0, description="Timeout for getting a message from Redis pub/sub.")
    REDIS_PUBSUB_ERROR_SLEEP_S: float = Field(default=1.0, description="Sleep duration after an error in pub/sub listener.")
    REDIS_MAX_PUBSUB_RETRIES: int = Field(default=5, description="Max consecutive retries for Redis pub/sub connection/subscription.")
    REDIS_RECONNECT_DELAY_S: float = Field(default=5.0, description="Delay before attempting to reconnect to Redis pub/sub.")

    # Subscriber settings
    SUBSCRIBER_MIN_QUEUE_SIZE: int = Field(default=10, description="Minimum queue size for an event subscriber.")
    SUBSCRIBER_DEFAULT_QUEUE_SIZE: int = Field(default=100, description="Default queue size for an event subscriber.")
    SUBSCRIBER_MAX_QUEUE_SIZE: int = Field(default=1000, description="Maximum queue size for an event subscriber.")
    SUBSCRIBER_IDLE_CHECK_INTERVAL_S: float = Field(default=5.0, description="Interval to check subscriber stream for idleness.")
    SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD: int = Field(default=6, description="Number of idle check intervals before deeming a subscriber inactive.") # e.g., 6 * 5s = 30s

    # Lock Manager settings
    # LOCK_CLEANUP_INTERVAL_S is already in SWMConfig
    LOCK_CLEANUP_SHUTDOWN_GRACE_S: int = Field(default=2, description="Grace period for lock cleanup task shutdown.")

    # Redis Transaction settings (for RedisKemCache)
    REDIS_TRANSACTION_MAX_RETRIES: int = Field(default=3, description="Max retries for Redis WATCH/MULTI/EXEC transactions.")
    REDIS_TRANSACTION_RETRY_INITIAL_DELAY_S: float = Field(default=0.01, description="Initial delay for Redis transaction retry.")
    REDIS_TRANSACTION_RETRY_BACKOFF_FACTOR: float = Field(default=2.0, description="Backoff factor for Redis transaction retry delay.")

    # SWM_INDEXED_METADATA_KEYS is already in SWMConfig
    # GRPC_SERVER_SHUTDOWN_GRACE_S can be inherited from BaseServiceConfig for SWM (async server)


    @field_validator("SWM_INDEXED_METADATA_KEYS", mode="before")
    @classmethod
    def _parse_comma_separated_list(cls, value: Union[str, List[str]]) -> List[str]:
        if isinstance(value, str):
            return [key.strip() for key in value.split(',') if key.strip()]
        elif isinstance(value, list):
            return [str(key).strip() for key in value if str(key).strip()]
        return []

# Note: The __main__ block below is for quick testing of this config.py file directly.
# For it to work correctly with the new YAML loading, you'd need a sample 'config.yml'
# in the same directory or point DCSM_CONFIG_FILE env var to it.
if __name__ == '__main__':
    # Create a dummy config.yml for testing
    dummy_config_content = """
shared:
  log_level: "DEBUG" # Base log level from shared
  retry_max_attempts: 5 # Shared retry attempts

glm:
  db_filename: "glm_from_yaml.db"
  qdrant_host: "qdrant.yaml.host"
  grpc_listen_address: "[::]:55551" # GLM specific
  log_level: "INFO" # GLM overrides shared log_level

kps:
  glm_service_address: "glm.yaml.host:55551"
  embedding_model_name: "model-from-yaml"
  # KPS will use shared log_level (DEBUG)

swm:
  max_cache_size_kems: 999
  redis_host: "redis.yaml.host"
  log_level: "WARNING" # SWM specific
"""
    with open("config_temp_test.yml", "w") as f:
        f.write(dummy_config_content)

    os.environ["DCSM_CONFIG_FILE"] = "config_temp_test.yml"
    # Test environment variable override
    os.environ["GLM_QDRANT_PORT"] = "7777" # GLM specific env var
    os.environ["LOG_LEVEL"] = "ERROR" # Global env var for LOG_LEVEL (should affect KPS)


    print("--- GLM Config (Testing YAML + Env Override) ---")
    glm_conf = GLMConfig()
    print(f"GLM Log Level: {glm_conf.LOG_LEVEL} (Expected: INFO from GLM YAML section)")
    assert glm_conf.LOG_LEVEL == "INFO"
    print(f"GLM DB Filename: {glm_conf.DB_FILENAME} (Expected: glm_from_yaml.db)")
    assert glm_conf.DB_FILENAME == "glm_from_yaml.db"
    print(f"GLM Qdrant Host: {glm_conf.QDRANT_HOST} (Expected: qdrant.yaml.host)")
    assert glm_conf.QDRANT_HOST == "qdrant.yaml.host"
    print(f"GLM Qdrant Port: {glm_conf.QDRANT_PORT} (Expected: 7777 from ENV var)")
    assert glm_conf.QDRANT_PORT == 7777
    print(f"GLM gRPC Address: {glm_conf.GRPC_LISTEN_ADDRESS} (Expected: [::]:55551 from GLM YAML)")
    assert glm_conf.GRPC_LISTEN_ADDRESS == "[::]:55551"
    print(f"GLM Retry Max Attempts: {glm_conf.RETRY_MAX_ATTEMPTS} (Expected: 5 from shared YAML)")
    assert glm_conf.RETRY_MAX_ATTEMPTS == 5


    print("\n--- KPS Config (Testing YAML Shared + Global Env Override) ---")
    kps_conf = KPSConfig()
    # LOG_LEVEL precedence: KPS_LOG_LEVEL (env) > LOG_LEVEL (global env) > kps.log_level (YAML) > shared.log_level (YAML) > default
    print(f"KPS Log Level: {kps_conf.LOG_LEVEL} (Expected: ERROR from global LOG_LEVEL env var)")
    assert kps_conf.LOG_LEVEL == "ERROR"
    print(f"KPS GLM Address: {kps_conf.GLM_SERVICE_ADDRESS} (Expected: glm.yaml.host:55551)")
    assert kps_conf.GLM_SERVICE_ADDRESS == "glm.yaml.host:55551"
    print(f"KPS Model: {kps_conf.EMBEDDING_MODEL_NAME} (Expected: model-from-yaml)")
    assert kps_conf.EMBEDDING_MODEL_NAME == "model-from-yaml"
    print(f"KPS Retry Max Attempts: {kps_conf.RETRY_MAX_ATTEMPTS} (Expected: 5 from shared YAML)")
    assert kps_conf.RETRY_MAX_ATTEMPTS == 5


    print("\n--- SWM Config (Testing YAML Specific) ---")
    swm_conf = SWMConfig()
    print(f"SWM Log Level: {swm_conf.LOG_LEVEL} (Expected: WARNING from SWM YAML)")
    assert swm_conf.LOG_LEVEL == "WARNING"
    print(f"SWM Max Cache KEMS: {swm_conf.MAX_CACHE_SIZE_KEMS} (Expected: 999)")
    assert swm_conf.MAX_CACHE_SIZE_KEMS == 999
    print(f"SWM Redis Host: {swm_conf.REDIS_HOST} (Expected: redis.yaml.host)")
    assert swm_conf.REDIS_HOST == "redis.yaml.host"
    print(f"SWM Retry Max Attempts: {swm_conf.RETRY_MAX_ATTEMPTS} (Expected: 5 from shared YAML)")
    assert swm_conf.RETRY_MAX_ATTEMPTS == 5

    # Cleanup
    del os.environ["DCSM_CONFIG_FILE"]
    if "GLM_QDRANT_PORT" in os.environ: del os.environ["GLM_QDRANT_PORT"]
    if "LOG_LEVEL" in os.environ: del os.environ["LOG_LEVEL"]
    try:
        os.remove("config_temp_test.yml")
    except OSError:
        pass
    print("\n--- Config test complete ---")

```
