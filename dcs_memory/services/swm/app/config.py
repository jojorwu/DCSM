from typing import List, Optional
from pydantic import Field
import os

from dcs_memory.common.config import BaseServiceConfig, SettingsConfigDict

class SWMConfig(BaseServiceConfig):
    model_config = SettingsConfigDict(
        env_prefix='SWM_', # Префикс для переменных окружения SWM
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    GLM_SERVICE_ADDRESS: str = "glm:50051" # Адрес GLM, используемый по умолчанию в Docker
    GRPC_LISTEN_ADDRESS: str = "[::]:50053"
    CACHE_MAX_SIZE: int = 200
    DEFAULT_PAGE_SIZE: int = 20 # DEFAULT_SWM_PAGE_SIZE в старом коде

    # SWM_INDEXED_METADATA_KEYS was a list of strings, obtained via split(',')
    # Pydantic can automatically convert a comma-separated string to a List[str]
    # if List[str] type hint is used and the env var is a comma-separated string.
    # pydantic-settings handles this for simple types.
    INDEXED_METADATA_KEYS: List[str] = Field(
        default_factory=list, # Default to an empty list if not provided
        description="Comma-separated list of KEM metadata keys to be indexed by SWM's internal cache. E.g., 'type,source'"
    )

    # Retry parameters for the GLM client within SWM
    GLM_RETRY_MAX_ATTEMPTS: int = Field(default=3, alias="SWM_GLM_RETRY_MAX_ATTEMPTS") # Using alias if a different env var name is preferred
    GLM_RETRY_INITIAL_DELAY_S: float = Field(default=1.0, alias="SWM_GLM_RETRY_INITIAL_DELAY_S")
    GLM_RETRY_BACKOFF_FACTOR: float = Field(default=2.0, alias="SWM_GLM_RETRY_BACKOFF_FACTOR")

    LOCK_CLEANUP_INTERVAL_S: int = Field(
        default=60,
        description="Interval in seconds for the background task that cleans up expired distributed locks."
    )

    # Configuration for subscriber event queues
    SUBSCRIBER_DEFAULT_QUEUE_SIZE: int = Field(
        default=100,
        description="Default queue size for an event subscriber if not specified or out of min/max bounds."
    )
    SUBSCRIBER_MIN_QUEUE_SIZE: int = Field(
        default=10,
        description="Minimum allowed queue size for an event subscriber."
    )
    SUBSCRIBER_MAX_QUEUE_SIZE: int = Field(
        default=1000,
        description="Maximum allowed queue size for an event subscriber."
    )
    SUBSCRIBER_IDLE_CHECK_INTERVAL_S: float = Field(
        default=5.0, # Check for activity every 5 seconds
        description="Interval in seconds for checking subscriber activity. Used as timeout for queue.get()."
    )
    SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD: int = Field(
        default=12, # e.g., 12 * 5s = 1 minute of inactivity
        description="Number of consecutive idle check intervals after which a subscriber is considered inactive and disconnected."
    )

    # Configuration for GLM persistence worker (batching from SWM to GLM)
    GLM_PERSISTENCE_QUEUE_MAX_SIZE: int = Field(
        default=1000,
        description="Maximum size of the internal queue for KEMs pending persistence to GLM."
    )
    GLM_PERSISTENCE_BATCH_SIZE: int = Field(
        default=50,
        description="Number of KEMs to batch together for a single BatchStoreKEMs call to GLM."
    )
    GLM_PERSISTENCE_FLUSH_INTERVAL_S: float = Field(
        default=10.0,
        description="Maximum interval in seconds to wait before flushing the persistence queue to GLM, even if batch size is not reached."
    )
    GLM_PERSISTENCE_BATCH_MAX_RETRIES: int = Field(
        default=3,
        description="Maximum number of retry attempts for a batch that failed to persist to GLM."
    )

if __name__ == '__main__':
    print("--- Тестирование SWMConfig ---")

    # Тест с .env файлом
    with open(".env_test_swm", "w") as f:
        f.write("SWM_GLM_SERVICE_ADDRESS=glm-test:50051\\n")
        f.write("SWM_CACHE_MAX_SIZE=555\\n")
        f.write("SWM_INDEXED_METADATA_KEYS=key1,key2, key3 \\n") # с пробелами для теста
        f.write("LOG_LEVEL=ERROR\\n") # Тест наследования и чтения LOG_LEVEL без префикса SWM_
        f.write("SWM_GLM_RETRY_MAX_ATTEMPTS=5\\n")


    class TestSWMConfig(SWMConfig):
        model_config = SettingsConfigDict(env_file='.env_test_swm', env_prefix='SWM_')

    swm_config = TestSWMConfig()

    print(f"GLM Address: {swm_config.GLM_SERVICE_ADDRESS}")
    assert swm_config.GLM_SERVICE_ADDRESS == "glm-test:50051"

    print(f"Cache Max Size: {swm_config.CACHE_MAX_SIZE}")
    assert swm_config.CACHE_MAX_SIZE == 555

    print(f"Indexed Keys: {swm_config.INDEXED_METADATA_KEYS} (type: {type(swm_config.INDEXED_METADATA_KEYS)})")
    # pydantic-settings должен был распарсить "key1,key2, key3 " в ['key1', 'key2', 'key3']
    assert swm_config.INDEXED_METADATA_KEYS == ["key1", "key2", "key3"]

    print(f"Log Level: {swm_config.LOG_LEVEL}")
    assert swm_config.LOG_LEVEL == "ERROR"
    assert swm_config.get_log_level_int() == logging.ERROR

    print(f"GLM Retry Max Attempts: {swm_config.GLM_RETRY_MAX_ATTEMPTS}")
    assert swm_config.GLM_RETRY_MAX_ATTEMPTS == 5

    # Тест значений по умолчанию
    config_default = SWMConfig() # Без .env файла и без переменных окружения (кроме тех, что могут быть установлены глобально)
    print(f"Default GLM Address: {config_default.GLM_SERVICE_ADDRESS}")
    assert config_default.GLM_SERVICE_ADDRESS == "glm:50051" # Это значение из Dockerfile/docker-compose
    print(f"Default Indexed Keys: {config_default.INDEXED_METADATA_KEYS}")
    assert config_default.INDEXED_METADATA_KEYS == []


    os.environ["SWM_DEFAULT_PAGE_SIZE"] = "77"
    os.environ["SWM_GLM_RETRY_INITIAL_DELAY_S"] = "0.5"
    # Для LOG_LEVEL (из BaseServiceConfig) Pydantic будет искать переменную LOG_LEVEL, а не SWM_LOG_LEVEL
    os.environ["LOG_LEVEL"] = "CRITICAL"

    config_env_override = SWMConfig() # Перечитывает переменные окружения
    print(f"Default Page Size (from env): {config_env_override.DEFAULT_PAGE_SIZE}")
    assert config_env_override.DEFAULT_PAGE_SIZE == 77
    print(f"GLM Retry Initial Delay (from env): {config_env_override.GLM_RETRY_INITIAL_DELAY_S}")
    assert config_env_override.GLM_RETRY_INITIAL_DELAY_S == 0.5
    print(f"Log Level (from env for Base): {config_env_override.LOG_LEVEL}")
    assert config_env_override.LOG_LEVEL == "CRITICAL"


    del os.environ["SWM_DEFAULT_PAGE_SIZE"]
    del os.environ["SWM_GLM_RETRY_INITIAL_DELAY_S"]
    del os.environ["LOG_LEVEL"]
    try:
        os.remove(".env_test_swm")
    except OSError:
        pass

    print("--- Тестирование SWMConfig завершено ---")
