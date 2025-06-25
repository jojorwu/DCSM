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

    # SWM_INDEXED_METADATA_KEYS был списком строк, получаемым через split(',')
    # Pydantic может автоматически преобразовывать строку в список, если указать тип List[str]
    # и если переменная окружения будет строкой с разделителями (запятая по умолчанию для pydantic-settings, если не указано иное)
    # Или можно использовать Field с json_loads, если переменная окружения будет JSON-строкой списка
    # Или оставить как строку и парсить в main.py при передаче в IndexedLRUCache.
    # Для простоты оставим как строку, которую нужно будет парсить.
    # Либо, pydantic-settings может сам распарсить 'val1,val2' в List[str]
    # Проверим документацию pydantic-settings для CSV. Да, он это умеет для простых типов.
    INDEXED_METADATA_KEYS: List[str] = [] # Например, SWM_INDEXED_METADATA_KEYS="type,source"

    # Параметры Retry для GLM клиента внутри SWM
    GLM_RETRY_MAX_ATTEMPTS: int = Field(default=3, alias="SWM_GLM_RETRY_MAX_ATTEMPTS") # Используем alias, если хотим другое имя env var
    GLM_RETRY_INITIAL_DELAY_S: float = Field(default=1.0, alias="SWM_GLM_RETRY_INITIAL_DELAY_S")
    GLM_RETRY_BACKOFF_FACTOR: float = Field(default=2.0, alias="SWM_GLM_RETRY_BACKOFF_FACTOR")


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
