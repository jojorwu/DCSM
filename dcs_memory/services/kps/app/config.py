from typing import Optional
from pydantic import Field
import os
import logging

from dcs_memory.common.config import BaseServiceConfig, SettingsConfigDict

class KPSConfig(BaseServiceConfig):
    model_config = SettingsConfigDict(
        env_prefix='KPS_', # Префикс для переменных окружения KPS
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    GLM_SERVICE_ADDRESS: str = "glm:50051" # Адрес GLM, используемый по умолчанию в Docker
    GRPC_LISTEN_ADDRESS: str = "[::]:50052"

    SENTENCE_TRANSFORMER_MODEL: str = "all-MiniLM-L6-v2"
    DEFAULT_VECTOR_SIZE: int = 384 # Должно соответствовать модели и настройкам GLM

    # Параметры Retry для GLM клиента внутри KPS
    # Используем Field с alias, чтобы переменные окружения были KPS_GLM_RETRY..., а поля в конфиге GLM_RETRY...
    GLM_RETRY_MAX_ATTEMPTS: int = Field(default=3, alias="KPS_GLM_RETRY_MAX_ATTEMPTS")
    GLM_RETRY_INITIAL_DELAY_S: float = Field(default=1.0, alias="KPS_GLM_RETRY_INITIAL_DELAY_S")
    GLM_RETRY_BACKOFF_FACTOR: float = Field(default=2.0, alias="KPS_GLM_RETRY_BACKOFF_FACTOR")

if __name__ == '__main__':
    print("--- Тестирование KPSConfig ---")

    # Тест с .env файлом
    with open(".env_test_kps", "w") as f:
        f.write("KPS_GLM_SERVICE_ADDRESS=glm-kps-test:50051\\n")
        f.write("KPS_SENTENCE_TRANSFORMER_MODEL=custom-model\\n")
        f.write("LOG_LEVEL=DEBUG\\n") # Тестируем базовый параметр
        f.write("KPS_GLM_RETRY_MAX_ATTEMPTS=2\\n")

    class TestKPSConfig(KPSConfig):
        model_config = SettingsConfigDict(env_file='.env_test_kps', env_prefix='KPS_')

    kps_config = TestKPSConfig()

    print(f"GLM Address: {kps_config.GLM_SERVICE_ADDRESS}")
    assert kps_config.GLM_SERVICE_ADDRESS == "glm-kps-test:50051"

    print(f"Sentence Transformer Model: {kps_config.SENTENCE_TRANSFORMER_MODEL}")
    assert kps_config.SENTENCE_TRANSFORMER_MODEL == "custom-model"

    print(f"Log Level: {kps_config.LOG_LEVEL}") # Должен быть из .env_test_kps
    assert kps_config.LOG_LEVEL == "DEBUG"
    assert kps_config.get_log_level_int() == logging.DEBUG

    print(f"GLM Retry Max Attempts: {kps_config.GLM_RETRY_MAX_ATTEMPTS}")
    assert kps_config.GLM_RETRY_MAX_ATTEMPTS == 2


    # Тест переопределения через переменные окружения
    os.environ["KPS_DEFAULT_VECTOR_SIZE"] = "768"
    os.environ["LOG_LEVEL"] = "WARNING" # Переопределяем базовый

    config_env_override = KPSConfig() # Читает из окружения, т.к. .env_test_kps не указан здесь

    print(f"Default Vector Size (from env): {config_env_override.DEFAULT_VECTOR_SIZE}")
    assert config_env_override.DEFAULT_VECTOR_SIZE == 768

    print(f"Log Level (from env for Base): {config_env_override.LOG_LEVEL}")
    assert config_env_override.LOG_LEVEL == "WARNING"


    del os.environ["KPS_DEFAULT_VECTOR_SIZE"]
    del os.environ["LOG_LEVEL"]
    try:
        os.remove(".env_test_kps")
    except OSError:
        pass

    print("--- Тестирование KPSConfig завершено ---")
