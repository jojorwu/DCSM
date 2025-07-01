from typing import Optional
from pydantic import Field # Для Field, если нужны будут более сложные валидации или алиасы
import os

# Добавляем путь к common, если запускаем этот файл напрямую (для тестов config)
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
# Это не очень хороший способ, лучше чтобы PYTHONPATH был настроен при запуске тестов.
# Для импорта BaseServiceConfig, предполагаем, что dcs_memory в PYTHONPATH.
from dcs_memory.common.config import BaseServiceConfig, SettingsConfigDict

class GLMConfig(BaseServiceConfig):
    # Pydantic V2: model_config как словарь
    model_config = SettingsConfigDict(
        env_prefix='GLM_', # Префикс для переменных окружения, например GLM_DB_FILENAME
        case_sensitive=False,
        env_file='.env', # Общий .env файл, если есть
        env_file_encoding='utf-8',
        extra='ignore'
    )

    # Путь к файлу БД SQLite. os.path.join(app_dir, ...) будет сделан в main.py
    # Здесь мы храним только имя файла или относительный путь от app_dir
    DB_FILENAME: str = "glm_metadata.sqlite3"

    QDRANT_HOST: str = "127.0.0.1" # Уже исправлено с localhost
    QDRANT_PORT: int = 6333
    QDRANT_COLLECTION: str = "glm_kems_demo_collection"

    DEFAULT_VECTOR_SIZE: int = 384 # Изменен дефолт с 25 на более реалистичный
    DEFAULT_PAGE_SIZE: int = 10
    SQLITE_BUSY_TIMEOUT: int = 7500 # milliseconds

    GRPC_LISTEN_ADDRESS: str = "[::]:50051"

    # Параметры для QdrantClient, если нужны (timeout и т.д., пока не добавляем)
    # QDRANT_CLIENT_TIMEOUT: int = 10

    # Переопределяем LOG_LEVEL, если нужно специфичное для GLM значение по умолчанию,
    # или оно будет наследоваться из BaseServiceConfig (INFO)
    # LOG_LEVEL: str = "INFO"


if __name__ == '__main__':
    # Пример использования и тестирования этой конфигурации
    print("--- Тестирование GLMConfig ---")

    # Создаем временный .env файл для теста специфичных переменных GLM
    with open(".env_test_glm", "w") as f:
        f.write("GLM_DB_FILENAME=test_glm.db\\n")
        f.write("GLM_QDRANT_HOST=testhost\\n")
        f.write("GLM_LOG_LEVEL=DEBUG\\n") # Переопределяем базовый LOG_LEVEL
        f.write("GLM_DEFAULT_VECTOR_SIZE=128\\n")

    class TestGLMConfig(GLMConfig):
        model_config = SettingsConfigDict(env_file='.env_test_glm', env_prefix='GLM_')

    glm_config = TestGLMConfig()

    print(f"DB Filename: {glm_config.DB_FILENAME}")
    assert glm_config.DB_FILENAME == "test_glm.db"

    print(f"Qdrant Host: {glm_config.QDRANT_HOST}")
    assert glm_config.QDRANT_HOST == "testhost"

    print(f"Log Level: {glm_config.LOG_LEVEL}")
    assert glm_config.LOG_LEVEL == "DEBUG"

    print(f"Vector Size: {glm_config.DEFAULT_VECTOR_SIZE}")
    assert glm_config.DEFAULT_VECTOR_SIZE == 128

    print(f"GRPC Listen Address (default): {glm_config.GRPC_LISTEN_ADDRESS}")
    assert glm_config.GRPC_LISTEN_ADDRESS == "[::]:50051"

    # Проверка наследованного метода
    print(f"Log Level Int: {glm_config.get_log_level_int()}")
    assert glm_config.get_log_level_int() == logging.DEBUG

    print("--- Тестирование GLMConfig с переменными окружения ---")
    os.environ["GLM_QDRANT_PORT"] = "7777"
    os.environ["GLM_DEFAULT_PAGE_SIZE"] = "99"
    # Для LOG_LEVEL из BaseServiceConfig, Pydantic ищет LOG_LEVEL, а не GLM_LOG_LEVEL, если не переопределен префикс для него.
    # Но так как LOG_LEVEL есть в BaseServiceConfig, и GLMConfig его наследует,
    # он будет прочитан как LOG_LEVEL, если GLM_LOG_LEVEL не установлен и нет env_prefix для BaseServiceConfig.
    # В BaseServiceConfig нет env_prefix, так что он ищет LOG_LEVEL.
    # В GLMConfig есть env_prefix='GLM_', так что для полей GLMConfig он ищет GLM_FIELD_NAME.
    # Если LOG_LEVEL определен в GLMConfig, он будет искать GLM_LOG_LEVEL.
    # Сейчас LOG_LEVEL наследуется, и должен искаться как LOG_LEVEL.
    os.environ["LOG_LEVEL"] = "WARNING" # Тестируем переопределение базового параметра

    # Нужно создать новый экземпляр, чтобы он перечитал переменные окружения
    # или использовать model_post_init или другие механизмы Pydantic для "горячей" перезагрузки (сложнее)
    # Проще создать новый экземпляр для теста.

    # Чтобы Pydantic перечитал переменные окружения для полей BaseServiceConfig без префикса,
    # и для полей GLMConfig с префиксом GLM_, нужно правильно настроить model_config.
    # BaseServiceConfig уже настроен на чтение без префикса.
    # GLMConfig наследует это, но добавляет свой префикс для *новых* полей или тех, что он переопределяет.
    # Поля, унаследованные от BaseServiceConfig, будут по-прежнему использовать конфигурацию BaseServiceConfig.

    # Создаем новый экземпляр GLMConfig без указания тестового env_file, чтобы он читал из окружения
    config_from_env = GLMConfig()
    print(f"Qdrant Port (from env): {config_from_env.QDRANT_PORT}")
    assert config_from_env.QDRANT_PORT == 7777

    print(f"Default Page Size (from env): {config_from_env.DEFAULT_PAGE_SIZE}")
    assert config_from_env.DEFAULT_PAGE_SIZE == 99

    print(f"Log Level (from env for Base): {config_from_env.LOG_LEVEL}")
    assert config_from_env.LOG_LEVEL == "WARNING" # Прочитает LOG_LEVEL из окружения

    del os.environ["GLM_QDRANT_PORT"]
    del os.environ["GLM_DEFAULT_PAGE_SIZE"]
    del os.environ["LOG_LEVEL"]
    try:
        os.remove(".env_test_glm")
    except OSError:
        pass

    print("--- Тестирование GLMConfig завершено ---")
