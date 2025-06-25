from pydantic_settings import BaseSettings, SettingsConfigDict
import logging

class BaseServiceConfig(BaseSettings):
    # Pydantic V2 использует model_config как словарь, а не класс Config
    model_config = SettingsConfigDict(
        env_file='.env',              # Опционально: имя .env файла для загрузки
        env_file_encoding='utf-8',
        extra='ignore',               # Игнорировать лишние переменные окружения
        case_sensitive=False          # Имена переменных окружения не чувствительны к регистру
                                      # (но Pydantic поля чувствительны)
    )

    LOG_LEVEL: str = "INFO" # Уровень логирования по умолчанию

    # Можно добавить другие общие параметры здесь, если они появятся

    def get_log_level_int(self) -> int:
        """Возвращает числовое значение уровня логирования."""
        level = self.LOG_LEVEL.upper()
        if level == "DEBUG":
            return logging.DEBUG
        if level == "INFO":
            return logging.INFO
        if level == "WARNING":
            return logging.WARNING
        if level == "ERROR":
            return logging.ERROR
        if level == "CRITICAL":
            return logging.CRITICAL
        return logging.INFO # По умолчанию INFO

if __name__ == '__main__':
    # Пример использования (требует установки pydantic-settings: pip install pydantic-settings)
    # Для теста создадим временный .env файл
    with open(".env_test_common_config", "w") as f:
        f.write("LOG_LEVEL=DEBUG\\n")

    class TestConfig(BaseServiceConfig):
        model_config = SettingsConfigDict(env_file='.env_test_common_config', extra='ignore')

    config = TestConfig()
    print(f"Log Level from env: {config.LOG_LEVEL}") # Должно быть DEBUG
    print(f"Log Level int: {config.get_log_level_int()}")

    # Убираем временный .env файл
    import os
    try:
        os.remove(".env_test_common_config")
    except OSError:
        pass

    # Пример без .env файла (использует значение по умолчанию)
    config_default = BaseServiceConfig()
    print(f"Default Log Level: {config_default.LOG_LEVEL}") # Должно быть INFO
    print(f"Default Log Level int: {config_default.get_log_level_int()}")

    # Пример с переопределением через переменную окружения
    # (для этого нужно было бы установить ее перед запуском скрипта)
    # os.environ["LOG_LEVEL"] = "WARNING"
    # config_env_override = BaseServiceConfig()
    # print(f"Overridden Log Level: {config_env_override.LOG_LEVEL}")
    # del os.environ["LOG_LEVEL"]
