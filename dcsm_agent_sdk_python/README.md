# DCSM Python Agent SDK

Это Python SDK для взаимодействия с сервисами Динамической Контекстуализированной Общей Памяти (ДКОП), такими как GLM (Global Long-Term Memory).
SDK предоставляет клиентские классы и утилиты для упрощения разработки агентов.

## Особенности

*   Клиент для GLM-сервиса (`GLMClient`) для операций с КЕП.
*   Локальная Память Агента (`LocalAgentMemory`) с LRU-кэшированием.
*   Основной класс `AgentSDK` для удобного доступа к функциям.

## Установка

1.  **Клонируйте репозиторий SDK** (или скопируйте директорию `dcsm_agent_sdk_python` в ваш проект).

2.  **Установите зависимости:**
    Перейдите в директорию `dcsm_agent_sdk_python` и выполните:
    ```bash
    pip install -r requirements.txt
    ```
    Это установит `grpcio`, `grpcio-tools`, `protobuf`, `cachetools`.

3.  **Сгенерируйте gRPC код (если необходимо):**
    SDK поставляется с уже сгенерированным gRPC кодом в директории `generated_grpc_code/`. Однако, если вы изменяли `.proto` файлы в директории `protos/` или хотите перегенерировать код, выполните скрипт из корневой директории `dcsm_agent_sdk_python`:
    ```bash
    ./generate_grpc_code.sh
    ```
    *Убедитесь, что скрипт имеет права на выполнение (`chmod +x generate_grpc_code.sh`).*

## Использование

Основной класс для использования - `AgentSDK`.

```python
# Пример импорта, если dcsm_agent_sdk_python находится в PYTHONPATH
# или установлен как пакет
from dcsm_agent_sdk_python.sdk import AgentSDK
# Если запускаете из dcsm_agent_sdk_python и example.py там же:
# from sdk import AgentSDK


# Предполагается, что GLM сервер запущен на localhost:50051
# Используйте контекстный менеджер для автоматического закрытия соединений
# sdk = AgentSDK(glm_server_address='localhost:50051', lpa_max_size=200)
# try:
#     sdk.glm_client.connect() # Подключаемся явно или при первом вызове
#     # Ваш код здесь
# finally:
#     sdk.close()
#
# Или с with:
# with AgentSDK(glm_server_address='localhost:50051', lpa_max_size=200) as sdk:
#    # Сохранение КЕП
#    kem_to_store = {
#        "id": "example_kem_001",
#        "content_type": "text/plain",
#        "content": "Это тестовая КЕП, сохраненная через SDK.",
#        "metadata": {"source": "sdk_readme_example", "language": "python"}
#    }
#    stored_ids, count, errors = sdk.store_kems([kem_to_store])
#    if count and count > 0 and stored_ids:
#        print(f"Успешно сохранена КЕП с ID: {stored_ids[0]}")
#
#        # Получение КЕП (сначала из ЛПА, потом с сервера)
#        retrieved_kem = sdk.get_kem(stored_ids[0])
#        if retrieved_kem:
#            print(f"Получена КЕП ID {retrieved_kem.get('id')}: {retrieved_kem.get('content')}")
#            print(f"Метаданные: {retrieved_kem.get('metadata')}")
#
#        # Обновление КЕП
#        update_payload = {"metadata": {"status": "reviewed", "language": "python_updated"}}
#        updated_kem = sdk.update_kem(stored_ids[0], update_payload)
#        if updated_kem:
#            print(f"КЕП ID {updated_kem.get('id')} обновлена. Новые метаданные: {updated_kem.get('metadata')}")
#            # Проверка, что ЛПА также обновилась
#            lpa_version = sdk.local_memory.get(stored_ids[0])
#            print(f"Метаданные из ЛПА после обновления: {lpa_version.get('metadata') if lpa_version else 'Не найдено в ЛПА'}")
#
#        # Удаление КЕП
#        # deleted = sdk.delete_kem(stored_ids[0])
#        # if deleted:
#        #     print(f"КЕП ID {stored_ids[0]} удалена.")
#        #     assert sdk.get_kem(stored_ids[0]) is None # Проверка, что КЕП действительно удалена
#    else:
#        print(f"Не удалось сохранить КЕП. Ошибки: {errors if errors else 'Нет информации об ошибках'}")

```

Смотрите `example.py` для более подробного примера использования.

## Запуск GLM Сервера (для тестирования SDK)

Для полноценного тестирования SDK требуется запущенный экземпляр GLM-сервиса.
Если вы создавали `glm_python_service` ранее, вы можете запустить его:
1.  Перейдите в директорию `glm_python_service`.
2.  Активируйте виртуальное окружение, если есть.
3.  Установите зависимости: `pip install -r requirements.txt`.
4.  Запустите сервер: `python app/main.py` (или `python -m app.main`).
Он должен запуститься на `localhost:50051`.
