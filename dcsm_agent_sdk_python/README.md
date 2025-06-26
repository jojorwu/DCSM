# DCSM Python Agent SDK

Это Python SDK для взаимодействия с сервисами Динамической Контекстуализированной Общей Памяти (ДКОП), такими как GLM (Global Long-Term Memory) и SWM (Shared Working Memory).
SDK предоставляет клиентские классы и утилиты для упрощения разработки агентов, включая поддержку многоагентных сценариев.

## Особенности

*   **Клиент для GLM-сервиса (`GLMClient`)**: Позволяет сохранять, извлекать, обновлять и удалять Контекстуализированные Единицы Памяти (КЕП) в долгосрочном хранилище. Включает retry-логику.
*   **Клиент для SWM-сервиса (`SWMClient`)**: Обеспечивает взаимодействие с общей рабочей памятью, включая:
    *   Публикацию КЕП в SWM.
    *   Запросы КЕП из кэша SWM.
    *   Загрузку КЕП из GLM в SWM.
    *   Подписку на события изменения КЕП в SWM.
    *   Работу с распределенными блокировками.
    *   Работу с распределенными счетчиками.
    *   Также включает retry-логику.
*   **Локальная Память Агента (`LocalAgentMemory`)**: LRU-кэш на стороне клиента с возможностью индексации по метаданным для быстрого локального доступа к часто используемым КЕП.
*   **Основной класс `AgentSDK`**: Высокоуровневый интерфейс, который объединяет возможности `GLMClient`, `SWMClient` и `LocalAgentMemory`, предоставляя удобные методы для:
    *   Управления КЕП в GLM и ЛПА.
    *   Пакетных операций с SWM (публикация, загрузка в ЛПА).
    *   Обработки событий SWM с автоматическим обновлением ЛПА.
    *   Работы с распределенными блокировками (включая контекстный менеджер).
    *   Работы с распределенными счетчиками.

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
# Для корректной работы с типами из proto при использовании методов SDK, связанных с SWM:
from dcsm_agent_sdk_python.generated_grpc_code import swm_service_pb2 # для LockStatusValue, SubscriptionTopic и т.д.
# from sdk import AgentSDK # Если запускаете из dcsm_agent_sdk_python и example.py там же

# Инициализация SDK (предполагается, что GLM и SWM серверы запущены на стандартных портах)
# sdk = AgentSDK(
#     glm_server_address='localhost:50051',
#     swm_server_address='localhost:50053',
#     lpa_max_size=200, # Размер Локальной Памяти Агента (ЛПА)
#     lpa_indexed_keys=['topic', 'source'] # Пример индексируемых ключей в ЛПА
# )

# Используйте контекстный менеджер для автоматического управления соединениями:
# with AgentSDK(glm_server_address='localhost:50051', swm_server_address='localhost:50053') as sdk:
#     # Ваш код работы с SDK здесь
#     pass


# Пример работы с GLM (подробности см. в sdk.AgentSDK.get_kem, store_kems и т.д.)
# with AgentSDK(...) as sdk:
#    # Сохранение КЕП в GLM
#    kem_to_store_glm = {
#        "id": "example_glm_kem_001", "content_type": "text/plain",
#        "content": "Это КЕП для GLM.", "metadata": {"source": "readme_glm"}
#    }
#    stored_kems, _, _ = sdk.store_kems([kem_to_store_glm])
#    if stored_kems:
#        print(f"КЕП '{stored_kems[0]['id']}' сохранена в GLM.")
#
#        # Получение КЕП из GLM (сначала проверяется ЛПА)
#        retrieved_kem = sdk.get_kem(stored_kems[0]['id'])
#        if retrieved_kem:
#            print(f"Получена КЕП: {retrieved_kem['content']}")

### Взаимодействие с SWM (Общая Рабочая Память)

# with AgentSDK(...) as sdk:
#     # 1. Пакетная публикация КЕП в SWM (без сохранения в GLM по умолчанию)
#     kems_for_swm = [
#         {"id": "swm_kem_1", "content": "Данные для SWM 1", "metadata": {"group": "alpha"}},
#         {"id": "swm_kem_2", "content": "Данные для SWM 2", "metadata": {"group": "alpha"}}
#     ]
#     successful_pubs, failed_pubs = sdk.publish_kems_to_swm_batch(kems_for_swm, persist_to_glm=False)
#     print(f"Опубликовано в SWM: {len(successful_pubs)}, не удалось: {len(failed_pubs)}")

#     # 2. Загрузка КЕП из SWM в ЛПА по метаданным
#     query_dict_swm = {"metadata_filters": {"group": "alpha"}}
#     loaded_to_lpa = sdk.load_kems_to_lpa_from_swm(query_dict_swm, max_kems_to_load=5)
#     print(f"Загружено в ЛПА из SWM: {len(loaded_to_lpa)} КЕП.")
#     for kem_in_lpa in loaded_to_lpa:
#         print(f"  ЛПА содержит: {kem_in_lpa['id']} - {kem_in_lpa['content']}")

### Подписка на события SWM

# def my_event_handler(event: swm_service_pb2.SWMMemoryEvent):
#     print(f"SDK_README: Получено событие SWM! Тип: {swm_service_pb2.SWMMemoryEvent.EventType.Name(event.event_type)}")
#     if event.HasField("kem_payload"):
#         print(f"  Для КЕП ID: {event.kem_payload.id}, Контент: {event.kem_payload.content.decode('utf-8')[:50]}...")

# with AgentSDK(...) as sdk:
#     # Подписка на все события KEM_LIFECYCLE_EVENTS для КЕП с metadata.group = "alpha"
#     topic = swm_service_pb2.SubscriptionTopic(
#         type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
#         filter_criteria="metadata.group=alpha"
#     )
#     # Запуск обработчика в фоновом потоке, с автообновлением ЛПА
#     event_thread = sdk.handle_swm_events(
#         topics=[topic],
#         event_callback=my_event_handler,
#         auto_update_lpa=True,
#         run_in_background=True
#     )
#     print("Подписка на события SWM активна в фоновом режиме...")
#     # ... (здесь агент может выполнять другую работу) ...
#     # Для остановки (если нужно явно, хотя поток демон): event_thread.join() или остановить SWMClient/SDK.

### Распределенные блокировки через SWM

# agent_id_for_lock = "my_agent_id_123"
# resource_name = "shared_resource_xyz"

# with AgentSDK(...) as sdk:
#     print(f"Пытаюсь получить блокировку на '{resource_name}'...")
#     with sdk.distributed_lock(resource_name, agent_id_for_lock, acquire_timeout_ms=5000, lease_duration_ms=30000) as lock_resp:
#         if lock_resp and lock_resp.status == swm_service_pb2.LockStatusValue.ACQUIRED:
#             print(f"Блокировка '{resource_name}' получена! Lock ID: {lock_resp.lock_id}. Работа с ресурсом...")
#             # ... (критическая секция, работа с общим ресурсом) ...
#             print(f"Работа с ресурсом '{resource_name}' завершена.")
#         elif lock_resp and lock_resp.status == swm_service_pb2.LockStatusValue.ALREADY_HELD_BY_YOU:
#             print(f"Блокировка '{resource_name}' уже удерживается мной (агентом {agent_id_for_lock}).")
#         else:
#             status_name = swm_service_pb2.LockStatusValue.Name(lock_resp.status) if lock_resp else "N/A"
#             print(f"Не удалось получить блокировку на '{resource_name}'. Статус: {status_name}")
#     print(f"Блокировка '{resource_name}' должна быть автоматически освобождена.")

### Распределенные счетчики через SWM

# counter_name = "global_event_counter"
# with AgentSDK(...) as sdk:
#     # Инкремент счетчика
#     new_value = sdk.increment_distributed_counter(counter_name, increment_by=1)
#     if new_value is not None:
#         print(f"Счетчик '{counter_name}' инкрементирован. Новое значение: {new_value}")

#     # Получение значения счетчика
#     current_value = sdk.get_distributed_counter(counter_name)
#     if current_value is not None:
#         print(f"Текущее значение счетчика '{counter_name}': {current_value}")

#     # Инкремент на -5 (декремент)
#     new_value_decr = sdk.increment_distributed_counter(counter_name, increment_by=-5)
#     if new_value_decr is not None:
#         print(f"Счетчик '{counter_name}' декрементирован на 5. Новое значение: {new_value_decr}")


# try:
#     sdk.glm_client.connect() # Подключаемся явно или при первом вызове
#     # sdk.swm_client.connect() # Также для SWM
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

Смотрите `example.py` для более подробного примера использования всех этих функций.

## Запуск Сервисов (для тестирования SDK)

Для полноценного тестирования SDK требуются запущенные экземпляры сервисов ДКОП (как минимум GLM и SWM).
Рекомендуемый способ запуска всех сервисов – через `docker-compose` из корневой директории проекта ДКОП:

```bash
# Из корневой директории проекта
docker-compose up --build
```

Это запустит:
*   GLM сервис (обычно на `localhost:50051`)
*   SWM сервис (обычно на `localhost:50053`)
*   KPS сервис (обычно на `localhost:50052`)
*   Qdrant (векторная база данных)

Убедитесь, что адреса и порты в вашем `AgentSDK` соответствуют конфигурации запущенных сервисов.
