# SWM (Shared Working Memory) Service - (dcs_memory/services/swm)

Это gRPC сервис для Общей Рабочей Памяти (SWM) в рамках проекта Динамической Контекстуализированной Общей Памяти (ДКОП).
SWM выступает как активный кэширующий слой для Контекстуализированных Единиц Памяти (КЕП), с которым взаимодействуют агенты. SWM может загружать КЕПы из GLM (Global Long-Term Memory), хранить их в своем кэше и предоставлять агентам через запросы и подписки.

Сервис использует централизованные определения `.proto` файлов (`kem.proto`, `glm_service.proto`, `swm_service.proto`), расположенные в `dcs_memory/common/grpc_protos/`.

## Основные Функции

*   **Кэширование КЕП:** Использует Redis для хранения активных КЕП с политикой вытеснения LRU (управляемой Redis) для быстрого доступа.
*   **Взаимодействие с GLM:** Может загружать КЕП из GLM по запросу и опционально сохранять/обновлять КЕП в GLM (с использованием механизма отложенной пакетной персистенции).
*   **Публикация КЕП:** Агенты могут публиковать КЕП в SWM.
*   **Запрос КЕП:** Агенты могут запрашивать КЕП из кэша SWM (Redis) с поддержкой фильтрации по ID, индексируемым метаданным и датам.
*   **Подписка на события:** Предоставляет стрим событий SWM (`KEM_PUBLISHED`, `KEM_UPDATED`, `KEM_EVICTED`), с фильтрацией по ID, метаданным и типу события. События `KEM_EVICTED` генерируются на основе Redis Keyspace Notifications.

## Зависимости

*   Python 3.8+
*   Pip
*   **GLM Service**: Должен быть запущен и доступен для SWM.
*   **Redis Server**: Должен быть запущен, доступен и сконфигурирован (см. Настройка).

## Настройка и Запуск

1.  **Клонируйте репозиторий** (если вы еще этого не сделали).

2.  **Убедитесь, что GLM сервис запущен** и доступен по адресу, указанному в конфигурации SWM (см. Переменные окружения). GLM, в свою очередь, требует доступный Qdrant и SQLite.

3.  **Перейдите в директорию SWM сервиса:**
    ```bash
    cd dcs_memory/services/swm
    ```

4.  **Создайте и активируйте виртуальное окружение (рекомендуется):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

5.  **Установите зависимости Python:**
    ```bash
    pip install -r requirements.txt
    ```
    (Включает `grpcio`, `aioredis`, `msgpack` и т.д.)

6.  **Настройте ваш Redis сервер:**
    *   Убедитесь, что Redis запущен и доступен для SWM.
    *   В конфигурации Redis (`redis.conf`) установите:
        *   `maxmemory <нужный_размер>` (например, `maxmemory 2gb`) для ограничения памяти кэша.
        *   `maxmemory-policy allkeys-lru` для использования LRU политики вытеснения.
        *   `notify-keyspace-events Ex` (или `KEA`) для включения уведомлений о вытеснении/истечении срока действия ключей (необходимо для событий `KEM_EVICTED`). Изменения в `redis.conf` требуют перезапуска Redis.

7.  **Сгенерируйте gRPC код (если вы изменяли `.proto` файлы):**
    Скрипт `generate_grpc_code.sh` в текущей директории (`dcs_memory/services/swm/`) отвечает за генерацию Python кода в локальную директорию `generated_grpc/`.
    ```bash
    ./generate_grpc_code.sh
    ```
    *Убедитесь, что скрипт имеет права на выполнение (`chmod +x generate_grpc_code.sh`).*

7.  **Настройте переменные окружения (обязательно для GLM_SERVICE_ADDRESS, опционально для остального):**
    *   `GLM_SERVICE_ADDRESS`: Адрес запущенного GLM сервиса (например, `localhost:50051`). **Обязательно.**
    *   `SWM_GRPC_LISTEN_ADDRESS`: Адрес, на котором SWM будет слушать (по умолчанию: `[::]:50053`).
    *   `SWM_REDIS_HOST`: Хост Redis сервера (по умолчанию: `localhost`).
    *   `SWM_REDIS_PORT`: Порт Redis сервера (по умолчанию: `6379`).
    *   `SWM_REDIS_DB`: Номер базы данных Redis для SWM (по умолчанию: `0`).
    *   `SWM_REDIS_PASSWORD`: Пароль для Redis (если используется, по умолчанию: нет).
    *   `SWM_INDEXED_METADATA_KEYS`: Список ключей метаданных через запятую для создания вторичных индексов в Redis (например, `type,status`).
    *   `DEFAULT_SWM_PAGE_SIZE`: Размер страницы по умолчанию для `QuerySWM` (по умолчанию: `20`).
    *   Другие параметры, такие как `SWM_SUBSCRIBER_*_QUEUE_SIZE`, `SWM_GLM_PERSISTENCE_*` также могут быть настроены (см. `app/config.py`).

9.  **Запустите gRPC сервер SWM:**
    Из директории `dcs_memory/services/swm/`:
    ```bash
    python -m app.main
    ```
    Сервер попытается подключиться к GLM. Логи будут выводиться в консоль.

## API (gRPC)

Сервис реализует gRPC интерфейс, определенный в `swm_service.proto` (находится в `dcs_memory/common/grpc_protos/`).

Основные методы:
*   **`PublishKEMToSWM(PublishKEMToSWMRequest) returns (PublishKEMToSWMResponse)`**:
    *   Публикует КЕП в кэш SWM.
    *   Если `persist_to_glm_if_new_or_updated` = true, SWM попытается сохранить/обновить КЕП в GLM.
    *   Возвращает ID КЕП, статус публикации в SWM и статус попытки сохранения в GLM.
*   **`SubscribeToSWMEvents(SubscribeToSWMEventsRequest) returns (stream SWMMemoryEvent)`**:
    *   Позволяет агенту подписаться на события в SWM (например, `KEM_PUBLISHED`, `KEM_UPDATED`, `KEM_EVICTED`).
    *   Запрос содержит ID агента и список `SubscriptionTopic`. Каждый топик может содержать `filter_criteria` (например, `"kem_id=some_id"`, `"metadata.type=document"`) и `desired_event_types` (список типов событий, на которые нужно подписаться для данного фильтра; если пуст - все типы).
    *   Возвращает поток `SWMMemoryEvent` (`KEM_PUBLISHED`, `KEM_UPDATED`, `KEM_EVICTED`). Событие `KEM_EVICTED` генерируется на основе уведомлений Redis Keyspace Notifications (содержит только ID KEM).
*   **`QuerySWM(QuerySWMRequest) returns (QuerySWMResponse)`**:
    *   Запрашивает КЕП из кэша SWM (Redis), используя `KEMQuery` и пагинацию.
    *   **Поддерживаемые фильтры в `KEMQuery` для SWM кэша (Redis):**
        *   `ids`: Извлечение по списку ID КЕП.
        *   `metadata_filters`: Точное совпадение по ключам метаданных, указанным в `SWM_INDEXED_METADATA_KEYS`. Фильтры по другим ключам метаданных будут применены пост-фактум к результатам, полученным по индексным фильтрам.
        *   Фильтры по датам (`created_at_start/end`, `updated_at_start/end`) используют индексы в Redis.
    *   **Сортировка:** По умолчанию результаты сортируются по `updated_at` в порядке убывания (самые свежие сначала).
    *   **Неподдерживаемые фильтры в `KEMQuery` для SWM кэша:**
        *   `embedding_query` и `text_query`.
    *   Поддерживает пагинацию через `page_size` и `page_token` (offset-based).
    *   **Примечание:** Запросы без эффективных индексных фильтров (ID, индексируемые метаданные, даты) могут вернуть пустой результат или ошибку, так как полное сканирование кэша Redis неэффективно.
*   **`LoadKEMsFromGLM(LoadKEMsFromGLMRequest) returns (LoadKEMsFromGLMResponse)`**:
    *   Запрашивает у SWM загрузку КЕП из GLM в свой кэш (Redis) на основе `KEMQuery`.
    *   Возвращает количество загруженных КЕП и их ID.

Обратитесь к файлам `kem.proto`, `glm_service.proto` и `swm_service.proto` для детального описания всех сообщений и полей.

## Тестирование

Директория `tests/` предназначена для модульных и интеграционных тестов (пока не реализованы).
Для ручного тестирования:
1.  Убедитесь, что GLM сервис запущен.
2.  Запустите SWM сервис.
3.  Используйте gRPC-клиент для вызова методов SWM.
