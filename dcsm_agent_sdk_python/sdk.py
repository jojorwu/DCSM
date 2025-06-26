# dcsm_agent_sdk_python/sdk.py
from .glm_client import GLMClient
from .swm_client import SWMClient # <--- Новый импорт
from .local_memory import LocalAgentMemory
import typing

import contextlib # <--- Новый импорт для contextmanager

# Импортируем KEMQuery из общего места, если нужно его создавать в AgentSDK
from .generated_grpc_code import glm_service_pb2 as common_glm_pb2 # Для KEMQuery
from .generated_grpc_code import swm_service_pb2 as common_swm_pb2 # Для SubscriptionTopic, SWMMemoryEvent

class AgentSDK:
    def __init__(self,
                 glm_server_address: str = 'localhost:50051',
                 swm_server_address: str = 'localhost:50053', # <--- Новый параметр
                 lpa_max_size: int = 100,
                 lpa_indexed_keys: typing.List[str] = None,
                 connect_on_init: bool = True): # <--- Новый параметр для управления подключением при инициализации
        print("AgentSDK: Инициализация...")
        self.glm_client = GLMClient(server_address=glm_server_address)
        self.swm_client = SWMClient(server_address=swm_server_address) # <--- Инициализация SWMClient
        self.local_memory = LocalAgentMemory(max_size=lpa_max_size, indexed_keys=lpa_indexed_keys if lpa_indexed_keys else [])

        if connect_on_init:
            try:
                self.glm_client.connect()
            except Exception as e_glm:
                print(f"AgentSDK: Предупреждение - не удалось подключиться к GLM при инициализации: {e_glm}")
            try:
                self.swm_client.connect()
            except Exception as e_swm:
                print(f"AgentSDK: Предупреждение - не удалось подключиться к SWM при инициализации: {e_swm}")

        print(f"AgentSDK: GLMClient, SWMClient и LocalAgentMemory (indexed_keys: {lpa_indexed_keys if lpa_indexed_keys else []}) инициализированы.")

    def query_local_memory(self, metadata_filters: typing.Optional[dict] = None, ids: typing.Optional[list[str]] = None) -> list[dict]:
        """ Запрашивает КЕП из локальной памяти (ЛПА) с возможностью фильтрации. """
        # print(f"AgentSDK: Запрос query_local_memory с metadata_filters={metadata_filters}, ids={ids}")
        # Предполагаем, что LocalAgentMemory теперь имеет метод query
        return self.local_memory.query(metadata_filters=metadata_filters, ids=ids)

    def get_kem(self, kem_id: str, force_remote: bool = False) -> typing.Optional[dict]:
        print("AgentSDK: Запрос get_kem для ID '{}', force_remote={}".format(kem_id, force_remote))
        if not force_remote:
            cached_kem = self.local_memory.get(kem_id)
            if cached_kem:
                print("AgentSDK: КЕП ID '{}' найдена в ЛПА.".format(kem_id))
                return cached_kem

        print("AgentSDK: КЕП ID '{}' не найдена в ЛПА или запрошен принудительный удаленный вызов. Обращение к GLM...".format(kem_id))
        # GLMClient.retrieve_kems ожидает фильтры, для получения по ID можно использовать metadata_filters
        # или добавить специальный метод get_kem_by_id в GLMClient, если GLM сервис его поддерживает.
        # Используем ids_filter для получения КЕП по ID.
        # GLMClient.retrieve_kems теперь возвращает (list[dict] | None, str | None)
        remote_kems_tuple = self.glm_client.retrieve_kems(ids_filter=[kem_id], page_size=1)

        kems_list_candidate = None
        if remote_kems_tuple and remote_kems_tuple[0] is not None:
            kems_list_candidate = remote_kems_tuple[0]

        if isinstance(kems_list_candidate, list) and len(kems_list_candidate) > 0:
            remote_kem = kems_list_candidate[0]
            if isinstance(remote_kem, dict) and remote_kem.get('id') == kem_id:
                self.local_memory.put(kem_id, remote_kem)
                return remote_kem

        # Если мы здесь, значит, КЕП не найдена или ответ некорректен
        # print(f"AgentSDK: КЕП ID '{kem_id}' не найдена в GLM или получен некорректный ответ от GLM (ожидался список КЕП).") # Этот print уже был, но закомментирован
        return None

    def store_kems(self, kems_data: list[dict]) -> tuple[list[dict] | None, list[str] | None, str | None]:
        """Сохраняет пакет КЕП в GLM и обновляет ЛПА актуальными данными с сервера."""
        print("AgentSDK: Запрос store_kems для {} КЕП.".format(len(kems_data)))
        # glm_client.batch_store_kems возвращает (successfully_stored_kems_as_dicts, failed_kem_references, overall_error_message)
        stored_kems_dicts, failed_refs, error_msg = self.glm_client.batch_store_kems(kems_data)

        if stored_kems_dicts:
            print(f"AgentSDK: Успешно сохранено/обновлено {len(stored_kems_dicts)} КЕП в GLM. Обновление ЛПА.")
            for kem_dict in stored_kems_dicts:
                if kem_dict and 'id' in kem_dict: # Убедимся, что есть ID
                    self.local_memory.put(kem_dict['id'], kem_dict)
            # Возвращаем список сохраненных КЕП (dict), список ссылок на неудавшиеся и общее сообщение об ошибке
            return stored_kems_dicts, failed_refs, error_msg
        else:
            print(f"AgentSDK: Не удалось сохранить КЕПы в GLM. Ошибка: {error_msg}")
            return None, failed_refs, error_msg


    def update_kem(self, kem_id: str, kem_data_update: dict) -> typing.Optional[dict]:
        print("AgentSDK: Запрос update_kem для ID '{}'.".format(kem_id))
        updated_kem_on_server = self.glm_client.update_kem(kem_id, kem_data_update)
        if updated_kem_on_server:
            print("AgentSDK: КЕП ID '{}' успешно обновлена в GLM. Обновление ЛПА.".format(kem_id))
            self.local_memory.put(kem_id, updated_kem_on_server) # Кэшируем то, что вернул сервер
        else:
            print("AgentSDK: Не удалось обновить КЕП ID '{}' в GLM.".format(kem_id))
        return updated_kem_on_server

    def delete_kem(self, kem_id: str) -> bool:
        print("AgentSDK: Запрос delete_kem для ID '{}'.".format(kem_id))
        success = self.glm_client.delete_kem(kem_id)
        if success:
            print("AgentSDK: КЕП ID '{}' успешно удалена из GLM. Удаление из ЛПА.".format(kem_id))
            self.local_memory.delete(kem_id)
        else:
            print("AgentSDK: Не удалось удалить КЕП ID '{}' из GLM.".format(kem_id))
        return success

    def close(self):
        print("AgentSDK: Закрытие соединений...")
        if self.glm_client:
            self.glm_client.close()
        if self.swm_client: # <--- Закрываем SWMClient
            self.swm_client.close()
        print("AgentSDK: Соединения закрыты.")

    def __enter__(self):
        # Соединяемся с GLM, если еще не подключены или не было попытки при init
        if self.glm_client and not self.glm_client.stub:
            try:
                self.glm_client.connect()
            except Exception as e_glm:
                 print(f"AgentSDK (__enter__): Предупреждение - не удалось подключиться к GLM: {e_glm}")

        # Соединяемся с SWM, если еще не подключены или не было попытки при init
        if self.swm_client and not self.swm_client.stub:
            try:
                self.swm_client.connect()
            except Exception as e_swm:
                print(f"AgentSDK (__enter__): Предупреждение - не удалось подключиться к SWM: {e_swm}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # --- SWM Related High-Level Methods ---

    def publish_kems_to_swm_batch(self, kems_data: list[dict], persist_to_glm: bool = False) -> Tuple[List[dict], List[dict]]:
        """
        Публикует пакет КЕП в SWM.
        Возвращает кортеж: (список успешно опубликованных результатов, список КЕП, которые не удалось опубликовать).
        Каждый элемент в списке успешных результатов - это словарь, возвращаемый SWMClient.publish_kem_to_swm.
        Каждый элемент в списке неуспешных - это исходный dict КЕП, который не удалось отправить.
        """
        if not self.swm_client:
            print("AgentSDK: SWMClient не инициализирован.")
            return [], kems_data # Все неуспешные, если клиента нет

        successfully_published_results = []
        failed_to_publish_kems = []

        for kem_data_item in kems_data:
            try:
                # Убедимся, что swm_client подключен перед вызовом
                self.swm_client._ensure_connected()
                result = self.swm_client.publish_kem_to_swm(kem_data_item, persist_to_glm)
                if result and result.get("published_to_swm"):
                    successfully_published_results.append(result)
                    # Опционально: обновить ЛПА, если КЕП была успешно сохранена и persist_to_glm=True
                    # и если SWM вернул достаточно информации для обновления ЛПА (например, полную КЕП).
                    # Текущий SWMClient.publish_kem_to_swm возвращает только статус.
                    # Если ID есть в результате, и он совпадает с исходным (или новый, если исходного не было),
                    # можно обновить ЛПА исходными данными + ID из ответа.
                    # Это потребует более сложной логики, если ID генерируется сервером.
                    # Пока что ЛПА не обновляем на этом этапе автоматически.
                else:
                    print(f"AgentSDK: Не удалось опубликовать КЕП (ID: {kem_data_item.get('id', 'N/A')}) в SWM. Результат: {result}")
                    failed_to_publish_kems.append(kem_data_item)
            except ConnectionError as e:
                print(f"AgentSDK: Ошибка соединения при публикации КЕП (ID: {kem_data_item.get('id', 'N/A')}) в SWM: {e}")
                failed_to_publish_kems.append(kem_data_item)
            except grpc.RpcError as e:
                print(f"AgentSDK: gRPC ошибка при публикации КЕП (ID: {kem_data_item.get('id', 'N/A')}) в SWM: {e.code()} - {e.details()}")
                failed_to_publish_kems.append(kem_data_item)
            except Exception as e:
                print(f"AgentSDK: Непредвиденная ошибка при публикации КЕП (ID: {kem_data_item.get('id', 'N/A')}) в SWM: {e}")
                failed_to_publish_kems.append(kem_data_item)

        print(f"AgentSDK: Batch publish to SWM: {len(successfully_published_results)} succeeded, {len(failed_to_publish_kems)} failed.")
        return successfully_published_results, failed_to_publish_kems

    def load_kems_to_lpa_from_swm(self, kem_query_dict: dict, max_kems_to_load: int = 0) -> List[dict]:
        """
        Загружает КЕП из SWM в Локальную Память Агента (ЛПА) на основе запроса.

        :param kem_query_dict: Словарь, представляющий KEMQuery (например, {"metadata_filters": {"key": "value"}, "ids": ["id1"]}).
                               AgentSDK преобразует его в proto KEMQuery.
        :param max_kems_to_load: Максимальное количество КЕП для загрузки. Если 0, загружает одну страницу (размер по умолчанию SWM).
                                 Если > 0, будет пытаться загрузить до этого количества, возможно, делая несколько запросов с пагинацией.
        :return: Список загруженных в ЛПА КЕП (в виде словарей).
        """
        if not self.swm_client:
            print("AgentSDK: SWMClient не инициализирован.")
            return []

        # Преобразуем kem_query_dict в proto KEMQuery
        # Это можно сделать через ParseDict или вручную создавая объект KEMQuery
        # Для простоты, создадим его вручную здесь, как в GLMClient
        query_proto = common_glm_pb2.KEMQuery()
        if kem_query_dict.get("text_query"): # SWM не поддерживает, но для общности KEMQuery
            query_proto.text_query = kem_query_dict["text_query"]
        if kem_query_dict.get("embedding_query"): # SWM не поддерживает
            query_proto.embedding_query.extend(kem_query_dict["embedding_query"])
        if kem_query_dict.get("metadata_filters"):
            for key, value in kem_query_dict["metadata_filters"].items():
                query_proto.metadata_filters[key] = str(value)
        if kem_query_dict.get("ids"):
            query_proto.ids.extend(kem_query_dict["ids"])
        # Добавить обработку полей дат, если они есть в kem_query_dict

        loaded_to_lpa_kems = []
        current_page_token = ""
        kems_loaded_count = 0

        # Определяем размер страницы для запроса к SWM
        # Если max_kems_to_load не задан или 0, делаем один запрос с дефолтным page_size SWM
        # Если max_kems_to_load > 0, можем использовать его как page_size или делать несколько запросов
        # Пока что, если max_kems_to_load > 0, используем его как page_size для первого запроса.
        # Для более сложной логики с несколькими страницами до лимита, цикл нужно будет доработать.

        page_size_for_swm = max_kems_to_load if max_kems_to_load > 0 else 0 # 0 - SWM использует свой default

        # Простой вариант: один запрос. Для пагинации до max_kems_to_load нужен цикл.
        # TODO: Реализовать цикл пагинации, если max_kems_to_load требует больше одной страницы.
        # Пока что, если max_kems_to_load > 0, это будет page_size для одного запроса.
        # Если max_kems_to_load = 0, то SWM вернет свою дефолтную страницу.

        keep_fetching = True
        while keep_fetching:
            try:
                self.swm_client._ensure_connected()
                kems_page_list, next_page_token = self.swm_client.query_swm(
                    kem_query=query_proto,
                    page_size=page_size_for_swm if max_kems_to_load > 0 and kems_loaded_count == 0 else 0, # Либо запрашиваем конкретное кол-во, либо дефолт SWM
                    page_token=current_page_token
                )

                if kems_page_list:
                    for kem_dict in kems_page_list:
                        if kem_dict and 'id' in kem_dict:
                            self.local_memory.put(kem_dict['id'], kem_dict)
                            loaded_to_lpa_kems.append(kem_dict)
                            kems_loaded_count += 1
                            if max_kems_to_load > 0 and kems_loaded_count >= max_kems_to_load:
                                keep_fetching = False; break

                if not next_page_token or not kems_page_list: # Нет следующей страницы или текущая пуста
                    keep_fetching = False

                current_page_token = next_page_token if next_page_token else ""

                # Если max_kems_to_load == 0, мы делаем только один запрос
                if max_kems_to_load == 0:
                    keep_fetching = False

            except ConnectionError as e:
                print(f"AgentSDK: Ошибка соединения при загрузке КЕП из SWM в ЛПА: {e}")
                keep_fetching = False # Прекращаем попытки при ошибке соединения
            except grpc.RpcError as e:
                print(f"AgentSDK: gRPC ошибка при загрузке КЕП из SWM в ЛПА: {e.code()} - {e.details()}")
                keep_fetching = False # Прекращаем при gRPC ошибке
            except Exception as e:
                print(f"AgentSDK: Непредвиденная ошибка при загрузке КЕП из SWM в ЛПА: {e}")
                keep_fetching = False # Прекращаем

        print(f"AgentSDK: Загружено {len(loaded_to_lpa_kems)} КЕП из SWM в ЛПА.")
        return loaded_to_lpa_kems

    def handle_swm_events(self,
                          topics: Optional[List[common_swm_pb2.SubscriptionTopic]] = None, # Используем common_swm_pb2
                          event_callback: Optional[typing.Callable[[common_swm_pb2.SWMMemoryEvent], None]] = None,
                          agent_id: str = "default_sdk_agent_handler",
                          auto_update_lpa: bool = False,
                          run_in_background: bool = False) -> Optional[threading.Thread]:
        """
        Подписывается на события SWM и обрабатывает их с помощью callback-функции.
        Может автоматически обновлять ЛПА на основе событий.

        :param topics: Список SubscriptionTopic для фильтрации событий.
        :param event_callback: Функция, которая будет вызвана для каждого полученного события.
        :param agent_id: ID агента для подписки.
        :param auto_update_lpa: Если True, ЛПА будет автоматически обновляться на основе событий
                                KEM_PUBLISHED, KEM_UPDATED, KEM_EVICTED.
        :param run_in_background: Если True, обработка событий будет запущена в отдельном потоке,
                                  и метод вернет объект этого потока. Иначе, метод будет блокирующим.
        :return: Объект threading.Thread, если run_in_background=True, иначе None.
        """
        if not self.swm_client:
            print("AgentSDK: SWMClient не инициализирован. Невозможно подписаться на события.")
            return None

        event_stream_generator = self.swm_client.subscribe_to_swm_events(topics=topics, agent_id=agent_id)

        if not event_stream_generator:
            print(f"AgentSDK: Не удалось получить генератор потока событий от SWMClient для agent_id {agent_id}.")
            return None

        print(f"AgentSDK: Начинается прослушивание событий SWM для agent_id {agent_id}...")

        def _process_events():
            try:
                for event in event_stream_generator:
                    if event_callback:
                        try:
                            event_callback(event)
                        except Exception as cb_exc:
                            print(f"AgentSDK: Ошибка в callback-функции обработки события SWM: {cb_exc}", exc_info=True)

                    if auto_update_lpa and event.HasField("kem_payload") and event.kem_payload.id:
                        kem_payload_dict = self.swm_client._kem_proto_to_dict(event.kem_payload) # Используем конвертер из SWMClient
                        if event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED or \
                           event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_UPDATED: # Используем common_swm_pb2
                            self.local_memory.put(event.kem_payload.id, kem_payload_dict)
                            print(f"AgentSDK (LPA AutoUpdate): КЕП ID '{event.kem_payload.id}' добавлена/обновлена в ЛПА из события {common_swm_pb2.SWMMemoryEvent.EventType.Name(event.event_type)}.")
                        elif event.event_type == common_swm_pb2.SWMMemoryEvent.EventType.KEM_EVICTED: # Используем common_swm_pb2
                            self.local_memory.delete(event.kem_payload.id)
                            print(f"AgentSDK (LPA AutoUpdate): КЕП ID '{event.kem_payload.id}' удалена из ЛПА из события KEM_EVICTED.")
            except Exception as e_proc:
                 print(f"AgentSDK: Ошибка в цикле обработки событий SWM для agent_id {agent_id}: {e_proc}", exc_info=True)
            finally:
                print(f"AgentSDK: Завершено прослушивание событий SWM для agent_id {agent_id}.")

        if run_in_background:
            thread = threading.Thread(target=_process_events, daemon=True)
            thread.start()
            return thread
        else:
            _process_events()
            return None

    # --- Distributed Lock High-Level Methods ---

    def acquire_distributed_lock(self, resource_id: str, agent_id: str,
                                 timeout_ms: int = 0, lease_duration_ms: int = 0,
                                 rpc_timeout: int = 5) -> Optional[common_swm_pb2.AcquireLockResponse]:
        """
        Запрашивает распределенную блокировку на ресурс через SWM.

        :param resource_id: ID ресурса для блокировки.
        :param agent_id: ID агента, запрашивающего блокировку.
        :param timeout_ms: Время ожидания получения блокировки на сервере SWM (в мс).
        :param lease_duration_ms: Запрашиваемое время удержания блокировки (в мс). 0 - до явного освобождения.
        :param rpc_timeout: Таймаут для самого gRPC вызова (в сек).
        :return: AcquireLockResponse protobuf сообщение или None в случае серьезной ошибки SDK.
        """
        if not self.swm_client:
            print("AgentSDK: SWMClient не инициализирован. Невозможно запросить блокировку.")
            return None # Или вернуть синтетический ответ с ошибкой

        # SWMClient.acquire_lock уже обрабатывает ошибки и может вернуть AcquireLockResponse с ERROR статусом.
        return self.swm_client.acquire_lock(resource_id, agent_id, timeout_ms, lease_duration_ms, rpc_timeout)

    def release_distributed_lock(self, resource_id: str, agent_id: str,
                                 lock_id: Optional[str] = None, rpc_timeout: int = 5) -> Optional[common_swm_pb2.ReleaseLockResponse]:
        """
        Освобождает распределенную блокировку на ресурс через SWM.

        :param resource_id: ID заблокированного ресурса.
        :param agent_id: ID агента, освобождающего блокировку (должен быть владельцем).
        :param lock_id: Опциональный ID блокировки (для дополнительной проверки).
        :param rpc_timeout: Таймаут для gRPC вызова (в сек).
        :return: ReleaseLockResponse protobuf сообщение или None.
        """
        if not self.swm_client:
            print("AgentSDK: SWMClient не инициализирован. Невозможно освободить блокировку.")
            return None

        return self.swm_client.release_lock(resource_id, agent_id, lock_id, rpc_timeout)

    def get_distributed_lock_info(self, resource_id: str, rpc_timeout: int = 5) -> Optional[common_swm_pb2.LockInfo]:
        """
        Получает информацию о состоянии распределенной блокировки для ресурса.

        :param resource_id: ID ресурса.
        :param rpc_timeout: Таймаут для gRPC вызова (в сек).
        :return: LockInfo protobuf сообщение или None.
        """
        if not self.swm_client:
            print("AgentSDK: SWMClient не инициализирован. Невозможно получить информацию о блокировке.")
            return None
        return self.swm_client.get_lock_info(resource_id, rpc_timeout)

    @contextlib.contextmanager
    def distributed_lock(self, resource_id: str, agent_id: str,
                         acquire_timeout_ms: int = 10000, # Таймаут на получение блокировки (10 сек)
                         lease_duration_ms: int = 60000,  # Время жизни блокировки (60 сек)
                         rpc_timeout: int = 5):
        """
        Контекстный менеджер для удобной работы с распределенными блокировками.
        Автоматически запрашивает блокировку при входе в блок 'with' и освобождает при выходе.

        Пример:
        with sdk.distributed_lock("my_resource", "agent_x") as lock_response:
            if lock_response and lock_response.status == LockStatusValue.ACQUIRED:
                # ... работа с ресурсом ...
            else:
                # ... блокировка не получена ...

        :param resource_id: ID ресурса.
        :param agent_id: ID агента.
        :param acquire_timeout_ms: Таймаут ожидания получения блокировки на сервере SWM.
        :param lease_duration_ms: Время аренды блокировки.
        :param rpc_timeout: Таймаут для gRPC вызовов.
        :yield: AcquireLockResponse или None.
        """
        acquired_lock_response: Optional[common_swm_pb2.AcquireLockResponse] = None
        acquired_lock_id: Optional[str] = None

        try:
            print(f"AgentSDK (distributed_lock): Попытка захвата блокировки для '{resource_id}' агентом '{agent_id}'...")
            acquired_lock_response = self.acquire_distributed_lock(
                resource_id, agent_id,
                timeout_ms=acquire_timeout_ms,
                lease_duration_ms=lease_duration_ms,
                rpc_timeout=rpc_timeout
            )

            if acquired_lock_response and \
               (acquired_lock_response.status == common_swm_pb2.LockStatusValue.ACQUIRED or \
                acquired_lock_response.status == common_swm_pb2.LockStatusValue.ALREADY_HELD_BY_YOU):
                print(f"AgentSDK (distributed_lock): Блокировка для '{resource_id}' успешно получена/удерживается. Lock ID: {acquired_lock_response.lock_id}")
                acquired_lock_id = acquired_lock_response.lock_id
                yield acquired_lock_response # Передаем ответ в блок 'with'
            else:
                status_name = common_swm_pb2.LockStatusValue.Name(acquired_lock_response.status) if acquired_lock_response else "N/A"
                print(f"AgentSDK (distributed_lock): Не удалось получить блокировку для '{resource_id}'. Статус: {status_name}")
                yield acquired_lock_response # Все равно передаем ответ, чтобы вызывающий код мог проверить статус

        finally:
            if acquired_lock_id and acquired_lock_response and \
               (acquired_lock_response.status == common_swm_pb2.LockStatusValue.ACQUIRED or \
                acquired_lock_response.status == common_swm_pb2.LockStatusValue.ALREADY_HELD_BY_YOU) :
                # Освобождаем только если мы действительно ее получили (не ALREADY_HELD от другого вызова, а именно этот)
                # Для ALREADY_HELD_BY_YOU, если lock_id совпадает, тоже можно освобождать.
                # Проще всего ориентироваться на то, что acquired_lock_id был установлен.
                print(f"AgentSDK (distributed_lock): Освобождение блокировки для '{resource_id}', Lock ID: {acquired_lock_id} агентом '{agent_id}'.")
                # Передаем ID блокировки, если он есть, для более точного освобождения
                self.release_distributed_lock(resource_id, agent_id, lock_id=acquired_lock_id, rpc_timeout=rpc_timeout)

    # --- Distributed Counter High-Level Methods ---

    def increment_distributed_counter(self, counter_id: str, increment_by: int = 1, rpc_timeout: int = 5) -> Optional[int]:
        """
        Инкрементирует (или декрементирует) распределенный счетчик в SWM.

        :param counter_id: ID счетчика.
        :param increment_by: Значение, на которое изменить счетчик.
        :param rpc_timeout: Таймаут для gRPC вызова.
        :return: Новое значение счетчика или None в случае ошибки.
        """
        if not self.swm_client:
            print("AgentSDK: SWMClient не инициализирован. Невозможно инкрементировать счетчик.")
            return None

        response = self.swm_client.increment_counter(counter_id, increment_by, rpc_timeout)
        if response and (not response.status_message or "успешно" in response.status_message.lower() or not "error" in response.status_message.lower()): # Простая проверка на успех
            return response.current_value
        else:
            print(f"AgentSDK: Ошибка при инкрементировании счетчика '{counter_id}'. Сообщение: {response.status_message if response else 'Нет ответа'}")
            return None # Или можно вернуть предыдущее значение, если оно известно, или специальное значение ошибки

    def get_distributed_counter(self, counter_id: str, rpc_timeout: int = 5) -> Optional[int]:
        """
        Получает текущее значение распределенного счетчика из SWM.

        :param counter_id: ID счетчика.
        :param rpc_timeout: Таймаут для gRPC вызова.
        :return: Текущее значение счетчика или None в случае ошибки (или если счетчик не существует и сервер вернул ошибку).
                 Если счетчик не существует и сервер возвращает 0 + сообщение, то вернется 0.
        """
        if not self.swm_client:
            print("AgentSDK: SWMClient не инициализирован. Невозможно получить значение счетчика.")
            return None

        response = self.swm_client.get_counter(counter_id, rpc_timeout)
        if response and (not response.status_message or "успешно" in response.status_message.lower() or "не найден" in response.status_message.lower() or not "error" in response.status_message.lower()): # Простая проверка на успех или "не найден"
             # Если "не найден", сервер SWM возвращает 0 и сообщение. Это валидный сценарий.
            return response.current_value
        else:
            print(f"AgentSDK: Ошибка при получении значения счетчика '{counter_id}'. Сообщение: {response.status_message if response else 'Нет ответа'}")
            return None


if __name__ == '__main__':
    print("Запуск примера использования AgentSDK (требуется запущенный GLM сервер на localhost:50051)")
    print("Этот пример не будет выполнен автоматически при выполнении subtask.")
    print("Для локального теста установите переменную окружения RUN_AGENT_SDK_EXAMPLE=true")

    import os
    if os.getenv("RUN_AGENT_SDK_EXAMPLE") == "true":
        try:
            # Используем контекстный менеджер для SDK
            with AgentSDK() as sdk:
                # Пример: Сохранение КЕП
                kem_to_store1 = {"id": "sdk_kem_main_001", "content_type": "text/plain", "content": "Первая КЕП через SDK (main)", "metadata": {"source":"sdk_example_main", "version":"1"}}
                kem_to_store2 = {"id": "sdk_kem_main_002", "content_type": "text/plain", "content": "Вторая КЕП через SDK (main)", "metadata": {"source":"sdk_example_main", "version":"1"}}

                print("\\n--- Сохранение КЕП ---")
                ids, count, errors = sdk.store_kems([kem_to_store1, kem_to_store2])
                if count and count > 0:
                    print("Успешно сохранено {} КЕП с ID: {}".format(count, ids))
                else:
                    print("Не удалось сохранить КЕП. Ошибки: {}".format(errors))

                # Пример: Получение КЕП
                print("\\n--- Получение sdk_kem_main_001 (сначала из ЛПА, если есть) ---")
                kem = sdk.get_kem("sdk_kem_main_001")
                if kem:
                    print("Получена КЕП sdk_kem_main_001: '{}'".format(kem.get("content")))
                else:
                    print("КЕП sdk_kem_main_001 не найдена.")

                print("\\n--- Принудительное получение sdk_kem_main_001 с сервера ---")
                kem_forced = sdk.get_kem("sdk_kem_main_001", force_remote=True)
                if kem_forced:
                    print("Принудительно получена КЕП sdk_kem_main_001: '{}'".format(kem_forced.get("content")))
                else:
                    print("КЕП sdk_kem_main_001 не найдена на сервере (принудительно).")

                print("\\n--- Получение несуществующей КЕП ---")
                non_existent_kem = sdk.get_kem("non_existent_id_123")
                if non_existent_kem:
                    print("Это не должно было произойти! Получена несуществующая КЕП.")
                else:
                    print("Несуществующая КЕП non_existent_id_123 не найдена, как и ожидалось.")

                print("\\n--- Обновление sdk_kem_main_001 ---")
                if ids and "sdk_kem_main_001" in ids: # Проверяем, что КЕП была сохранена
                    update_data = {"metadata": {"source":"sdk_example_main", "version":"2", "status": "updated"}}
                    updated = sdk.update_kem("sdk_kem_main_001", update_data)
                    if updated:
                        print("КЕП sdk_kem_main_001 обновлена. Новые метаданные: {}".format(updated.get("metadata")))
                        lpa_kem = sdk.local_memory.get("sdk_kem_main_001")
                        print("КЕП sdk_kem_main_001 в ЛПА (метаданные): {}".format(lpa_kem.get("metadata") if lpa_kem else "Не найдена"))
                    else:
                        print("Не удалось обновить sdk_kem_main_001.")
                else:
                    print("Пропуск обновления sdk_kem_main_001, так как она не была успешно сохранена.")


                print("\\n--- Удаление sdk_kem_main_002 ---")
                if ids and "sdk_kem_main_002" in ids: # Проверяем, что КЕП была сохранена
                    deleted = sdk.delete_kem("sdk_kem_main_002")
                    if deleted:
                        print("КЕП sdk_kem_main_002 удалена.")
                        assert sdk.local_memory.get("sdk_kem_main_002") is None, "КЕП sdk_kem_main_002 все еще в ЛПА!"
                        assert sdk.get_kem("sdk_kem_main_002") is None, "КЕП sdk_kem_main_002 все еще доступна через get_kem!"
                    else:
                        print("Не удалось удалить sdk_kem_main_002.")
                else:
                    print("Пропуск удаления sdk_kem_main_002, так как она не была успешно сохранена.")

            print("\nAgentSDK пример завершен.")
        except Exception as e:
            print("Произошла ошибка во время выполнения примера AgentSDK: {}".format(e))
            import traceback
            traceback.print_exc()
    else:
        print("Переменная окружения RUN_AGENT_SDK_EXAMPLE не установлена в 'true', пример не выполняется.")
