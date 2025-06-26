import grpc
import logging
from typing import Optional, List, Tuple

from .generated_grpc_code import kem_pb2
from .generated_grpc_code import glm_service_pb2 # KEMQuery
from .generated_grpc_code import swm_service_pb2
from .generated_grpc_code import swm_service_pb2_grpc
from google.protobuf.json_format import MessageToDict, ParseDict

from dcs_memory.common.grpc_utils import retry_grpc_call, RETRYABLE_ERROR_CODES, DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_DELAY_S, DEFAULT_BACKOFF_FACTOR, DEFAULT_JITTER_FRACTION

logger = logging.getLogger(__name__)

class SWMClient:
    def __init__(self,
                 server_address: str = 'localhost:50053',
                 retry_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 retry_initial_delay_s: float = DEFAULT_INITIAL_DELAY_S,
                 retry_backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
                 retry_jitter_fraction: float = DEFAULT_JITTER_FRACTION):
        self.server_address = server_address
        self.channel = None
        self.stub = None
        self.retry_max_attempts = retry_max_attempts
        self.retry_initial_delay_s = retry_initial_delay_s
        self.retry_backoff_factor = retry_backoff_factor
        self.retry_jitter_fraction = retry_jitter_fraction
        # logger.info(f"SWMClient_DUMMY initialized for {self.server_address}") # Dummy log

    def connect(self):
        if not self.channel:
            try:
                self.channel = grpc.insecure_channel(self.server_address)
                # Проверка соединения (опционально, но полезно для быстрой диагностики)
                # grpc.channel_ready_future(self.channel).result(timeout=5) # 5 секунд на подключение
                self.stub = swm_service_pb2_grpc.SharedWorkingMemoryServiceStub(self.channel)
                logger.info(f"SWMClient: Успешно подключен к SWM сервису по адресу: {self.server_address}")
            except grpc.FutureTimeoutError:
                logger.error(f"SWMClient: Не удалось подключиться к SWM сервису по адресу {self.server_address} за 5 секунд.")
                self.channel = None # Сбрасываем канал, если подключение не удалось
                self.stub = None
            except Exception as e:
                logger.error(f"SWMClient: Ошибка при подключении к SWM сервису {self.server_address}: {e}", exc_info=True)
                self.channel = None
                self.stub = None


    def _ensure_connected(self):
        if not self.stub:
            self.connect()
        if not self.stub: # Если после connect() все еще нет стаба, значит, была ошибка
            raise ConnectionError(f"SWMClient: Не удалось установить соединение с SWM сервисом по адресу {self.server_address}")

    def _kem_dict_to_proto(self, kem_data: dict) -> kem_pb2.KEM:
        # Эта функция может быть общей для GLMClient и SWMClient, вынесена в utils? Пока дублируем.
        kem_data_copy = kem_data.copy()
        if 'created_at' in kem_data_copy and not isinstance(kem_data_copy['created_at'], str):
            del kem_data_copy['created_at']
        if 'updated_at' in kem_data_copy and not isinstance(kem_data_copy['updated_at'], str):
            del kem_data_copy['updated_at']
        if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
            kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')
        return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)

    def _kem_proto_to_dict(self, kem_proto: kem_pb2.KEM) -> dict:
        # Аналогично, может быть общей
        kem_dict = MessageToDict(kem_proto, preserving_proto_field_name=True, including_default_value_fields=True) # включая default поля
        return kem_dict

    @retry_grpc_call
    def publish_kem_to_swm(self, kem_data: dict, persist_to_glm_if_new_or_updated: bool = False, timeout: int = 10) -> Optional[dict]:
        self._ensure_connected()
        kem_proto = self._kem_dict_to_proto(kem_data)
        request = swm_service_pb2.PublishKEMToSWMRequest(
            kem_to_publish=kem_proto,
            persist_to_glm_if_new_or_updated=persist_to_glm_if_new_or_updated
        )
        logger.debug(f"SWMClient: PublishKEMToSWM request: {request}")
        response = self.stub.PublishKEMToSWM(request, timeout=timeout)

        # SWM.PublishKEMToSWM возвращает PublishKEMToSWMResponse, содержащий kem_id, published_to_swm, etc.
        # Для консистентности с другими методами SDK, которые возвращают саму КЕП (dict),
        # можно было бы запросить эту КЕП из SWM по ID, если published_to_swm=true.
        # Но это дополнительный вызов. Пока вернем dict с информацией из ответа.
        if response and response.published_to_swm:
            # Если КЕП была сохранена в GLM, ее ID мог измениться или уточниться.
            # SWM должен вернуть финальный ID.
            # Мы можем вернуть dict, представляющий результат операции, а не саму КЕП.
            # Или, если SWM возвращает обновленную КЕП в ответе (нет в proto), то ее.
            # Пока что, сформируем ответ на основе ID и статусов.
            # Если persist_to_glm=True, то kem_id в ответе - это ID из GLM.
            # Если КЕП обновлялась в SWM и GLM, то хорошо бы иметь актуальную КЕП.
            # Текущий proto не возвращает полную КЕП.
            # Вариант: если published_to_swm и есть kem_id, запросить ее через query_swm?
            # Пока простой ответ:
            logger.info(f"SWMClient: KEM ID '{response.kem_id}' published to SWM. Status: {response.status_message}")
            return {
                "kem_id": response.kem_id,
                "published_to_swm": response.published_to_swm,
                "persistence_triggered_to_glm": response.persistence_triggered_to_glm,
                "status_message": response.status_message
            }
        elif response:
            logger.warning(f"SWMClient: Failed to publish KEM to SWM. kem_id='{response.kem_id}', msg='{response.status_message}'")
            return {
                "kem_id": response.kem_id, # может быть пустым, если ID не был присвоен
                "published_to_swm": False,
                "persistence_triggered_to_glm": response.persistence_triggered_to_glm,
                "status_message": response.status_message
            }
        return None


    @retry_grpc_call
    def query_swm(self, kem_query: glm_service_pb2.KEMQuery,
                  page_size: int = 0, page_token: str = "", timeout: int = 10) -> Tuple[Optional[List[dict]], Optional[str]]:
        self._ensure_connected()
        request = swm_service_pb2.QuerySWMRequest(
            query=kem_query,
            page_size=page_size,
            page_token=page_token
        )
        logger.debug(f"SWMClient: QuerySWM request: {request}")
        response = self.stub.QuerySWM(request, timeout=timeout)

        if response and response.kems:
            kems_as_dicts = [self._kem_proto_to_dict(kem) for kem in response.kems]
            logger.info(f"SWMClient: QuerySWM returned {len(kems_as_dicts)} KEMs. Next page token: '{response.next_page_token}'.")
            return kems_as_dicts, response.next_page_token
        elif response: # Ответ есть, но kems пустой
            logger.info(f"SWMClient: QuerySWM returned no KEMs. Next page token: '{response.next_page_token}'.")
            return [], response.next_page_token
        return None, None

    @retry_grpc_call
    def load_kems_from_glm(self, query_for_glm: glm_service_pb2.KEMQuery, timeout: int = 20) -> Optional[dict]:
        self._ensure_connected()
        request = swm_service_pb2.LoadKEMsFromGLMRequest(query_for_glm=query_for_glm)
        logger.debug(f"SWMClient: LoadKEMsFromGLM request: {request}")
        response = self.stub.LoadKEMsFromGLM(request, timeout=timeout)

        if response:
            logger.info(f"SWMClient: LoadKEMsFromGLM response: Queried GLM: {response.kems_queried_in_glm_count}, Loaded to SWM: {response.kems_loaded_to_swm_count}, IDs: {list(response.loaded_kem_ids)}. Msg: {response.status_message}")
            return {
                "kems_queried_in_glm_count": response.kems_queried_in_glm_count,
                "kems_loaded_to_swm_count": response.kems_loaded_to_swm_count,
                "loaded_kem_ids": list(response.loaded_kem_ids),
                "status_message": response.status_message
            }
        return None

    def subscribe_to_swm_events(self, topics: Optional[List[swm_service_pb2.SubscriptionTopic]] = None,
                                agent_id: str = "default_sdk_agent",
                                timeout_per_event: Optional[float] = 5.0) -> Optional[typing.Generator[swm_service_pb2.SWMMemoryEvent, None, None]]:
        """
        Подписывается на события SWM и возвращает генератор, который выдает события.

        :param topics: Список объектов SubscriptionTopic для фильтрации. Если None или пуст, подписка на все события.
        :param agent_id: Идентификатор агента-подписчика.
        :param timeout_per_event: Таймаут (в секундах) для ожидания следующего события от сервера.
                                   Если None, будет блокироваться до следующего события или закрытия стрима.
                                   Используется для grpc.Stream.next(timeout=...).
        :return: Генератор SWMMemoryEvent или None, если не удалось начать подписку.
        """
        self._ensure_connected()

        request = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id=agent_id)
        if topics:
            request.topics.extend(topics)

        logger.info(f"SWMClient: Подписка на события SWM с запросом: {request}")

        try:
            event_stream = self.stub.SubscribeToSWMEvents(request) # Без таймаута на сам вызов метода

            def event_generator():
                try:
                    for event in event_stream:
                        logger.debug(f"SWMClient: Получено событие SWM: {event.event_type} для KEM ID {event.kem_payload.id if event.kem_payload else 'N/A'}")
                        yield event
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.CANCELLED:
                        logger.info(f"SWMClient: Стрим событий SWM был отменен клиентом (agent_id: {agent_id}).")
                    elif e.code() == grpc.StatusCode.UNAVAILABLE:
                         logger.warning(f"SWMClient: Стрим событий SWM недоступен (сервер мог быть остановлен). Agent_id: {agent_id}. Код: {e.code()}, Детали: {e.details()}")
                    else:
                        logger.error(f"SWMClient: gRPC ошибка в стриме событий SWM (agent_id: {agent_id}). Код: {e.code()}, Детали: {e.details()}", exc_info=True)
                    # Прекращаем генерацию при ошибке или отмене
                except Exception as e_gen:
                    logger.error(f"SWMClient: Непредвиденная ошибка в генераторе событий SWM (agent_id: {agent_id}): {e_gen}", exc_info=True)
                finally:
                    logger.info(f"SWMClient: Генератор событий SWM для agent_id '{agent_id}' завершен.")

            return event_generator()

        except grpc.RpcError as e:
            logger.error(f"SWMClient: Не удалось инициировать подписку на события SWM. Код: {e.code()}, Детали: {e.details()}", exc_info=True)
            return None
        except Exception as e_init:
            logger.error(f"SWMClient: Непредвиденная ошибка при инициации подписки на события SWM: {e_init}", exc_info=True)
            return None

    # --- Lock methods ---
    @retry_grpc_call
    def acquire_lock(self, resource_id: str, agent_id: str, timeout_ms: int = 0, lease_duration_ms: int = 0, rpc_timeout: int = 5) -> Optional[swm_service_pb2.AcquireLockResponse]:
        self._ensure_connected()
        request = swm_service_pb2.AcquireLockRequest(
            resource_id=resource_id,
            agent_id=agent_id,
            timeout_ms=timeout_ms,
            lease_duration_ms=lease_duration_ms
        )
        logger.debug(f"SWMClient: AcquireLock request: {request}")
        try:
            response = self.stub.AcquireLock(request, timeout=rpc_timeout)
            logger.info(f"SWMClient: AcquireLock for resource '{resource_id}' by agent '{agent_id}' status: {swm_service_pb2.LockStatusValue.Name(response.status)}. Lock ID: {response.lock_id}")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on AcquireLock for resource '{resource_id}': {e.code()} - {e.details()}", exc_info=True)
            # Можно вернуть "синтетический" ответ с ошибкой или None
            return swm_service_pb2.AcquireLockResponse(
                resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ERROR,
                message=f"gRPC Error: {e.details()}"
            )
        except Exception as e_acq:
            logger.error(f"SWMClient: Unexpected error on AcquireLock for resource '{resource_id}': {e_acq}", exc_info=True)
            return swm_service_pb2.AcquireLockResponse(
                resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ERROR,
                message=f"Unexpected SDK error: {e_acq}"
            )


    @retry_grpc_call
    def release_lock(self, resource_id: str, agent_id: str, lock_id: Optional[str] = None, rpc_timeout: int = 5) -> Optional[swm_service_pb2.ReleaseLockResponse]:
        self._ensure_connected()
        request = swm_service_pb2.ReleaseLockRequest(
            resource_id=resource_id,
            agent_id=agent_id,
            lock_id=lock_id if lock_id else ""
        )
        logger.debug(f"SWMClient: ReleaseLock request: {request}")
        try:
            response = self.stub.ReleaseLock(request, timeout=rpc_timeout)
            logger.info(f"SWMClient: ReleaseLock for resource '{resource_id}' by agent '{agent_id}' status: {swm_service_pb2.ReleaseStatusValue.Name(response.status)}.")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on ReleaseLock for resource '{resource_id}': {e.code()} - {e.details()}", exc_info=True)
            return swm_service_pb2.ReleaseLockResponse(
                resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING,
                message=f"gRPC Error: {e.details()}"
            )
        except Exception as e_rel:
            logger.error(f"SWMClient: Unexpected error on ReleaseLock for resource '{resource_id}': {e_rel}", exc_info=True)
            return swm_service_pb2.ReleaseLockResponse(
                resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING,
                message=f"Unexpected SDK error: {e_rel}"
            )

    @retry_grpc_call
    def get_lock_info(self, resource_id: str, rpc_timeout: int = 5) -> Optional[swm_service_pb2.LockInfo]:
        self._ensure_connected()
        request = swm_service_pb2.GetLockInfoRequest(resource_id=resource_id)
        logger.debug(f"SWMClient: GetLockInfo request for resource '{resource_id}'")
        try:
            response = self.stub.GetLockInfo(request, timeout=rpc_timeout)
            logger.info(f"SWMClient: GetLockInfo for resource '{resource_id}': is_locked={response.is_locked}, holder='{response.current_holder_agent_id}'.")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on GetLockInfo for resource '{resource_id}': {e.code()} - {e.details()}", exc_info=True)
            return None # Или вернуть LockInfo с is_locked=false и сообщением об ошибке? Пока None.
        except Exception as e_info:
            logger.error(f"SWMClient: Unexpected error on GetLockInfo for resource '{resource_id}': {e_info}", exc_info=True)
            return None

    # --- Distributed Counter methods ---
    @retry_grpc_call
    def increment_counter(self, counter_id: str, increment_by: int = 1, rpc_timeout: int = 5) -> Optional[swm_service_pb2.CounterValueResponse]:
        self._ensure_connected()
        request = swm_service_pb2.IncrementCounterRequest(counter_id=counter_id, increment_by=increment_by)
        logger.debug(f"SWMClient: IncrementCounter request: {request}")
        try:
            response = self.stub.IncrementCounter(request, timeout=rpc_timeout)
            logger.info(f"SWMClient: IncrementCounter for counter_id '{counter_id}' new value: {response.current_value}")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on IncrementCounter for counter_id '{counter_id}': {e.code()} - {e.details()}", exc_info=True)
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"gRPC Error: {e.details()}") # Возвращаем 0 и ошибку
        except Exception as e_inc:
            logger.error(f"SWMClient: Unexpected error on IncrementCounter for counter_id '{counter_id}': {e_inc}", exc_info=True)
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"Unexpected SDK error: {e_inc}")


    @retry_grpc_call
    def get_counter(self, counter_id: str, rpc_timeout: int = 5) -> Optional[swm_service_pb2.CounterValueResponse]:
        self._ensure_connected()
        request = swm_service_pb2.DistributedCounterRequest(counter_id=counter_id)
        logger.debug(f"SWMClient: GetCounter request for counter_id '{counter_id}'")
        try:
            response = self.stub.GetCounter(request, timeout=rpc_timeout)
            logger.info(f"SWMClient: GetCounter for counter_id '{counter_id}' value: {response.current_value}")
            return response
        except grpc.RpcError as e:
            logger.error(f"SWMClient: gRPC error on GetCounter for counter_id '{counter_id}': {e.code()} - {e.details()}", exc_info=True)
            # Сервер возвращает 0 и сообщение, если счетчик не найден. Если ошибка gRPC, то это другое.
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"gRPC Error: {e.details()}")
        except Exception as e_getc:
            logger.error(f"SWMClient: Unexpected error on GetCounter for counter_id '{counter_id}': {e_getc}", exc_info=True)
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"Unexpected SDK error: {e_getc}")

    def close(self):
        if self.channel:
            self.channel.close()
            self.channel = None
            self.stub = None
            logger.info("SWMClient: Канал SWMClient закрыт.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == '__main__':
    # Пример использования (требует запущенного SWM и GLM сервисов)
    # Этот блок не будет выполняться при импорте, только при прямом запуске файла.
    # Для реального теста нужно настроить адреса и иметь запущенные сервисы.
    print("SWMClient Example (requires SWM server on localhost:50053)")

    # Настройка базового логирования для примера
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Пример KEMQuery (используется в GLM и SWM)
    kem_q = glm_service_pb2.KEMQuery()
    # kem_q.ids.extend(["some_id_1", "some_id_2"])
    kem_q.metadata_filters["example_meta_key"] = "example_value"

    # Пример данных КЕП
    example_kem_data = {
        "id": "swm_client_test_001",
        "content_type": "text/plain",
        "content": "Это тестовая КЕП для SWM клиента.",
        "metadata": {"source": "swm_client_example", "version": "1.0"},
        "embeddings": [i*0.01 for i in range(384)] # Пример эмбеддинга
    }

    try:
        with SWMClient(server_address='localhost:50053') as client:
            # 1. Публикация КЕП
            print("\n--- Публикация КЕП в SWM ---")
            publish_result = client.publish_kem_to_swm(example_kem_data, persist_to_glm_if_new_or_updated=True)
            if publish_result and publish_result.get("published_to_swm"):
                print(f"КЕП опубликована: ID='{publish_result.get('kem_id')}', Статус GLM: {publish_result.get('persistence_triggered_to_glm')}")
                published_kem_id = publish_result.get('kem_id')

                # 2. Запрос этой КЕП из SWM
                if published_kem_id:
                    print(f"\n--- Запрос КЕП ID '{published_kem_id}' из SWM ---")
                    query_by_id = glm_service_pb2.KEMQuery()
                    query_by_id.ids.extend([published_kem_id])
                    kems_tuple_id = client.query_swm(kem_query=query_by_id, page_size=1)
                    if kems_tuple_id and kems_tuple_id[0]:
                        print(f"Найдена КЕП: {kems_tuple_id[0][0]}")
                    else:
                        print(f"КЕП ID '{published_kem_id}' не найдена в SWM после публикации.")
            else:
                print(f"Не удалось опубликовать КЕП. Результат: {publish_result}")

            # 3. Запрос по метаданным (может вернуть и другие КЕП, если они есть)
            print("\n--- Запрос КЕП из SWM по метаданным {'source': 'swm_client_example'} ---")
            query_by_meta = glm_service_pb2.KEMQuery()
            query_by_meta.metadata_filters["source"] = "swm_client_example"
            kems_tuple_meta, next_page = client.query_swm(kem_query=query_by_meta, page_size=5)
            if kems_tuple_meta:
                print(f"Найдено {len(kems_tuple_meta)} КЕП по метаданным:")
                for k_dict in kems_tuple_meta:
                    print(f"  ID: {k_dict.get('id')}, Content: {k_dict.get('content')[:30]}...")
            else:
                print("Не найдено КЕП по метаданным.")

            # 4. Загрузка КЕП из GLM в SWM (пример запроса)
            print("\n--- Запрос на загрузку КЕП из GLM в SWM (по метаданным {'topic': 'AI'}) ---")
            load_query = glm_service_pb2.KEMQuery()
            load_query.metadata_filters["topic"] = "AI" # Предположим, в GLM есть КЕПы с такой метаинформацией
            load_result = client.load_kems_from_glm(query_for_glm=load_query)
            if load_result:
                print(f"Результат загрузки из GLM: {load_result}")
            else:
                print("Не удалось выполнить запрос на загрузку из GLM.")

    except ConnectionError as e:
        print(f"ОШИБКА СОЕДИНЕНИЯ: {e}")
    except grpc.RpcError as e:
        print(f"gRPC ОШИБКА: code={e.code()}, details={e.details()}")
    except Exception as e:
        print(f"НЕПРЕДВИДЕННАЯ ОШИБКА: {e}", exc_info=True)

```
