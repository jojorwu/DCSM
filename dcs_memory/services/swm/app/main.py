import grpc
from concurrent import futures
import time
import sys
import os
import uuid
import logging
import threading
from cachetools import LRUCache
from google.protobuf.timestamp_pb2 import Timestamp
import typing # Добавлено для typing.Optional и typing.List

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
# --- Конец настройки логирования ---

# --- Начало блока для корректного импорта сгенерированного кода ---
current_script_path = os.path.abspath(__file__)
app_dir_swm = os.path.dirname(current_script_path)
service_root_dir_swm = os.path.dirname(app_dir_swm)

if service_root_dir_swm not in sys.path:
    sys.path.insert(0, service_root_dir_swm)

# generated_grpc/__init__.py должен теперь сам обрабатывать свои внутренние пути
from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2 # Нужен для KEMQuery
from generated_grpc import glm_service_pb2_grpc # Нужен для GLM клиента
from generated_grpc import swm_service_pb2
from generated_grpc import swm_service_pb2_grpc
# --- Конец блока для корректного импорта ---

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

# --- Конфигурация ---
GLM_SERVICE_ADDRESS_CONFIG = os.getenv("GLM_SERVICE_ADDRESS", "localhost:50051")
SWM_GRPC_LISTEN_ADDRESS_CONFIG = os.getenv("SWM_GRPC_LISTEN_ADDRESS", "[::]:50053")
SWM_INTERNAL_CACHE_MAX_SIZE = int(os.getenv("SWM_CACHE_MAX_SIZE", 200))
DEFAULT_SWM_PAGE_SIZE = int(os.getenv("DEFAULT_SWM_PAGE_SIZE", 20)) # Отдельный дефолт для SWM
# --- Конец конфигурации ---

class SharedWorkingMemoryServiceImpl(swm_service_pb2_grpc.SharedWorkingMemoryServiceServicer):
    def __init__(self):
        logger.info("Инициализация SharedWorkingMemoryServiceImpl...")
        self.glm_channel = None
        self.glm_stub = None
        self.swm_cache: LRUCache[str, kem_pb2.KEM] = LRUCache(maxsize=SWM_INTERNAL_CACHE_MAX_SIZE) # Уточнили тип
        self.cache_lock = threading.Lock()

        try:
            self.glm_channel = grpc.insecure_channel(GLM_SERVICE_ADDRESS_CONFIG)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.glm_channel)
            logger.info(f"GLM клиент для SWM инициализирован, целевой адрес: {GLM_SERVICE_ADDRESS_CONFIG}")
        except Exception as e:
            logger.error(f"Ошибка при инициализации GLM клиента в SWM: {e}")

    def _get_kem_from_cache(self, kem_id: str) -> typing.Optional[kem_pb2.KEM]:
        with self.cache_lock:
            return self.swm_cache.get(kem_id)

    def _put_kem_to_cache(self, kem: kem_pb2.KEM) -> None:
        if not kem or not kem.id:
            logger.warning("Попытка добавить невалидную КЕП в кэш SWM.")
            return
        with self.cache_lock:
            self.swm_cache[kem.id] = kem
        logger.info("КЕП ID '{}' добавлена/обновлена в кэше SWM. Размер кэша: {}/{}".format(
            kem.id, len(self.swm_cache), self.swm_cache.maxsize))
        self._notify_subscribers(kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED if kem.id not in self.swm_cache else swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED) # Simplified logic


    def _delete_kem_from_cache(self, kem_id: str) -> bool:
        with self.cache_lock:
            if kem_id in self.swm_cache:
                evicted_kem = self.swm_cache.pop(kem_id)
                logger.info(f"КЕП ID '{kem_id}' удалена из кэша SWM.")
                if evicted_kem: # Should always be true if it was in cache
                    self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED)
                return True
            return False

    def _notify_subscribers(self, kem: kem_pb2.KEM, event_type: swm_service_pb2.SWMMemoryEvent.EventType, source_agent_id: str = "SWM_SERVER"):
        logger.info(f"Подготовка к уведомлению подписчиков о событии {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} для КЕП ID '{kem.id}'")
        if not self.subscribers:
            return

        event_time_proto = Timestamp(); event_time_proto.GetCurrentTime()

        event = swm_service_pb2.SWMMemoryEvent(
            event_id=str(uuid.uuid4()), event_type=event_type,
            kem_payload=kem, event_time=event_time_proto,
            source_agent_id=source_agent_id,
            details=f"Event {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} for KEM ID {kem.id}"
        )

        with self.subscribers_lock:
            current_subscribers = list(self.subscribers.items())

        for agent_id, stream_writer_context in current_subscribers:
            try:
                logger.info(f"Отправка события {event.event_id} агенту {agent_id}...")
                # In a real gRPC stream, context itself is used to write.
                # This part needs a proper PubSub mechanism. For now, this is a placeholder.
                # If stream_writer_context is the gRPC context:
                # stream_writer_context.write(event)
            except Exception as e:
                logger.error(f"Ошибка при отправке события агенту {agent_id}: {e}. Удаление подписчика.")
                with self.subscribers_lock: self.subscribers.pop(agent_id, None)


    # --- Реализация RPC методов ---

    def LoadKEMsFromGLM(self, request: swm_service_pb2.LoadKEMsFromGLMRequest, context):
        logger.info("SWM: LoadKEMsFromGLM вызван с запросом: {}".format(request.query_for_glm))
        if not self.glm_stub:
            msg = "GLM сервис недоступен для SWM (клиент не инициализирован)."
            logger.error(msg)
            context.abort(grpc.StatusCode.INTERNAL, msg)
            return swm_service_pb2.LoadKEMsFromGLMResponse(status_message=msg)

        glm_retrieve_request = glm_service_pb2.RetrieveKEMsRequest(query=request.query_for_glm)
        # The KEMQuery in query_for_glm should contain page_size and page_token for GLM if pagination is needed from GLM.
        # limit_override is not directly in RetrieveKEMsRequest, it should be part of KEMQuery's page_size.
        # For simplicity, assume query_for_glm.page_size is used if needed.
        # If request.limit_override is present and meant to control the *total* number from GLM over multiple pages,
        # that would require a loop here, which is not implemented.
        # For now, we assume one call to GLM with parameters from query_for_glm.

        loaded_kems_count = 0
        loaded_ids = []
        kems_from_glm_for_stats = 0

        try:
            logger.info("SWM: Запрос к GLM.RetrieveKEMs: {}".format(glm_retrieve_request))
            glm_response = self.glm_stub.RetrieveKEMs(glm_retrieve_request, timeout=20)

            if glm_response and glm_response.kems:
                kems_from_glm_for_stats = len(glm_response.kems)
                for kem_from_glm in glm_response.kems:
                    self._put_kem_to_cache(kem_from_glm)
                    loaded_ids.append(kem_from_glm.id)
                loaded_kems_count = len(loaded_ids)
                logger.info("SWM: {} КЕП загружено из GLM и добавлено/обновлено в кэш SWM.".format(loaded_kems_count))
                status_msg = "Загружено {} КЕП.".format(loaded_kems_count)
            else:
                status_msg = "GLM не вернул КЕП по запросу."
                logger.info(status_msg)

            return swm_service_pb2.LoadKEMsFromGLMResponse(
                kems_queried_in_glm_count=kems_from_glm_for_stats,
                kems_loaded_to_swm_count=loaded_kems_count,
                loaded_kem_ids=loaded_ids,
                status_message=status_msg
            )
        except grpc.RpcError as e:
            msg = "SWM: gRPC ошибка при вызове GLM.RetrieveKEMs: code={}, details={}".format(e.code(), e.details())
            logger.error(msg)
            context.abort(e.code(), msg)
            return swm_service_pb2.LoadKEMsFromGLMResponse(status_message=msg)
        except Exception as e:
            msg = "SWM: Непредвиденная ошибка в LoadKEMsFromGLM при работе с GLM: {}".format(e)
            logger.error(msg, exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, msg)
            return swm_service_pb2.LoadKEMsFromGLMResponse(status_message=msg)

    def QuerySWM(self, request: swm_service_pb2.QuerySWMRequest, context):
        query = request.query # Это dcsm.KEMQuery
        logger.info("SWM: QuerySWM вызван с KEMQuery: {}".format(query))

        if query.embedding_query or query.text_query: # Проверка на неподдерживаемые типы запросов
            msg = "Векторный или текстовый поиск напрямую в SWM кэше не поддерживается. Используйте GLM."
            logger.warning(msg)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
            return swm_service_pb2.QuerySWMResponse()

        page_size = request.page_size if request.page_size > 0 else DEFAULT_SWM_PAGE_SIZE
        offset = 0
        if request.page_token:
            try: offset = int(request.page_token)
            except ValueError:
                logger.warning("Неверный page_token для QuerySWM: '{}', используется offset=0.".format(request.page_token))

        candidate_kems: typing.List[kem_pb2.KEM] = []
        with self.cache_lock:
            all_kems_in_cache = list(self.swm_cache.values())

        logger.debug("Всего КЕП в кэше SWM перед фильтрацией: {}".format(len(all_kems_in_cache)))

        if query.ids:
            ids_set = set(query.ids)
            candidate_kems = [kem for kem in all_kems_in_cache if kem.id in ids_set]
        else:
            candidate_kems = all_kems_in_cache

        if query.metadata_filters:
            filtered_by_metadata = []
            for kem in candidate_kems:
                match = True
                for key, value in query.metadata_filters.items():
                    if kem.metadata.get(key) != value:
                        match = False; break
                if match: filtered_by_metadata.append(kem)
            candidate_kems = filtered_by_metadata

        def check_date_filter(kem_ts_field, filter_start_ts, filter_end_ts) -> bool:
            match = True
            if filter_start_ts and (filter_start_ts.seconds > 0 or filter_start_ts.nanos > 0):
                if kem_ts_field.ToNanoseconds() < filter_start_ts.ToNanoseconds(): match = False
            if match and filter_end_ts and (filter_end_ts.seconds > 0 or filter_end_ts.nanos > 0):
                if kem_ts_field.ToNanoseconds() > filter_end_ts.ToNanoseconds(): match = False
            return match

        if query.HasField("created_at_start") or query.HasField("created_at_end"):
            candidate_kems = [k for k in candidate_kems if check_date_filter(k.created_at, query.created_at_start, query.created_at_end)]

        if query.HasField("updated_at_start") or query.HasField("updated_at_end"):
            candidate_kems = [k for k in candidate_kems if check_date_filter(k.updated_at, query.updated_at_start, query.updated_at_end)]

        start_index = offset
        end_index = offset + page_size
        kems_on_page = candidate_kems[start_index:end_index]

        next_page_token_str = ""
        if end_index < len(candidate_kems):
            next_page_token_str = str(end_index)

        logger.info("QuerySWM: Возвращено {} КЕП.".format(len(kems_on_page)))
        return swm_service_pb2.QuerySWMResponse(kems=kems_on_page, next_page_token=next_page_token_str)


    def PublishKEMToSWM(self, request: swm_service_pb2.PublishKEMToSWMRequest, context):
        kem_to_publish = request.kem_to_publish
        logger.info("SWM: PublishKEMToSWM вызван для КЕП ID (предложенный): '{}'".format(kem_to_publish.id))

        kem_id_final = kem_to_publish.id
        if not kem_id_final:
            kem_id_final = str(uuid.uuid4())
            kem_to_publish.id = kem_id_final
            logger.info("SWM: ID не предоставлен, сгенерирован новый ID: '{}'".format(kem_id_final))

        ts = Timestamp(); ts.GetCurrentTime()
        # Если КЕП уже есть в кэше, сохраняем ее created_at
        existing_kem_in_cache = self._get_kem_from_cache(kem_id_final)
        if existing_kem_in_cache and existing_kem_in_cache.HasField("created_at"):
            kem_to_publish.created_at.CopyFrom(existing_kem_in_cache.created_at)
        elif not kem_to_publish.HasField("created_at"): # Новая для кэша, created_at не задан
            kem_to_publish.created_at.CopyFrom(ts)
        # updated_at всегда обновляется
        kem_to_publish.updated_at.CopyFrom(ts)

        self._put_kem_to_cache(kem_to_publish)

        published_to_swm_flag = True
        persistence_triggered_flag = False
        status_msg = "КЕП ID '{}' успешно опубликована в SWM.".format(kem_id_final)

        if request.persist_to_glm_if_new_or_updated:
            if not self.glm_stub:
                msg_glm = "GLM сервис недоступен, КЕП ID '{}' не будет сохранена.".format(kem_id_final)
                logger.error(msg_glm); status_msg += " " + msg_glm
            else:
                try:
                    logger.info("SWM: Инициирование сохранения КЕП ID '{}' в GLM...".format(kem_id_final))
                    # Передаем КЕП как есть, GLM разберется с created_at/updated_at при сохранении
                    glm_store_req = glm_service_pb2.StoreKEMRequest(kem=kem_to_publish)
                    glm_store_resp = self.glm_stub.StoreKEM(glm_store_req, timeout=10)
                    if glm_store_resp and glm_store_resp.kem and glm_store_resp.kem.id:
                        # Обновляем КЕП в SWM на ту, что вернул GLM, для консистентности ID и временных меток GLM
                        self._put_kem_to_cache(glm_store_resp.kem)
                        kem_id_final = glm_store_resp.kem.id
                        status_msg += " Успешно сохранено/обновлено в GLM с ID '{}'.".format(kem_id_final)
                        persistence_triggered_flag = True
                    else:
                        msg_glm_err = "GLM.StoreKEM не вернул ответ для КЕП ID '{}'.".format(kem_id_final)
                        logger.error(msg_glm_err); status_msg += " " + msg_glm_err
                except Exception as e:
                    msg_glm_rpc_err = "Ошибка при сохранении КЕП ID '{}' в GLM: {}".format(kem_id_final, e)
                    logger.error(msg_glm_rpc_err, exc_info=True); status_msg += " " + msg_glm_rpc_err

        return swm_service_pb2.PublishKEMToSWMResponse(
            kem_id=kem_id_final, published_to_swm=published_to_swm_flag,
            persistence_triggered_to_glm=persistence_triggered_flag, status_message=status_msg
        )

    def SubscribeToSWMEvents(self, request: swm_service_pb2.SubscribeToSWMEventsRequest, context):
        agent_id = request.agent_id
        logger.info(f"SWM: Новый подписчик {agent_id} на темы: {request.topics}")

        # TODO: Реализовать полноценную систему подписки с фильтрацией по topics
        # и очередями для каждого подписчика.

        # Заглушка: отправляем тестовые события, пока клиент активен
        event_count = 0
        while context.is_active():
            time.sleep(5)
            evt_time = Timestamp(); evt_time.GetCurrentTime()
            fake_kem = kem_pb2.KEM(id=f"heartbeat_kem_{event_count}_{agent_id}", metadata={"subscriber": agent_id})
            event = swm_service_pb2.SWMMemoryEvent(
                event_id=str(uuid.uuid4()),
                event_type=swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED,
                kem_payload=fake_kem, event_time=evt_time, details="Heartbeat/Test Event"
            )
            try:
                yield event
                event_count += 1
                logger.debug(f"Отправлено тестовое событие {event_count} подписчику {agent_id}")
            except grpc.RpcError as e:
                logger.warning(f"Ошибка при отправке события подписчику {agent_id}: {e.code()} - {e.details()}")
                break
        logger.info(f"SWM: Стрим для подписчика {agent_id} завершен.")

    def __del__(self):
        if self.glm_channel:
            self.glm_channel.close()
            logger.info("Канал GLM клиента в SWM закрыт.")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    swm_service_pb2_grpc.add_SharedWorkingMemoryServiceServicer_to_server(
        SharedWorkingMemoryServiceImpl(), server
    )
    server.add_insecure_port(SWM_GRPC_LISTEN_ADDRESS_CONFIG)
    logger.info(f"Запуск SWM (Shared Working Memory Service) на {SWM_GRPC_LISTEN_ADDRESS_CONFIG}...")
    server.start()
    logger.info(f"SWM запущен и ожидает соединений на {SWM_GRPC_LISTEN_ADDRESS_CONFIG}.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Остановка SWM...")
    finally:
        server.stop(None)
        logger.info("SWM остановлен.")

if __name__ == '__main__':
    serve()
