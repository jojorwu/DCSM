import grpc
import grpc.aio # Added for grpc.aio.server
import asyncio # Added for asyncio.Lock
from concurrent import futures
import time
import sys
import os
import uuid
import logging
# import threading # Replaced by asyncio for lock
from cachetools import LRUCache # Still needed for type hinting if not fully replaced by wrapper's type hint
from .cache import AsyncLRUCache # Import the async wrapper
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
        # self.swm_cache: LRUCache[str, kem_pb2.KEM] = LRUCache(maxsize=SWM_INTERNAL_CACHE_MAX_SIZE) # Old synchronous cache
        self.swm_cache: AsyncLRUCache[str, kem_pb2.KEM] = AsyncLRUCache(maxsize=SWM_INTERNAL_CACHE_MAX_SIZE) # New async cache
        self.cache_lock = asyncio.Lock() # New asyncio lock

        # Initialize subscriber-related attributes
        self.subscribers: typing.Dict[str, grpc.aio.ServicerContext] = {} # Store agent_id to context/stream
        self.subscribers_lock = asyncio.Lock() # Lock for self.subscribers

        try:
            self.glm_channel = grpc.insecure_channel(GLM_SERVICE_ADDRESS_CONFIG)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.glm_channel)
            logger.info(f"GLM клиент для SWM инициализирован, целевой адрес: {GLM_SERVICE_ADDRESS_CONFIG}")
        except Exception as e:
            logger.error(f"Ошибка при инициализации GLM клиента в SWM: {e}")

    async def _get_kem_from_cache(self, kem_id: str) -> typing.Optional[kem_pb2.KEM]:
        async with self.cache_lock:
            # The get method of AsyncLRUCache is already async
            return await self.swm_cache.get(kem_id)

    async def _put_kem_to_cache(self, kem: kem_pb2.KEM) -> None:
        if not kem or not kem.id:
            logger.warning("Попытка добавить невалидную КЕП в кэш SWM.")
            return

        kem_exists_in_cache_before_put: bool
        async with self.cache_lock:
            # Check existence before put for notification logic
            kem_exists_in_cache_before_put = await self.swm_cache.__contains__(kem.id)
            await self.swm_cache.set(kem.id, kem) # Use .set() method
            current_len = await self.swm_cache.__len__()
            max_s = await self.swm_cache.maxsize

        logger.info("КЕП ID '{}' добавлена/обновлена в кэше SWM. Размер кэша: {}/{}".format(
            kem.id, current_len, max_s)) # Use awaited len and maxsize

        # Determine event type based on whether KEM was already in cache *before* this put operation
        event_type = swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED if kem_exists_in_cache_before_put else swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED

        # _notify_subscribers is now async, so await it.
        await self._notify_subscribers(kem, event_type)


    async def _delete_kem_from_cache(self, kem_id: str) -> bool:
        async with self.cache_lock:
            if await self.swm_cache.__contains__(kem_id): # Use async __contains__
                evicted_kem = await self.swm_cache.pop(kem_id) # Use async pop
                logger.info(f"КЕП ID '{kem_id}' удалена из кэша SWM.")
                if evicted_kem: # Should always be true if it was in cache
                    # _notify_subscribers is now async, so await it.
                    await self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED)
                return True
            return False

    async def _notify_subscribers(self, kem: kem_pb2.KEM, event_type: swm_service_pb2.SWMMemoryEvent.EventType, source_agent_id: str = "SWM_SERVER"):
        logger.info(f"Подготовка к уведомлению подписчиков о событии {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} для КЕП ID '{kem.id}'")

        active_subscribers_to_notify: list[tuple[str, grpc.aio.ServicerContext]] = []
        async with self.subscribers_lock:
            if not self.subscribers:
                return
            # Create a list of subscribers to notify outside the lock to avoid holding lock during writes
            active_subscribers_to_notify = list(self.subscribers.items())

        if not active_subscribers_to_notify:
            return

        event_time_proto = Timestamp(); event_time_proto.GetCurrentTime()
        event = swm_service_pb2.SWMMemoryEvent(
            event_id=str(uuid.uuid4()), event_type=event_type,
            kem_payload=kem, event_time=event_time_proto,
            source_agent_id=source_agent_id,
            details=f"Event {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} for KEM ID {kem.id}"
        )

        subscribers_to_remove = []
        for agent_id, stream_context in active_subscribers_to_notify:
            try:
                if stream_context.is_active(): # Check if stream is still active before writing
                    logger.info(f"Отправка события {event.event_id} агенту {agent_id}...")
                    await stream_context.write(event) # Use await for async write
                else:
                    logger.warning(f"Stream для агента {agent_id} неактивен. Пометка на удаление.")
                    subscribers_to_remove.append(agent_id)
            except Exception as e: # Broad exception to catch various stream errors
                logger.error(f"Ошибка при отправке события агенту {agent_id}: {e}. Пометка на удаление.")
                subscribers_to_remove.append(agent_id)

        if subscribers_to_remove:
            async with self.subscribers_lock:
                for agent_id in subscribers_to_remove:
                    self.subscribers.pop(agent_id, None)
                    logger.info(f"Удален неактивный/ошибочный подписчик: {agent_id}")


    # --- Реализация RPC методов ---

    async def LoadKEMsFromGLM(self, request: swm_service_pb2.LoadKEMsFromGLMRequest, context): # async def
        logger.info("SWM: LoadKEMsFromGLM вызван с запросом: {}".format(request.query_for_glm))
        if not self.glm_stub:
            msg = "GLM сервис недоступен для SWM (клиент не инициализирован)."
            logger.error(msg)
            # For async gRPC methods, context.abort is not directly available.
            # We need to raise an RpcError or set code and details.
            # await context.abort(grpc.StatusCode.INTERNAL, msg) # This is for grpc.aio.ServicerContext
            # Assuming 'context' will be an async context, it might have 'set_code' and 'set_details'
            # For now, let's keep it simple and rely on how grpc.aio handles exceptions or manual status setting.
            # A common way is to raise an RpcError or let the framework handle it.
            # If context.abort is not available, we'll need to adjust error handling for async gRPC.
            # For grpc.aio, you typically raise an RpcError or set details and code on the context.
            # Let's assume we'll raise an error that the framework converts.
            # Or, more directly:
            await context.set_code(grpc.StatusCode.INTERNAL)
            await context.set_details(msg)
            return swm_service_pb2.LoadKEMsFromGLMResponse(status_message=msg)

        glm_retrieve_request = glm_service_pb2.RetrieveKEMsRequest(query=request.query_for_glm)

        loaded_kems_count = 0
        loaded_ids = []
        kems_from_glm_for_stats = 0

        try:
            logger.info("SWM: Запрос к GLM.RetrieveKEMs: {}".format(glm_retrieve_request))
            # Run synchronous gRPC call in a separate thread
            glm_response = await asyncio.to_thread(
                self.glm_stub.RetrieveKEMs, glm_retrieve_request, timeout=20
            )

            if glm_response and glm_response.kems:
                kems_from_glm_for_stats = len(glm_response.kems)
                for kem_from_glm in glm_response.kems:
                    await self._put_kem_to_cache(kem_from_glm) # await async helper
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
            await context.set_code(e.code())
            await context.set_details(msg)
            # context.abort(e.code(), msg) # old
            return swm_service_pb2.LoadKEMsFromGLMResponse(status_message=msg)
        except Exception as e:
            msg = "SWM: Непредвиденная ошибка в LoadKEMsFromGLM при работе с GLM: {}".format(e)
            logger.error(msg, exc_info=True)
            await context.set_code(grpc.StatusCode.INTERNAL)
            await context.set_details(msg)
            # context.abort(grpc.StatusCode.INTERNAL, msg) # old
            return swm_service_pb2.LoadKEMsFromGLMResponse(status_message=msg)

    async def QuerySWM(self, request: swm_service_pb2.QuerySWMRequest, context): # async def
        query = request.query # Это dcsm.KEMQuery
        logger.info("SWM: QuerySWM вызван с KEMQuery: {}".format(query))

        if query.embedding_query or query.text_query: # Проверка на неподдерживаемые типы запросов
            msg = "Векторный или текстовый поиск напрямую в SWM кэше не поддерживается. Используйте GLM."
            logger.warning(msg)
            await context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            await context.set_details(msg)
            # context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg) # old
            return swm_service_pb2.QuerySWMResponse()

        page_size = request.page_size if request.page_size > 0 else DEFAULT_SWM_PAGE_SIZE
        offset = 0
        if request.page_token:
            try: offset = int(request.page_token)
            except ValueError:
                logger.warning("Неверный page_token для QuerySWM: '{}', используется offset=0.".format(request.page_token))

        candidate_kems: typing.List[kem_pb2.KEM] = []
        # Accessing cache needs to be async
        async with self.cache_lock: # Use async lock
            # self.swm_cache.values() is now an async method in AsyncLRUCache
            all_kems_in_cache = await self.swm_cache.values()

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


    async def PublishKEMToSWM(self, request: swm_service_pb2.PublishKEMToSWMRequest, context): # async def
        kem_to_publish = request.kem_to_publish
        logger.info("SWM: PublishKEMToSWM вызван для КЕП ID (предложенный): '{}'".format(kem_to_publish.id))

        kem_id_final = kem_to_publish.id
        if not kem_id_final:
            kem_id_final = str(uuid.uuid4())
            kem_to_publish.id = kem_id_final
            logger.info("SWM: ID не предоставлен, сгенерирован новый ID: '{}'".format(kem_id_final))

        ts = Timestamp(); ts.GetCurrentTime()

        # If KEM is already in cache, preserve its created_at
        # _get_kem_from_cache is now async
        existing_kem_in_cache = await self._get_kem_from_cache(kem_id_final)
        if existing_kem_in_cache and existing_kem_in_cache.HasField("created_at"):
            kem_to_publish.created_at.CopyFrom(existing_kem_in_cache.created_at)
        elif not kem_to_publish.HasField("created_at"): # New to cache, created_at not set
            kem_to_publish.created_at.CopyFrom(ts)
        # updated_at is always updated
        kem_to_publish.updated_at.CopyFrom(ts)

        await self._put_kem_to_cache(kem_to_publish) # await async helper

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
                    glm_store_req = glm_service_pb2.StoreKEMRequest(kem=kem_to_publish)
                    # Run synchronous gRPC call in a separate thread
                    glm_store_resp = await asyncio.to_thread(
                        self.glm_stub.StoreKEM, glm_store_req, timeout=10
                    )
                    if glm_store_resp and glm_store_resp.kem and glm_store_resp.kem.id:
                        # Update KEM in SWM with the one returned by GLM for consistency
                        await self._put_kem_to_cache(glm_store_resp.kem) # await async helper
                        kem_id_final = glm_store_resp.kem.id
                        status_msg += " Успешно сохранено/обновлено в GLM с ID '{}'.".format(kem_id_final)
                        persistence_triggered_flag = True
                    else:
                        msg_glm_err = "GLM.StoreKEM не вернул ответ для КЕП ID '{}'.".format(kem_id_final)
                        logger.error(msg_glm_err); status_msg += " " + msg_glm_err
                except Exception as e: # Includes grpc.RpcError from to_thread context
                    msg_glm_rpc_err = "Ошибка при сохранении КЕП ID '{}' в GLM: {}".format(kem_id_final, e)
                    logger.error(msg_glm_rpc_err, exc_info=True); status_msg += " " + msg_glm_rpc_err
                    # Note: No context.abort here, error is added to status_msg. Client won't see gRPC error directly for this part.

        return swm_service_pb2.PublishKEMToSWMResponse(
            kem_id=kem_id_final, published_to_swm=published_to_swm_flag,
            persistence_triggered_to_glm=persistence_triggered_flag, status_message=status_msg
        )

    async def SubscribeToSWMEvents(self, request: swm_service_pb2.SubscribeToSWMEventsRequest, context: grpc.aio.ServicerContext): # async def, added type hint for context
        agent_id = request.agent_id
        logger.info(f"SWM: Новый подписчик {agent_id} на темы: {request.topics}")

        # Add subscriber to the dictionary
        async with self.subscribers_lock:
            if agent_id in self.subscribers:
                logger.warning(f"Агент {agent_id} уже подписан. Переподписка.")
                # Optionally, you might want to handle this differently, e.g., close the old stream.
            self.subscribers[agent_id] = context
            logger.info(f"Агент {agent_id} добавлен в список подписчиков.")

        event_count = 0
        try:
            # Send a confirmation or initial event if desired
            # await context.write(swm_service_pb2.SWMMemoryEvent(details=f"Successfully subscribed agent {agent_id}"))

            while context.is_active():
                # This loop now primarily keeps the stream alive.
                # Actual event sending is done by _notify_subscribers.
                # We can send periodic heartbeats here if needed.
                await asyncio.sleep(30) # Keep connection alive, or send heartbeats

                # Example Heartbeat (optional)
                # logger.debug(f"Heartbeat check for subscriber {agent_id}...")
                # evt_time = Timestamp(); evt_time.GetCurrentTime()
                # fake_kem = kem_pb2.KEM(id=f"heartbeat_for_{agent_id}", metadata={"type": "heartbeat"})
                # event = swm_service_pb2.SWMMemoryEvent(
                #     event_id=str(uuid.uuid4()),
                #     event_type=swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED, # Or a dedicated HEARTBEAT type
                #     kem_payload=fake_kem, event_time=evt_time, details="Heartbeat"
                # )
                # await context.write(event)
                # logger.debug(f"Sent heartbeat to {agent_id}")

                event_count +=1 # Just to show activity if not sending heartbeats

        except grpc.aio.AioRpcError as e: # Catch AioRpcError for stream issues like client disconnect
            logger.warning(f"Ошибка gRPC stream для подписчика {agent_id}: {e.code()} - {e.details()}")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка в SubscribeToSWMEvents для {agent_id}: {e}", exc_info=True)
        finally:
            logger.info(f"SWM: Стрим для подписчика {agent_id} завершен. Удаление из списка подписчиков.")
            async with self.subscribers_lock:
                self.subscribers.pop(agent_id, None)
            logger.info(f"Агент {agent_id} удален из списка подписчиков.")


    def __del__(self): # __del__ cannot be async
        if self.glm_channel:
            self.glm_channel.close()
                fake_kem = kem_pb2.KEM(id=f"heartbeat_kem_{event_count}_{agent_id}", metadata={"subscriber": agent_id})
                event = swm_service_pb2.SWMMemoryEvent(
                    event_id=str(uuid.uuid4()),
                    event_type=swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED,
                    kem_payload=fake_kem, event_time=evt_time, details="Heartbeat/Test Event"
                )
                # In grpc.aio, you yield directly to send stream responses.
                yield event
                event_count += 1
                logger.debug(f"Отправлено тестовое событие {event_count} подписчику {agent_id}")
        except grpc.aio.AioRpcError as e: # Catch AioRpcError for stream issues like client disconnect
            logger.warning(f"Ошибка gRPC stream для подписчика {agent_id}: {e.code()} - {e.details()}")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка в SubscribeToSWMEvents для {agent_id}: {e}", exc_info=True)
        finally:
            logger.info(f"SWM: Стрим для подписчика {agent_id} завершен.")


    def __del__(self): # __del__ cannot be async
        if self.glm_channel:
            self.glm_channel.close()
            logger.info("Канал GLM клиента в SWM закрыт.")

async def serve_async(): # Renamed to serve_async and made async
    # server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) # Old synchronous server
    server = grpc.aio.server() # New asynchronous server from grpc.aio

    swm_service_pb2_grpc.add_SharedWorkingMemoryServiceServicer_to_server(
        SharedWorkingMemoryServiceImpl(), server
    )
    server.add_insecure_port(SWM_GRPC_LISTEN_ADDRESS_CONFIG)

    logger.info(f"Запуск SWM (Shared Working Memory Service) асинхронно на {SWM_GRPC_LISTEN_ADDRESS_CONFIG}...")
    await server.start() # await server.start()
    logger.info(f"SWM (асинхронный) запущен и ожидает соединений на {SWM_GRPC_LISTEN_ADDRESS_CONFIG}.")

    try:
        await server.wait_for_termination() # await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Остановка SWM (асинхронного)...")
    finally:
        await server.stop(None) # await server.stop(grace_period_seconds)
        logger.info("SWM (асинхронный) остановлен.")

if __name__ == '__main__':
    # asyncio.run(serve()) # Run the async function
    logging.basicConfig(level=logging.INFO) # Ensure logging is configured if not already
    try:
        asyncio.run(serve_async())
    except Exception as e:
        logger.critical(f"SWM (асинхронный) не удалось запустить: {e}", exc_info=True)
