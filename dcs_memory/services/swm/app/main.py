import grpc
from concurrent import futures
import time
import sys
import os
import uuid
import logging
import threading
import queue # Для очередей событий подписчиков
from cachetools import LRUCache, Cache
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
SWM_INDEXED_METADATA_KEYS_CONFIG = [key.strip() for key in os.getenv("SWM_INDEXED_METADATA_KEYS", "").split(',') if key.strip()]
# --- Конец конфигурации ---

# Класс IndexedLRUCache должен быть определен до его использования
class IndexedLRUCache(Cache): # Наследуемся от Cache для type hinting, но реализуем свой интерфейс поверх LRUCache
    def __init__(self, maxsize, indexed_keys: typing.List[str], on_evict_callback: typing.Optional[typing.Callable[[kem_pb2.KEM], None]] = None):
        super().__init__(maxsize)
        self._lru = LRUCache(maxsize=maxsize)
        self._indexed_keys = set(indexed_keys)
        self._metadata_indexes: typing.Dict[str, typing.Dict[str, typing.Set[str]]] = {key: {} for key in self._indexed_keys}
        self._lock = threading.Lock()
        self._on_evict_callback = on_evict_callback

    def _add_to_metadata_indexes(self, kem: kem_pb2.KEM):
        if not kem or not kem.id: return
        for meta_key in self._indexed_keys:
            if meta_key in kem.metadata:
                value = kem.metadata[meta_key]
                self._metadata_indexes.setdefault(meta_key, {}).setdefault(value, set()).add(kem.id)

    def _remove_from_metadata_indexes(self, kem: kem_pb2.KEM):
        if not kem or not kem.id: return
        for meta_key in self._indexed_keys:
            if meta_key in kem.metadata: # Проверяем, было ли это поле у удаляемой КЕП
                original_value = kem.metadata[meta_key]
                if meta_key in self._metadata_indexes and original_value in self._metadata_indexes[meta_key]:
                    self._metadata_indexes[meta_key][original_value].discard(kem.id)
                    if not self._metadata_indexes[meta_key][original_value]:
                        del self._metadata_indexes[meta_key][original_value]
                    if not self._metadata_indexes[meta_key]:
                        del self._metadata_indexes[meta_key]

    def __setitem__(self, kem_id: str, kem: kem_pb2.KEM):
        with self._lock:
            evicted_kem = None
            if kem_id in self._lru: # Обновление существующего элемента
                old_kem = self._lru[kem_id] # Получаем старую версию для удаления из индекса
                self._remove_from_metadata_indexes(old_kem)
            elif len(self._lru) >= self._lru.maxsize: # Добавление нового, кэш полон, будет вытеснение
                # popitem() без аргументов удаляет и возвращает "самый старый" (LRU) элемент
                _evicted_id, evicted_kem = self._lru.popitem()
                if evicted_kem:
                    self._remove_from_metadata_indexes(evicted_kem)
                    if self._on_evict_callback:
                        try:
                            self._on_evict_callback(evicted_kem)
                        except Exception as e_cb: # Ловим ошибки из колбэка, чтобы не сломать кэш
                            logger.error(f"Ошибка в on_evict_callback для КЕП ID '{_evicted_id}': {e_cb}", exc_info=True)

            self._lru[kem_id] = kem
            self._add_to_metadata_indexes(kem)

    def __getitem__(self, kem_id: str) -> kem_pb2.KEM:
        with self._lock:
            return self._lru[kem_id] # LRUCache обновит порядок

    def __delitem__(self, kem_id: str):
        with self._lock:
            if kem_id in self._lru:
                kem_to_remove = self._lru.pop(kem_id) # Используем pop, чтобы он вернул значение
                self._remove_from_metadata_indexes(kem_to_remove)
            else: # Если ключа нет, вызываем KeyError, как и стандартный del
                raise KeyError(kem_id)

    def get(self, kem_id: str, default=None) -> typing.Optional[kem_pb2.KEM]:
        with self._lock:
            # Используем get от LRUCache, чтобы он обновил порядок использования
            return self._lru.get(kem_id, default)

    def pop(self, kem_id: str, default=object()) -> kem_pb2.KEM: # object() как уникальный маркер отсутствия
        with self._lock:
            if kem_id in self._lru:
                kem = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem)
                return kem
            elif default is not object():
                return default
            else:
                raise KeyError(kem_id)

    def __len__(self) -> int:
        with self._lock:
            return len(self._lru)

    def __contains__(self, kem_id: str) -> bool:
        with self._lock:
            return kem_id in self._lru

    def values(self) -> typing.ValuesView[kem_pb2.KEM]:
        # Возвращаем ValuesView для консистентности с dict, но это будет копия из-за блокировки
        # или нужно обеспечить потокобезопасный итератор, что сложнее.
        # Проще вернуть копию списка, как было в QuerySWM.
        with self._lock:
            return list(self._lru.values())

    def items(self) -> typing.ItemsView[str, kem_pb2.KEM]:
        with self._lock:
            return list(self._lru.items()) # Аналогично, возвращаем копию списка пар

    def clear(self):
        with self._lock:
            self._lru.clear()
            self._metadata_indexes.clear()
            self._metadata_indexes = {key: {} for key in self._indexed_keys}

    @property
    def maxsize(self):
        return self._lru.maxsize

    def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Optional[typing.Set[str]]:
        """ Возвращает множество ID КЕП, соответствующих фильтру по метаданным, или None, если ключ не индексируется. """
        if meta_key not in self._indexed_keys:
            return None # Этот ключ не индексируется, фильтрация должна быть полной
        with self._lock:
            return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()


class SharedWorkingMemoryServiceImpl(swm_service_pb2_grpc.SharedWorkingMemoryServiceServicer):
    def __init__(self):
        logger.info(f"Инициализация SharedWorkingMemoryServiceImpl... Размер кэша: {SWM_INTERNAL_CACHE_MAX_SIZE}, Индексируемые ключи метаданных: {SWM_INDEXED_METADATA_KEYS_CONFIG}")
        self.glm_channel = None
        self.glm_stub = None

        # Pub/Sub атрибуты
        # Ключ - subscriber_id, Значение - queue.Queue для этого подписчика
        self.subscribers: typing.Dict[str, queue.Queue] = {}
        self.subscribers_lock = threading.Lock()

        # Используем новый IndexedLRUCache с колбэком на вытеснение
        self.swm_cache = IndexedLRUCache(
            maxsize=SWM_INTERNAL_CACHE_MAX_SIZE,
            indexed_keys=SWM_INDEXED_METADATA_KEYS_CONFIG,
            on_evict_callback=self._handle_kem_eviction
        )

        try:
            self.glm_channel = grpc.insecure_channel(GLM_SERVICE_ADDRESS_CONFIG)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.glm_channel)
            logger.info(f"GLM клиент для SWM инициализирован, целевой адрес: {GLM_SERVICE_ADDRESS_CONFIG}")
        except Exception as e:
            logger.error(f"Ошибка при инициализации GLM клиента в SWM: {e}")

    def _handle_kem_eviction(self, evicted_kem: kem_pb2.KEM):
        """Колбэк, вызываемый IndexedLRUCache при вытеснении элемента."""
        if evicted_kem:
            logger.info(f"SWM: КЕП ID '{evicted_kem.id}' была вытеснена из кэша. Уведомление подписчиков.")
            self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED, source_agent_id="SWM_CACHE_EVICTION")

    def _get_kem_from_cache(self, kem_id: str) -> typing.Optional[kem_pb2.KEM]:
        # self.cache_lock не нужен, IndexedLRUCache потокобезопасен
        return self.swm_cache.get(kem_id)

    def _put_kem_to_cache(self, kem: kem_pb2.KEM) -> None:
        if not kem or not kem.id:
            logger.warning("Попытка добавить невалидную КЕП в кэш SWM.")
            return
        # self.cache_lock не нужен
        # Важно: нужно проверить, был ли элемент действительно новым для _notify_subscribers
        # Это можно сделать, проверив наличие ключа до вызова __setitem__ или сравнив старое и новое значение,
        # но IndexedLRUCache.__setitem__ не возвращает информацию о том, был ли это insert или update.
        # Простой способ:
        was_present = kem.id in self.swm_cache
        self.swm_cache[kem.id] = kem # IndexedLRUCache управляет индексами и LRU

        logger.info("КЕП ID '{}' добавлена/обновлена в кэше SWM. Размер кэша: {}/{}".format(
            kem.id, len(self.swm_cache), self.swm_cache.maxsize))

        # Логика для _notify_subscribers должна учитывать, был ли элемент новым или обновлен.
        # Если self.swm_cache[kem.id] = kem вызывает обновление, то это KEM_UPDATED.
        # Если это новая вставка, то KEM_PUBLISHED.
        # IndexedLRUCache не дает этой информации напрямую после __setitem__.
        # Мы можем проверить до __setitem__ был ли ключ.
        event_type = swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED if was_present else swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED
        self._notify_subscribers(kem, event_type)


    def _delete_kem_from_cache(self, kem_id: str) -> bool:
        # self.cache_lock не нужен
        if kem_id in self.swm_cache:
            # pop вернет КЕП, если он был, или вызовет KeyError/вернет default
            # Нам нужна сама КЕП для _notify_subscribers
            try:
                evicted_kem = self.swm_cache.pop(kem_id) # pop из IndexedLRUCache
                logger.info(f"КЕП ID '{kem_id}' удалена из кэша SWM.")
                if evicted_kem: # Должно быть всегда True, если не было ошибки
                    self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED)
                return True
            except KeyError: # Если между 'in' и 'pop' элемент исчез (маловероятно с блокировкой внутри pop)
                logger.warning(f"КЕП ID '{kem_id}' не найдена в кэше при попытке удаления (возможно, гонка).")
                return False
        return False

    def _notify_subscribers(self, kem: kem_pb2.KEM, event_type: swm_service_pb2.SWMMemoryEvent.EventType, source_agent_id: str = "SWM_SERVER"):
        # Этот метод теперь будет класть события в очереди подписчиков
        # Фильтрация по темам/критериям будет добавлена позже
        if not kem or not kem.id: # Доп. проверка на валидность КЕП
            logger.warning("_notify_subscribers вызван с невалидной КЕП.")
            return

        logger.info(f"Формирование события {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} для КЕП ID '{kem.id}'")

        event_time_proto = Timestamp(); event_time_proto.GetCurrentTime()
        event = swm_service_pb2.SWMMemoryEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            kem_payload=kem,
            event_time=event_time_proto,
            source_agent_id=source_agent_id,
            details=f"Event {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} for KEM ID {kem.id}"
        )

        subscribers_to_notify: typing.List[queue.Queue] = []
        with self.subscribers_lock:
            if not self.subscribers:
                return # Нет активных подписчиков
            # Собираем очереди тех, кого нужно уведомить (пока всех)
            subscribers_to_notify = list(self.subscribers.values())

        if not subscribers_to_notify:
            logger.debug("Нет подписчиков для уведомления.")
            return

        logger.debug(f"Рассылка события {event.event_id} для {len(subscribers_to_notify)} подписчиков.")
        for q in subscribers_to_notify:
            try:
                q.put_nowait(event) # Используем nowait, чтобы не блокировать этот поток.
                                    # Если очередь подписчика переполнена (если она ограничена), это вызовет queue.Full.
                                    # Это означает, что подписчик не успевает обрабатывать события.
            except queue.Full:
                logger.warning(f"Очередь подписчика переполнена для события {event.event_id}. Событие может быть потеряно для этого подписчика.")
            except Exception as e: # Общая защита
                logger.error(f"Неожиданная ошибка при добавлении события в очередь подписчика: {e}", exc_info=True)

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

        processed_kems: typing.List[kem_pb2.KEM] = []

        # Шаг 1: Применение фильтров по индексированным метаданным
        # final_candidate_ids будет содержать ID, если индексные фильтры были успешно применены.
        # Иначе, если индексные фильтры не применялись или не дали сужения, он останется None.
        final_candidate_ids: typing.Optional[typing.Set[str]] = None

        # Определяем, какие фильтры метаданных являются индексированными, а какие нет
        indexed_metadata_queries: typing.Dict[str, str] = {}
        unindexed_metadata_queries: typing.Dict[str, str] = {}

        if query.metadata_filters:
            for key, value in query.metadata_filters.items():
                if key in self.swm_cache._indexed_keys:
                    indexed_metadata_queries[key] = value
                else:
                    unindexed_metadata_queries[key] = value

        if indexed_metadata_queries:
            logger.debug(f"Применение индексированных фильтров метаданных: {indexed_metadata_queries}")
            intersected_ids: typing.Set[str] = set() # Начинаем с пустого, чтобы правильно обработать первый фильтр

            first_indexed_filter = True
            for key, value in indexed_metadata_queries.items():
                ids_from_index = self.swm_cache.get_ids_by_metadata_filter(key, value)
                # get_ids_by_metadata_filter возвращает set(), даже если пустой, или None если ключ не индексируется (здесь это уже проверено)

                if first_indexed_filter:
                    intersected_ids = ids_from_index
                    first_indexed_filter = False
                else:
                    intersected_ids.intersection_update(ids_from_index)

                if not intersected_ids: break # Если пересечение уже пустое

            if not intersected_ids:
                logger.info("QuerySWM: Результат пуст после применения индексированных фильтров метаданных.")
                return swm_service_pb2.QuerySWMResponse(kems=[], next_page_token="")
            final_candidate_ids = intersected_ids
            logger.debug(f"Кандидатские ID после индексированных фильтров: {len(final_candidate_ids)}")

        # Шаг 2: Получить КЕПы для дальнейшей обработки
        # Инициализируем processed_kems как пустой список здесь
        processed_kems: typing.List[kem_pb2.KEM] = []
        if final_candidate_ids is not None: # Если были применены индексные фильтры
            # Извлекаем только те КЕПы, ID которых прошли индексную фильтрацию
            for kem_id in final_candidate_ids:
                kem = self.swm_cache.get(kem_id) # Используем get для обновления LRU
                if kem: processed_kems.append(kem)
            logger.debug(f"Извлечено {len(processed_kems)} КЕП по ID из индексного поиска для дальнейшей фильтрации.")
        else:
            # Индексные фильтры не использовались, берем все из кэша
            # self.swm_cache.values() возвращает list из-за реализации IndexedLRUCache
            processed_kems = self.swm_cache.values()
            logger.debug(f"Индексные фильтры не применялись. Взято {len(processed_kems)} КЕП из кэша для фильтрации.")

        # Шаг 3: Применить фильтр по IDs из запроса (query.ids)
        # Этот фильтр применяется ко всем КЕПам, отобранным на предыдущих шагах (либо все из кэша, либо по индексным фильтрам)
        if query.ids:
            ids_set = set(query.ids)
            processed_kems = [kem for kem in processed_kems if kem.id in ids_set]
            logger.debug(f"КЕП после фильтрации по query.ids: {len(processed_kems)}")

        # Шаг 4: Применить неиндексированные фильтры метаданных
        if unindexed_metadata_queries:
            logger.debug(f"Применение неиндексированных фильтров метаданных: {unindexed_metadata_queries}")

            temp_filtered_kems = []
            for kem in processed_kems:
                match = True
                for key, value in unindexed_metadata_queries.items():
                    if kem.metadata.get(key) != value:
                        match = False; break
                if match: temp_filtered_kems.append(kem)
            processed_kems = temp_filtered_kems
            logger.debug(f"КЕП после неиндексированных фильтров метаданных: {len(processed_kems)}")

        # Шаг 5: Фильтры по датам
        def check_date_filter(kem_ts_field, filter_start_ts, filter_end_ts) -> bool:
            match = True
            if filter_start_ts and (filter_start_ts.seconds > 0 or filter_start_ts.nanos > 0):
                if kem_ts_field.ToNanoseconds() < filter_start_ts.ToNanoseconds(): match = False
            if match and filter_end_ts and (filter_end_ts.seconds > 0 or filter_end_ts.nanos > 0):
                if kem_ts_field.ToNanoseconds() > filter_end_ts.ToNanoseconds(): match = False
            return match

        if query.HasField("created_at_start") or query.HasField("created_at_end"):
            processed_kems = [k for k in processed_kems if check_date_filter(k.created_at, query.created_at_start, query.created_at_end)]

        if query.HasField("updated_at_start") or query.HasField("updated_at_end"):
            processed_kems = [k for k in processed_kems if check_date_filter(k.updated_at, query.updated_at_start, query.updated_at_end)]

        start_index = offset
        end_index = offset + page_size
        kems_on_page = processed_kems[start_index:end_index]

        next_page_token_str = ""
        if end_index < len(processed_kems):
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

        subscriber_id = agent_id if agent_id else str(uuid.uuid4())
        event_queue = queue.Queue(maxsize=100) # Ограничим размер очереди, чтобы избежать утечек памяти

        with self.subscribers_lock:
            if subscriber_id in self.subscribers:
                # Можно либо отклонять, либо разрешать несколько подписок от одного agent_id,
                # но тогда subscriber_id должен быть уникальным для каждой подписки.
                # Пока что, если agent_id уже есть, будем считать это ошибкой или переподпиской.
                # Для простоты, новый подписчик с тем же ID заменит старого.
                logger.warning(f"Подписчик с ID '{subscriber_id}' уже существует. Старая подписка будет заменена.")
            self.subscribers[subscriber_id] = event_queue
            logger.info(f"SWM: Новый подписчик '{subscriber_id}' зарегистрирован. Всего подписчиков: {len(self.subscribers)}")

        try:
            # Можно отправить начальное "подтверждение подписки" событие
            # welcome_event = swm_service_pb2.SWMMemoryEvent(...)
            # yield welcome_event

            while context.is_active():
                try:
                    event_to_send = event_queue.get(timeout=1.0) # Таймаут, чтобы периодически проверять context.is_active()
                    yield event_to_send
                    # event_queue.task_done() # Если бы это была JoinableQueue
                except queue.Empty:
                    # Таймаут, просто продолжаем цикл, чтобы проверить context.is_active()
                    continue
                except grpc.RpcError as rpc_error: # Например, если клиент отсоединился
                    logger.info(f"SWM: RPC ошибка для подписчика '{subscriber_id}': {rpc_error.code()} - {rpc_error.details()}. Завершение стрима.")
                    break
                except Exception as e: # Другие неожиданные ошибки
                    logger.error(f"SWM: Ошибка в стриме для подписчика '{subscriber_id}': {e}", exc_info=True)
                    context.abort(grpc.StatusCode.INTERNAL, f"Ошибка стрима событий: {e}")
                    break
        finally:
            with self.subscribers_lock:
                removed_q = self.subscribers.pop(subscriber_id, None)
                if removed_q:
                    logger.info(f"SWM: Подписчик '{subscriber_id}' удален. Осталось подписчиков: {len(self.subscribers)}")
                else:
                    logger.warning(f"SWM: Попытка удалить несуществующего подписчика '{subscriber_id}'.")
            # Очистить очередь, если она не пуста, чтобы освободить ресурсы (если события сложные)
            if removed_q:
                while not removed_q.empty():
                    try: removed_q.get_nowait()
                    except queue.Empty: break


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
