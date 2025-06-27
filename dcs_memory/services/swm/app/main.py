import grpc
from concurrent import futures
import time
import sys
import os
import uuid
import logging
import threading
import queue # For subscriber event queues
from dataclasses import dataclass, field # For SubscriberInfo
from cachetools import LRUCache, Cache
from google.protobuf.timestamp_pb2 import Timestamp
import typing

# Import configuration
from .config import SWMConfig

# Global config instance
config = SWMConfig()

# --- Logging Setup ---
logging.basicConfig(
    level=config.get_log_level_int(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
# --- End Logging Setup ---

# --- gRPC Code Import Block ---
current_script_path = os.path.abspath(__file__)
app_dir_swm = os.path.dirname(current_script_path)
service_root_dir_swm = os.path.dirname(app_dir_swm)

if service_root_dir_swm not in sys.path:
    sys.path.insert(0, service_root_dir_swm)

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2
from generated_grpc import glm_service_pb2_grpc
from generated_grpc import swm_service_pb2
from generated_grpc import swm_service_pb2_grpc
from dcs_memory.common.grpc_utils import retry_grpc_call
# --- End gRPC Code Import Block ---

@dataclass
class SubscriberInfo:
    event_queue: queue.Queue
    topics: typing.List[swm_service_pb2.SubscriptionTopic] = field(default_factory=list)

class IndexedLRUCache(Cache):
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
            if meta_key in kem.metadata:
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
            if kem_id in self._lru:
                old_kem = self._lru[kem_id]
                self._remove_from_metadata_indexes(old_kem)
            elif len(self._lru) >= self._lru.maxsize:
                _evicted_id, evicted_kem = self._lru.popitem()
                if evicted_kem:
                    self._remove_from_metadata_indexes(evicted_kem)
                    if self._on_evict_callback:
                        try:
                            self._on_evict_callback(evicted_kem)
                        except Exception as e_cb:
                            logger.error(f"Error in on_evict_callback for KEM ID '{_evicted_id}': {e_cb}", exc_info=True)
            self._lru[kem_id] = kem
            self._add_to_metadata_indexes(kem)
    def __getitem__(self, kem_id: str) -> kem_pb2.KEM:
        with self._lock:
            return self._lru[kem_id]
    def __delitem__(self, kem_id: str):
        with self._lock:
            if kem_id in self._lru:
                kem_to_remove = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem_to_remove)
            else:
                raise KeyError(kem_id)
    def get(self, kem_id: str, default=None) -> typing.Optional[kem_pb2.KEM]:
        with self._lock:
            return self._lru.get(kem_id, default)
    def pop(self, kem_id: str, default=object()) -> kem_pb2.KEM:
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
        with self._lock:
            return list(self._lru.values())
    def items(self) -> typing.ItemsView[str, kem_pb2.KEM]:
        with self._lock:
            return list(self._lru.items())
    def clear(self):
        with self._lock:
            self._lru.clear()
            self._metadata_indexes.clear()
            self._metadata_indexes = {key: {} for key in self._indexed_keys}
    @property
    def maxsize(self):
        return self._lru.maxsize
    def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Optional[typing.Set[str]]:
        if meta_key not in self._indexed_keys:
            return None
        with self._lock:
            return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()

@dataclass
class LockInfoInternal:
    resource_id: str
    agent_id: str
    lock_id: str
    acquired_at_unix_ms: int
    lease_duration_ms: int
    lease_expires_at_unix_ms: int

class SharedWorkingMemoryServiceImpl(swm_service_pb2_grpc.SharedWorkingMemoryServiceServicer):
    def __init__(self):
        self.config = config
        logger.info(f"Initializing SharedWorkingMemoryServiceImpl... Cache size: {self.config.CACHE_MAX_SIZE}, Indexed keys: {self.config.INDEXED_METADATA_KEYS}")
        self.glm_channel = None
        self.glm_stub = None
        self.retry_max_attempts = self.config.GLM_RETRY_MAX_ATTEMPTS
        self.retry_initial_delay_s = self.config.GLM_RETRY_INITIAL_DELAY_S
        self.retry_backoff_factor = self.config.GLM_RETRY_BACKOFF_FACTOR
        self.subscribers: typing.Dict[str, SubscriberInfo] = {}
        self.subscribers_lock = threading.Lock()
        self.swm_cache = IndexedLRUCache(
            maxsize=self.config.CACHE_MAX_SIZE,
            indexed_keys=self.config.INDEXED_METADATA_KEYS,
            on_evict_callback=self._handle_kem_eviction
        )
        self.locks: typing.Dict[str, LockInfoInternal] = {}
        self.locks_lock = threading.Lock()
        self.lock_condition = threading.Condition(self.locks_lock)
        self.counters: typing.Dict[str, int] = {}
        self.counters_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._lock_cleanup_thread: typing.Optional[threading.Thread] = None
        self._lock_cleanup_interval_seconds = getattr(self.config, 'LOCK_CLEANUP_INTERVAL_S', 60)
        self._start_expired_lock_cleanup_thread()

        try:
            self.glm_channel = grpc.insecure_channel(self.config.GLM_SERVICE_ADDRESS)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.glm_channel)
            logger.info(f"GLM client for SWM initialized, target address: {self.config.GLM_SERVICE_ADDRESS}")
        except Exception as e:
            logger.error(f"Error initializing GLM client in SWM: {e}")

    def _expired_lock_cleanup_task(self):
        logger.info("SWM: Expired lock cleanup background thread started.")
        while not self._stop_event.wait(self._lock_cleanup_interval_seconds):
            logger.debug("SWM: Performing periodic cleanup of expired locks...")
            with self.lock_condition:
                current_time_ms = int(time.time() * 1000)
                expired_resource_ids = []
                for resource_id, lock_info in list(self.locks.items()):
                    if lock_info.lease_duration_ms > 0 and current_time_ms >= lock_info.lease_expires_at_unix_ms:
                        expired_resource_ids.append(resource_id)

                if expired_resource_ids:
                    cleaned_count = 0
                    for resource_id in expired_resource_ids:
                        lock_to_remove = self.locks.get(resource_id)
                        if lock_to_remove and lock_to_remove.lease_duration_ms > 0 and \
                           current_time_ms >= lock_to_remove.lease_expires_at_unix_ms:
                            del self.locks[resource_id]
                            logger.info(f"SWM Cleanup: Expired lock for resource='{lock_to_remove.resource_id}' (agent '{lock_to_remove.agent_id}') removed.")
                            self.lock_condition.notify_all()
                            cleaned_count +=1
                    if cleaned_count > 0:
                        logger.info(f"SWM Cleanup: Removed {cleaned_count} expired locks.")
                else:
                    logger.debug("SWM Cleanup: No expired locks found.")
        logger.info("SWM: Expired lock cleanup background thread stopped.")

    def _start_expired_lock_cleanup_thread(self):
        if self._lock_cleanup_thread is None or not self._lock_cleanup_thread.is_alive():
            self._stop_event.clear()
            self._lock_cleanup_thread = threading.Thread(target=self._expired_lock_cleanup_task, daemon=True)
            self._lock_cleanup_thread.start()
            logger.info("SWM: Lock cleanup background thread successfully started/restarted.")
        else:
            logger.info("SWM: Lock cleanup background thread is already active.")

    def _stop_expired_lock_cleanup_thread(self):
        if self._lock_cleanup_thread and self._lock_cleanup_thread.is_alive():
            logger.info("SWM: Stopping lock cleanup background thread...")
            self._stop_event.set()
        self._lock_cleanup_thread = None

    @retry_grpc_call
    def _glm_retrieve_kems_with_retry(self, request: glm_service_pb2.RetrieveKEMsRequest, timeout: int = 20) -> glm_service_pb2.RetrieveKEMsResponse:
        if not self.glm_stub:
            logger.error("SWM._glm_retrieve_kems_with_retry: GLM stub not initialized.")
            raise grpc.RpcError("GLM stub not available in SWM")
        return self.glm_stub.RetrieveKEMs(request, timeout=timeout)

    @retry_grpc_call
    def _glm_store_kem_with_retry(self, request: glm_service_pb2.StoreKEMRequest, timeout: int = 10) -> glm_service_pb2.StoreKEMResponse:
        if not self.glm_stub:
            logger.error("SWM._glm_store_kem_with_retry: GLM stub not initialized.")
            raise grpc.RpcError("GLM stub not available in SWM")
        return self.glm_stub.StoreKEM(request, timeout=timeout)

    def _handle_kem_eviction(self, evicted_kem: kem_pb2.KEM):
        if evicted_kem:
            logger.info(f"SWM: KEM ID '{evicted_kem.id}' was evicted from cache. Notifying subscribers.")
            self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED, source_agent_id="SWM_CACHE_EVICTION")

    def _get_kem_from_cache(self, kem_id: str) -> typing.Optional[kem_pb2.KEM]:
        return self.swm_cache.get(kem_id)

    def _put_kem_to_cache(self, kem: kem_pb2.KEM) -> None:
        if not kem or not kem.id:
            logger.warning("Attempt to add an invalid KEM to SWM cache.")
            return
        was_present = kem.id in self.swm_cache
        self.swm_cache[kem.id] = kem
        logger.info("KEM ID '{}' added/updated in SWM cache. Cache size: {}/{}".format(
            kem.id, len(self.swm_cache), self.swm_cache.maxsize))
        event_type = swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED if was_present else swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED
        self._notify_subscribers(kem, event_type)

    def _delete_kem_from_cache(self, kem_id: str) -> bool:
        if kem_id in self.swm_cache:
            try:
                evicted_kem = self.swm_cache.pop(kem_id)
                logger.info(f"KEM ID '{kem_id}' removed from SWM cache.")
                if evicted_kem:
                    self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED)
                return True
            except KeyError:
                logger.warning(f"KEM ID '{kem_id}' not found in cache during deletion attempt (possible race condition).")
                return False
        return False

    def _notify_subscribers(self, kem: kem_pb2.KEM, event_type: swm_service_pb2.SWMMemoryEvent.EventType, source_agent_id: str = "SWM_SERVER"):
        if not kem or not kem.id:
            logger.warning("_notify_subscribers called with an invalid KEM.")
            return
        logger.info(f"Forming event {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} for KEM ID '{kem.id}'")
        event_time_proto = Timestamp(); event_time_proto.GetCurrentTime()
        event_to_dispatch = swm_service_pb2.SWMMemoryEvent(
            event_id=str(uuid.uuid4()), event_type=event_type, kem_payload=kem,
            event_time=event_time_proto, source_agent_id=source_agent_id,
            details=f"Event {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} for KEM ID {kem.id}"
        )
        subscribers_copy: typing.Dict[str, SubscriberInfo] = {}
        with self.subscribers_lock:
            if not self.subscribers: return
            subscribers_copy = dict(self.subscribers)
        if not subscribers_copy:
            logger.debug("No subscribers to notify.")
            return
        logger.debug(f"Checking and dispatching event {event_to_dispatch.event_id} to {len(subscribers_copy)} subscribers.")
        for sub_id, sub_info in subscribers_copy.items():
            should_send = False
            if not sub_info.topics: should_send = True
            else:
                for topic in sub_info.topics:
                    criteria = topic.filter_criteria.strip()
                    if not criteria: should_send = True; break
                    if '=' not in criteria:
                        logger.warning(f"Invalid filter_criteria format '{criteria}' for subscriber {sub_id}. Skipping filter.")
                        should_send = True; break
                    filter_key, filter_value = criteria.split("=", 1)
                    filter_key = filter_key.strip(); filter_value = filter_value.strip()
                    if filter_key == "kem_id":
                        if event_to_dispatch.kem_payload.id == filter_value: should_send = True; break
                    elif filter_key.startswith("metadata."):
                        meta_actual_key = filter_key.split("metadata.", 1)[1]
                        if meta_actual_key in event_to_dispatch.kem_payload.metadata and \
                           event_to_dispatch.kem_payload.metadata[meta_actual_key] == filter_value:
                            should_send = True; break
                    else:
                        logger.warning(f"Unknown filter key '{filter_key}' for subscriber {sub_id}. Event will be sent.")
                        should_send = True; break
            if should_send:
                try: sub_info.event_queue.put_nowait(event_to_dispatch)
                except queue.Full: logger.warning(f"Subscriber queue {sub_id} is full for event {event_to_dispatch.event_id}. Event lost.")
                except Exception as e: logger.error(f"Unexpected error adding event to subscriber queue {sub_id}: {e}", exc_info=True)

    def LoadKEMsFromGLM(self, request: swm_service_pb2.LoadKEMsFromGLMRequest, context):
        logger.info(f"SWM: LoadKEMsFromGLM called with query: {request.query_for_glm}")
        if not self.glm_stub:
            msg = "GLM service is unavailable to SWM (client not initialized)."
            logger.error(msg); context.abort(grpc.StatusCode.INTERNAL, msg)
            return swm_service_pb2.LoadKEMsFromGLMResponse(status_message=msg)
        glm_retrieve_request = glm_service_pb2.RetrieveKEMsRequest(query=request.query_for_glm)
        loaded_kems_count = 0; loaded_ids = []; kems_from_glm_for_stats = 0
        try:
            logger.info(f"SWM: Requesting GLM.RetrieveKEMs (with retry): {glm_retrieve_request}")
            glm_response = self._glm_retrieve_kems_with_retry(glm_retrieve_request, timeout=20)
            if glm_response and glm_response.kems:
                kems_from_glm_for_stats = len(glm_response.kems)
                for kem_from_glm in glm_response.kems:
                    self._put_kem_to_cache(kem_from_glm)
                    loaded_ids.append(kem_from_glm.id)
                loaded_kems_count = len(loaded_ids)
                status_msg = f"Loaded {loaded_kems_count} KEMs."
                logger.info(f"SWM: {status_msg}")
            else:
                status_msg = "GLM returned no KEMs for the query."; logger.info(status_msg)
            return swm_service_pb2.LoadKEMsFromGLMResponse(
                kems_queried_in_glm_count=kems_from_glm_for_stats, kems_loaded_to_swm_count=loaded_kems_count,
                loaded_kem_ids=loaded_ids, status_message=status_msg )
        except grpc.RpcError as e:
            msg = f"SWM: gRPC error calling GLM.RetrieveKEMs: code={e.code()}, details={e.details()}"
            logger.error(msg); context.abort(e.code(), msg)
        except Exception as e:
            msg = f"SWM: Unexpected error in LoadKEMsFromGLM while working with GLM: {e}"
            logger.error(msg, exc_info=True); context.abort(grpc.StatusCode.INTERNAL, msg)
        return swm_service_pb2.LoadKEMsFromGLMResponse(status_message="Error loading from GLM")


    def QuerySWM(self, request: swm_service_pb2.QuerySWMRequest, context):
        query = request.query
        logger.info(f"SWM: QuerySWM called with KEMQuery: {query}")
        if query.embedding_query or query.text_query:
            msg = "Vector or text search is not supported directly in SWM cache. Use GLM."
            logger.warning(msg); context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
            return swm_service_pb2.QuerySWMResponse()
        page_size = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
        offset = 0
        if request.page_token:
            try: offset = int(request.page_token)
            except ValueError: logger.warning(f"Invalid page_token for QuerySWM: '{request.page_token}', using offset=0.")

        processed_kems_list: typing.List[kem_pb2.KEM] = []
        final_candidate_ids: typing.Optional[typing.Set[str]] = None
        indexed_metadata_queries: typing.Dict[str, str] = {}
        unindexed_metadata_queries: typing.Dict[str, str] = {}

        if query.metadata_filters:
            for key, value in query.metadata_filters.items():
                if key in self.swm_cache._indexed_keys: indexed_metadata_queries[key] = value
                else: unindexed_metadata_queries[key] = value
        if indexed_metadata_queries:
            logger.debug(f"Applying indexed metadata filters: {indexed_metadata_queries}")
            intersected_ids: typing.Optional[typing.Set[str]] = None
            for key, value in indexed_metadata_queries.items():
                ids_from_index = self.swm_cache.get_ids_by_metadata_filter(key, value)
                if intersected_ids is None: intersected_ids = ids_from_index
                else: intersected_ids.intersection_update(ids_from_index if ids_from_index is not None else set())
                if not intersected_ids: break
            if not intersected_ids :
                logger.info("QuerySWM: Result is empty after applying indexed metadata filters.")
                return swm_service_pb2.QuerySWMResponse(kems=[], next_page_token="")
            final_candidate_ids = intersected_ids
            logger.debug(f"Candidate IDs after indexed filters: {len(final_candidate_ids if final_candidate_ids else [])}")

        if final_candidate_ids is not None:
            for kem_id in final_candidate_ids:
                kem = self.swm_cache.get(kem_id)
                if kem: processed_kems_list.append(kem)
        else:
            processed_kems_list = self.swm_cache.values()
        logger.debug(f"KEMs after indexed filter stage (or all from cache): {len(processed_kems_list)}")

        if query.ids:
            ids_set = set(query.ids)
            processed_kems_list = [kem for kem in processed_kems_list if kem.id in ids_set]
        if unindexed_metadata_queries:
            temp_filtered_kems = []
            for kem in processed_kems_list:
                match = True
                for key, value in unindexed_metadata_queries.items():
                    if kem.metadata.get(key) != value: match = False; break
                if match: temp_filtered_kems.append(kem)
            processed_kems_list = temp_filtered_kems
        def check_date_filter(kem_ts_field, filter_start_ts, filter_end_ts) -> bool:
            match = True
            if filter_start_ts and (filter_start_ts.seconds > 0 or filter_start_ts.nanos > 0):
                if kem_ts_field.ToNanoseconds() < filter_start_ts.ToNanoseconds(): match = False
            if match and filter_end_ts and (filter_end_ts.seconds > 0 or filter_end_ts.nanos > 0):
                if kem_ts_field.ToNanoseconds() > filter_end_ts.ToNanoseconds(): match = False
            return match
        if query.HasField("created_at_start") or query.HasField("created_at_end"):
            processed_kems_list = [k for k in processed_kems_list if check_date_filter(k.created_at, query.created_at_start, query.created_at_end)]
        if query.HasField("updated_at_start") or query.HasField("updated_at_end"):
            processed_kems_list = [k for k in processed_kems_list if check_date_filter(k.updated_at, query.updated_at_start, query.updated_at_end)]

        start_index = offset; end_index = offset + page_size
        kems_on_page = processed_kems_list[start_index:end_index]
        next_page_token_str = ""
        if end_index < len(processed_kems_list): next_page_token_str = str(end_index)
        logger.info(f"QuerySWM: Returning {len(kems_on_page)} KEMs.")
        return swm_service_pb2.QuerySWMResponse(kems=kems_on_page, next_page_token=next_page_token_str)

    def PublishKEMToSWM(self, request: swm_service_pb2.PublishKEMToSWMRequest, context):
        kem_to_publish = request.kem_to_publish
        logger.info(f"SWM: PublishKEMToSWM called for KEM ID (suggested): '{kem_to_publish.id}'")
        kem_id_final = kem_to_publish.id
        if not kem_id_final:
            kem_id_final = str(uuid.uuid4()); kem_to_publish.id = kem_id_final
            logger.info(f"SWM: No ID provided, new ID generated: '{kem_id_final}'")
        ts = Timestamp(); ts.GetCurrentTime()
        existing_kem_in_cache = self._get_kem_from_cache(kem_id_final)
        if existing_kem_in_cache and existing_kem_in_cache.HasField("created_at"):
            kem_to_publish.created_at.CopyFrom(existing_kem_in_cache.created_at)
        elif not kem_to_publish.HasField("created_at"): kem_to_publish.created_at.CopyFrom(ts)
        kem_to_publish.updated_at.CopyFrom(ts)
        self._put_kem_to_cache(kem_to_publish)
        published_to_swm_flag = True; persistence_triggered_flag = False
        status_msg = f"KEM ID '{kem_id_final}' successfully published to SWM."
        if request.persist_to_glm_if_new_or_updated:
            if not self.glm_stub:
                msg_glm = f"GLM service unavailable, KEM ID '{kem_id_final}' will not be persisted."
                logger.error(msg_glm); status_msg += " " + msg_glm
            else:
                try:
                    logger.info(f"SWM: Initiating persistence of KEM ID '{kem_id_final}' to GLM (with retry)...")
                    glm_store_req = glm_service_pb2.StoreKEMRequest(kem=kem_to_publish)
                    glm_store_resp = self._glm_store_kem_with_retry(glm_store_req, timeout=10)
                    if glm_store_resp and glm_store_resp.kem and glm_store_resp.kem.id:
                        self._put_kem_to_cache(glm_store_resp.kem)
                        kem_id_final = glm_store_resp.kem.id
                        status_msg += f" Successfully persisted/updated in GLM with ID '{kem_id_final}'."
                        persistence_triggered_flag = True
                    else:
                        msg_glm_err = f"GLM.StoreKEM did not return an expected response for KEM ID '{kem_id_final}'."
                        logger.error(msg_glm_err); status_msg += " " + msg_glm_err
                except Exception as e:
                    msg_glm_rpc_err = f"Error persisting KEM ID '{kem_id_final}' to GLM: {e}"
                    logger.error(msg_glm_rpc_err, exc_info=True); status_msg += " " + msg_glm_rpc_err
        return swm_service_pb2.PublishKEMToSWMResponse(
            kem_id=kem_id_final, published_to_swm=published_to_swm_flag,
            persistence_triggered_to_glm=persistence_triggered_flag, status_message=status_msg)

    def SubscribeToSWMEvents(self, request: swm_service_pb2.SubscribeToSWMEventsRequest, context):
        agent_id = request.agent_id
        logger.info(f"SWM: New subscriber {agent_id} for topics: {request.topics}")
        subscriber_id = agent_id if agent_id else str(uuid.uuid4())
        event_q = queue.Queue(maxsize=100)
        subscriber_info = SubscriberInfo(event_queue=event_q, topics=list(request.topics))
        with self.subscribers_lock:
            if subscriber_id in self.subscribers: logger.warning(f"Subscriber with ID '{subscriber_id}' already exists. Old subscription will be replaced.")
            self.subscribers[subscriber_id] = subscriber_info
            logger.info(f"SWM: New subscriber '{subscriber_id}' registered with {len(subscriber_info.topics)} topics/filters. Total subscribers: {len(self.subscribers)}")
        try:
            while context.is_active():
                try: yield event_q.get(timeout=1.0)
                except queue.Empty: continue
                except grpc.RpcError as rpc_error:
                    logger.info(f"SWM: RPC error for subscriber '{subscriber_id}': {rpc_error.code()} - {rpc_error.details()}. Terminating stream.")
                    break
                except Exception as e: logger.error(f"SWM: Error in event stream for subscriber '{subscriber_id}': {e}", exc_info=True); break
        finally:
            with self.subscribers_lock:
                removed_info = self.subscribers.pop(subscriber_id, None)
                if removed_info:
                    logger.info(f"SWM: Subscriber '{subscriber_id}' removed. Remaining subscribers: {len(self.subscribers)}")
                    while not removed_info.event_queue.empty():
                        try: removed_info.event_queue.get_nowait()
                        except queue.Empty: break
                else: logger.warning(f"SWM: Attempted to remove non-existent subscriber '{subscriber_id}'.")

    def __del__(self):
        logger.info("SWM: Shutting down SharedWorkingMemoryServiceImpl...")
        self._stop_expired_lock_cleanup_thread()
        if self.glm_channel:
            self.glm_channel.close()
            logger.info("GLM client channel in SWM closed.")

    # --- RPC Lock Implementations ---
    def AcquireLock(self, request: swm_service_pb2.AcquireLockRequest, context) -> swm_service_pb2.AcquireLockResponse:
        resource_id = request.resource_id; agent_id = request.agent_id
        timeout_ms = request.timeout_ms; lease_duration_ms = request.lease_duration_ms
        logger.info(f"SWM: AcquireLock request for resource='{resource_id}' by agent='{agent_id}', timeout={timeout_ms}ms, lease={lease_duration_ms}ms")
        start_time_monotonic = time.monotonic()
        with self.lock_condition:
            while True:
                current_time_ms = int(time.time() * 1000)
                existing_lock = self.locks.get(resource_id)
                if existing_lock and existing_lock.lease_duration_ms > 0 and \
                   current_time_ms >= existing_lock.lease_expires_at_unix_ms:
                    logger.info(f"SWM: Existing lock for resource='{resource_id}' by agent '{existing_lock.agent_id}' has expired. Removing.")
                    del self.locks[resource_id]; existing_lock = None
                    self.lock_condition.notify_all()
                if not existing_lock:
                    new_lock_id = str(uuid.uuid4()); acquired_at = current_time_ms
                    expires_at = acquired_at + lease_duration_ms if lease_duration_ms > 0 else 0
                    new_lock_info = LockInfoInternal(resource_id, agent_id, new_lock_id, acquired_at, lease_duration_ms, expires_at)
                    self.locks[resource_id] = new_lock_info
                    logger.info(f"SWM: Resource '{resource_id}' successfully locked by agent '{agent_id}'. Lock ID: {new_lock_id}, Lease expires at: {expires_at if expires_at > 0 else 'never'}")
                    return swm_service_pb2.AcquireLockResponse(resource_id, agent_id, swm_service_pb2.LockStatusValue.ACQUIRED, new_lock_id, acquired_at, expires_at, "Lock successfully acquired.")
                if existing_lock.agent_id == agent_id:
                    logger.info(f"SWM: Resource '{resource_id}' is already locked by the same agent '{agent_id}'.")
                    if lease_duration_ms > 0:
                        existing_lock.lease_expires_at_unix_ms = current_time_ms + lease_duration_ms
                        existing_lock.lease_duration_ms = lease_duration_ms
                        logger.info(f"SWM: Lease for '{resource_id}' updated. Expires at: {existing_lock.lease_expires_at_unix_ms}")
                    elif lease_duration_ms == 0 and existing_lock.lease_duration_ms > 0 :
                         existing_lock.lease_expires_at_unix_ms = 0; existing_lock.lease_duration_ms = 0
                         logger.info(f"SWM: Lease for '{resource_id}' removed (now held until explicit release).")
                    return swm_service_pb2.AcquireLockResponse(resource_id, agent_id, swm_service_pb2.LockStatusValue.ALREADY_HELD_BY_YOU, existing_lock.lock_id, existing_lock.acquired_at_unix_ms, existing_lock.lease_expires_at_unix_ms, "Lock already held by you.")
                if timeout_ms == 0:
                    logger.info(f"SWM: Resource '{resource_id}' is locked by agent '{existing_lock.agent_id}'. timeout_ms=0, denying for '{agent_id}'.")
                    return swm_service_pb2.AcquireLockResponse(resource_id, agent_id, swm_service_pb2.LockStatusValue.NOT_AVAILABLE, message=f"Resource is locked by agent {existing_lock.agent_id}.")
                elapsed_monotonic_ms = (time.monotonic() - start_time_monotonic) * 1000
                wait_timeout_sec: typing.Optional[float]
                if timeout_ms < 0: wait_timeout_sec = None
                else:
                    remaining_timeout_ms = timeout_ms - elapsed_monotonic_ms
                    if remaining_timeout_ms <= 0:
                        logger.info(f"SWM: Overall timeout ({timeout_ms}ms) expired for resource='{resource_id}', agent '{agent_id}'.")
                        return swm_service_pb2.AcquireLockResponse(resource_id, agent_id, swm_service_pb2.LockStatusValue.TIMEOUT, message="Lock acquisition timed out.")
                    wait_timeout_sec = remaining_timeout_ms / 1000.0
                logger.debug(f"SWM: Agent '{agent_id}' waiting for lock on '{resource_id}'. Wait timeout sec: {wait_timeout_sec}")
                if not self.lock_condition.wait(timeout=wait_timeout_sec):
                    logger.info(f"SWM: Condition wait timeout ({wait_timeout_sec}s) for resource='{resource_id}', agent '{agent_id}'.")
                logger.debug(f"SWM: Agent '{agent_id}' awakened for lock on '{resource_id}'. Re-checking...")

    def ReleaseLock(self, request: swm_service_pb2.ReleaseLockRequest, context) -> swm_service_pb2.ReleaseLockResponse:
        resource_id = request.resource_id; agent_id = request.agent_id; lock_id_from_request = request.lock_id
        logger.info(f"SWM: ReleaseLock request for resource='{resource_id}' by agent='{agent_id}', lock_id_req='{lock_id_from_request}'")
        with self.lock_condition:
            existing_lock = self.locks.get(resource_id)
            if not existing_lock:
                logger.warning(f"SWM: Attempt to release a non-existent lock for resource='{resource_id}'.")
                return swm_service_pb2.ReleaseLockResponse(resource_id, swm_service_pb2.ReleaseStatusValue.NOT_HELD, "Lock for resource not found.")
            current_time_ms = int(time.time() * 1000)
            if existing_lock.lease_duration_ms > 0 and current_time_ms >= existing_lock.lease_expires_at_unix_ms:
                logger.info(f"SWM: Lock for resource='{resource_id}' (agent '{existing_lock.agent_id}') already expired before ReleaseLock attempt. Removing.")
                del self.locks[resource_id]; self.lock_condition.notify_all()
                return swm_service_pb2.ReleaseLockResponse(resource_id, swm_service_pb2.ReleaseStatusValue.NOT_HELD, "Lock expired before release attempt.")
            if existing_lock.agent_id != agent_id:
                logger.warning(f"SWM: Agent '{agent_id}' attempting to release lock for resource='{resource_id}' held by agent '{existing_lock.agent_id}'. Denied.")
                return swm_service_pb2.ReleaseLockResponse(resource_id, swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING, "Lock held by another agent.")
            if lock_id_from_request and existing_lock.lock_id != lock_id_from_request:
                logger.warning(f"SWM: Invalid lock_id ('{lock_id_from_request}') for ReleaseLock on resource='{resource_id}' (expected '{existing_lock.lock_id}'). Denied.")
                return swm_service_pb2.ReleaseLockResponse(resource_id, swm_service_pb2.ReleaseStatusValue.INVALID_LOCK_ID, "Invalid lock ID.")
            del self.locks[resource_id]
            logger.info(f"SWM: Lock for resource='{resource_id}' successfully released by agent '{agent_id}'. Notifying waiters.")
            self.lock_condition.notify_all()
            return swm_service_pb2.ReleaseLockResponse(resource_id, swm_service_pb2.ReleaseStatusValue.RELEASED, "Lock successfully released.")

    def GetLockInfo(self, request: swm_service_pb2.GetLockInfoRequest, context) -> swm_service_pb2.LockInfo:
        resource_id = request.resource_id
        logger.debug(f"SWM: GetLockInfo request for resource='{resource_id}'")
        with self.lock_condition:
            current_time_ms = int(time.time() * 1000)
            lock_data = self.locks.get(resource_id)
            if lock_data:
                if lock_data.lease_duration_ms > 0 and current_time_ms >= lock_data.lease_expires_at_unix_ms:
                    logger.info(f"SWM (GetLockInfo): Lock for resource='{resource_id}' (agent '{lock_data.agent_id}') has expired. Removing.")
                    del self.locks[resource_id]; lock_data = None; self.lock_condition.notify_all()
            if lock_data:
                return swm_service_pb2.LockInfo(resource_id, True, lock_data.agent_id, lock_data.lock_id, lock_data.acquired_at_unix_ms, lock_data.lease_expires_at_unix_ms if lock_data.lease_duration_ms > 0 else 0)
            else:
                return swm_service_pb2.LockInfo(resource_id=resource_id, is_locked=False)

    # --- RPC Distributed Counter Implementations ---
    def IncrementCounter(self, request: swm_service_pb2.IncrementCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        counter_id = request.counter_id; increment_by = request.increment_by
        logger.info(f"SWM: IncrementCounter request for counter_id='{counter_id}', increment_by={increment_by}")
        if not counter_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT, "counter_id cannot be empty.")
        with self.counters_lock:
            current_value = self.counters.get(counter_id, 0)
            new_value = current_value + increment_by
            self.counters[counter_id] = new_value
            logger.info(f"SWM: Counter '{counter_id}' updated. Old value: {current_value}, New value: {new_value}")
            return swm_service_pb2.CounterValueResponse(counter_id, new_value, "Counter successfully updated.")

    def GetCounter(self, request: swm_service_pb2.DistributedCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        counter_id = request.counter_id
        logger.info(f"SWM: GetCounter request for counter_id='{counter_id}'")
        if not counter_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT, "counter_id cannot be empty.")
        with self.counters_lock:
            current_value = self.counters.get(counter_id)
            if current_value is None:
                logger.warning(f"SWM: Counter '{counter_id}' not found.")
                return swm_service_pb2.CounterValueResponse(counter_id, 0, f"Counter '{counter_id}' not found, returned default value 0.")
            else:
                logger.info(f"SWM: Counter '{counter_id}', current value: {current_value}")
                return swm_service_pb2.CounterValueResponse(counter_id, current_value, "Counter value successfully retrieved.")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # Создаем экземпляр сервисера один раз
    servicer_instance = SharedWorkingMemoryServiceImpl()
    swm_service_pb2_grpc.add_SharedWorkingMemoryServiceServicer_to_server(
        servicer_instance, server
    )
    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS)
    logger.info(f"Starting SWM (Shared Working Memory Service) on {config.GRPC_LISTEN_ADDRESS}...")
    server.start()
    logger.info(f"SWM started and listening on {config.GRPC_LISTEN_ADDRESS}.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Stopping SWM (KeyboardInterrupt)...")
    finally:
        logger.info("SWM: Initiating graceful shutdown...")
        # Уведомляем фоновый поток об остановке
        servicer_instance._stop_expired_lock_cleanup_thread() # Вызываем метод остановки у нашего инстанса
        # Даем время потоку завершиться, если он был в wait()
        if servicer_instance._lock_cleanup_thread and servicer_instance._lock_cleanup_thread.is_alive():
             logger.info("SWM: Waiting for lock cleanup thread to finish...")
             servicer_instance._lock_cleanup_thread.join(timeout=servicer_instance._lock_cleanup_interval_seconds + 1)
             if servicer_instance._lock_cleanup_thread.is_alive():
                 logger.warning("SWM: Lock cleanup thread did not finish in time.")

        server.stop(grace=5) # Даем 5 секунд на завершение активных RPC
        logger.info("SWM stopped.")

if __name__ == '__main__':
    serve()
