import grpc.aio as grpc_aio
import asyncio
import time
import sys
import os
import uuid
import logging
import threading
import asyncio
import queue # Renamed to sync_queue if still needed, but asyncio.Queue is primary for events
# import queue as sync_queue # This was the previous thought, directly use queue for clarity if needed

from dataclasses import dataclass, field
from cachetools import LRUCache, Cache
from google.protobuf.timestamp_pb2 import Timestamp
import typing
from typing import Optional, List, Set, Dict, Callable, AsyncGenerator, Tuple

from .config import SWMConfig

config = SWMConfig()

logging.basicConfig(
    level=config.get_log_level_int(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

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

@dataclass
class SubscriberInfo:
    event_queue: asyncio.Queue
    topics: typing.List[swm_service_pb2.SubscriptionTopic] = field(default_factory=list)
    parsed_filters: Dict[str, Set[str]] = field(default_factory=dict)
    subscribes_to_all_kem_lifecycle: bool = False

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
            if meta_key in kem.metadata: value = kem.metadata[meta_key]; self._metadata_indexes.setdefault(meta_key, {}).setdefault(value, set()).add(kem.id)
    def _remove_from_metadata_indexes(self, kem: kem_pb2.KEM):
        if not kem or not kem.id: return
        for meta_key in self._indexed_keys:
            if meta_key in kem.metadata:
                original_value = kem.metadata[meta_key]
                if meta_key in self._metadata_indexes and original_value in self._metadata_indexes[meta_key]:
                    self._metadata_indexes[meta_key][original_value].discard(kem.id)
                    if not self._metadata_indexes[meta_key][original_value]: del self._metadata_indexes[meta_key][original_value]
                    if not self._metadata_indexes[meta_key]: del self._metadata_indexes[meta_key]
    def __setitem__(self, kem_id: str, kem: kem_pb2.KEM):
        with self._lock:
            evicted_kem = None
            if kem_id in self._lru: old_kem = self._lru[kem_id]; self._remove_from_metadata_indexes(old_kem)
            elif len(self._lru) >= self._lru.maxsize:
                _evicted_id, evicted_kem = self._lru.popitem()
                if evicted_kem:
                    self._remove_from_metadata_indexes(evicted_kem)
                    if self._on_evict_callback:
                        try: self._on_evict_callback(evicted_kem)
                        except Exception as e_cb: logger.error(f"Error in on_evict_callback for KEM ID '{_evicted_id}': {e_cb}", exc_info=True)
            self._lru[kem_id] = kem; self._add_to_metadata_indexes(kem)
    def __getitem__(self, kem_id: str) -> kem_pb2.KEM:
        with self._lock: return self._lru[kem_id]
    def __delitem__(self, kem_id: str):
        with self._lock:
            if kem_id in self._lru: kem_to_remove = self._lru.pop(kem_id); self._remove_from_metadata_indexes(kem_to_remove)
            else: raise KeyError(kem_id)
    def get(self, kem_id: str, default=None) -> typing.Optional[kem_pb2.KEM]:
        with self._lock: return self._lru.get(kem_id, default)
    def pop(self, kem_id: str, default=object()) -> kem_pb2.KEM:
        with self._lock:
            if kem_id in self._lru: kem = self._lru.pop(kem_id); self._remove_from_metadata_indexes(kem); return kem
            elif default is not object(): return default # type: ignore
            else: raise KeyError(kem_id)
    def __len__(self) -> int: with self._lock: return len(self._lru)
    def __contains__(self, kem_id: str) -> bool: with self._lock: return kem_id in self._lru
    def values(self) -> List[kem_pb2.KEM]: with self._lock: return list(self._lru.values())
    def items(self) -> List[Tuple[str, kem_pb2.KEM]]: with self._lock: return list(self._lru.items())
    def clear(self):
        with self._lock: self._lru.clear(); self._metadata_indexes.clear(); self._metadata_indexes = {key: {} for key in self._indexed_keys}
    @property
    def maxsize(self): return self._lru.maxsize
    def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Set[str]:
        if meta_key not in self._indexed_keys: return set()
        with self._lock: return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()

@dataclass
class LockInfoInternal:
    resource_id: str; agent_id: str; lock_id: str
    acquired_at_unix_ms: int; lease_duration_ms: int; lease_expires_at_unix_ms: int

class SharedWorkingMemoryServiceImpl(swm_service_pb2_grpc.SharedWorkingMemoryServiceServicer):
    def __init__(self):
        self.config = config
        logger.info(f"Initializing SWM... Cache: {self.config.CACHE_MAX_SIZE}, IndexedKeys: {self.config.INDEXED_METADATA_KEYS}")
        self.glm_stub: Optional[glm_service_pb2_grpc.GlobalLongTermMemoryStub] = None
        self.subscribers_lock = asyncio.Lock()
        self.swm_cache = IndexedLRUCache(self.config.CACHE_MAX_SIZE, self.config.INDEXED_METADATA_KEYS, self._handle_kem_eviction_sync)
        self.locks: Dict[str, LockInfoInternal] = {}; self.lock_condition = asyncio.Condition()
        self.counters: Dict[str, int] = {}; self.counters_lock = asyncio.Lock()
        self._stop_event = asyncio.Event(); self._lock_cleanup_task: Optional[asyncio.Task] = None
        self._lock_cleanup_interval_seconds = getattr(self.config, 'LOCK_CLEANUP_INTERVAL_S', 60)

        self.subscribers: Dict[str, SubscriberInfo] = {}
        self.kem_id_to_subscribers: Dict[str, Set[str]] = {}
        self.metadata_exact_match_to_subscribers: Dict[str, Dict[str, Set[str]]] = {}
        self.general_kem_event_subscribers: Set[str] = set()
        try:
            self.sync_glm_channel = grpc.insecure_channel(self.config.GLM_SERVICE_ADDRESS)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.sync_glm_channel)
            logger.info(f"GLM client (sync) for SWM initialized, target: {self.config.GLM_SERVICE_ADDRESS}")
        except Exception as e: logger.error(f"Error initializing sync GLM client in SWM: {e}"); self.glm_stub = None

    async def start_background_tasks(self): # ... (implementation as before) ...
        if self._lock_cleanup_task is None or self._lock_cleanup_task.done():
            self._stop_event.clear(); self._lock_cleanup_task = asyncio.create_task(self._expired_lock_cleanup_task_async())
            logger.info("SWM: Expired lock cleanup task started.")
    async def stop_background_tasks(self): # ... (implementation as before) ...
        if self._lock_cleanup_task and not self._lock_cleanup_task.done():
            logger.info("SWM: Stopping expired lock cleanup task..."); self._stop_event.set()
            try: await asyncio.wait_for(self._lock_cleanup_task, timeout=self._lock_cleanup_interval_seconds + 1) # type: ignore
            except asyncio.TimeoutError: logger.warning("SWM: Expired lock cleanup task did not finish in time.")
            except asyncio.CancelledError: logger.info("SWM: Expired lock cleanup task was cancelled.")
        self._lock_cleanup_task = None
    async def _expired_lock_cleanup_task_async(self): # ... (implementation as before) ...
        logger.info("SWM: Async expired lock cleanup task started.")
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self._lock_cleanup_interval_seconds);
                if self._stop_event.is_set(): break
                logger.debug("SWM: Performing periodic cleanup of expired locks (async)...")
                async with self.lock_condition:
                    now_ms = int(time.time()*1000); expired_ids = [rid for rid, linfo in self.locks.items() if linfo.lease_duration_ms > 0 and now_ms >= linfo.lease_expires_at_unix_ms]
                    if expired_ids:
                        cleaned = 0
                        for rid in expired_ids:
                            l_rem = self.locks.get(rid)
                            if l_rem and l_rem.lease_duration_ms>0 and now_ms >= l_rem.lease_expires_at_unix_ms:
                                del self.locks[rid]; logger.info(f"SWM Cleanup: Expired lock for '{l_rem.resource_id}' (agent '{l_rem.agent_id}') removed."); self.lock_condition.notify_all(); cleaned+=1
                        if cleaned > 0: logger.info(f"SWM Cleanup: Removed {cleaned} expired locks.")
                    else: logger.debug("SWM Cleanup: No expired locks found.")
            except asyncio.CancelledError: logger.info("SWM: Lock cleanup task cancelled."); break
            except Exception as e: logger.error(f"SWM: Error in lock cleanup task: {e}", exc_info=True); await asyncio.sleep(self._lock_cleanup_interval_seconds) # Prevents tight loop on error
        logger.info("SWM: Async expired lock cleanup task stopped.")
    async def _glm_retrieve_kems_async(self, request: glm_service_pb2.RetrieveKEMsRequest, timeout: int = 20) -> glm_service_pb2.RetrieveKEMsResponse: # ... (implementation as before) ...
        if not self.glm_stub: raise grpc_aio.AioRpcError(grpc_aio.StatusCode.INTERNAL, "GLM client not available") # type: ignore
        return await asyncio.to_thread(self.glm_stub.RetrieveKEMs, request, timeout=timeout)
    async def _glm_store_kem_async(self, request: glm_service_pb2.StoreKEMRequest, timeout: int = 10) -> glm_service_pb2.StoreKEMResponse: # ... (implementation as before) ...
        if not self.glm_stub: raise grpc_aio.AioRpcError(grpc_aio.StatusCode.INTERNAL, "GLM client not available") # type: ignore
        return await asyncio.to_thread(self.glm_stub.StoreKEM, request, timeout=timeout)
    def _handle_kem_eviction_sync(self, evicted_kem: kem_pb2.KEM): # ... (implementation as before) ...
        if evicted_kem: logger.info(f"SWM: KEM ID '{evicted_kem.id}' evicted. Scheduling notification.");
        try: asyncio.run_coroutine_threadsafe(self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED, "SWM_CACHE"), asyncio.get_running_loop())
        except RuntimeError: logger.warning(f"SWM: No running loop for evicted KEM '{evicted_kem.id}' notification.")
    async def _put_kem_to_cache_async(self, kem: kem_pb2.KEM) -> None: # ... (implementation as before) ...
        if not kem or not kem.id: logger.warning("Invalid KEM to cache."); return
        was_present = await asyncio.to_thread(self.swm_cache.__contains__, kem.id)
        await asyncio.to_thread(self.swm_cache.__setitem__, kem.id, kem)
        logger.info(f"KEM ID '{kem.id}' put/updated in SWM cache. Size: {len(self.swm_cache)}/{self.swm_cache.maxsize}")
        await self._notify_subscribers(kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED if was_present else swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

    async def _notify_subscribers(self, kem: kem_pb2.KEM, event_type: swm_service_pb2.SWMMemoryEvent.EventType, source_agent_id: str = "SWM_SERVER"):
        if not kem or not kem.id: logger.warning("_notify_subscribers called with an invalid KEM."); return

        event_to_dispatch = swm_service_pb2.SWMMemoryEvent(
            event_id=str(uuid.uuid4()), event_type=event_type, kem_payload=kem,
            event_time=Timestamp(seconds=int(time.time())), # Simplified timestamp
            source_agent_id=source_agent_id,
            details=f"Event {swm_service_pb2.SWMMemoryEvent.EventType.Name(event_type)} for KEM ID {kem.id}"
        )

        candidate_subscriber_ids: Set[str] = set()
        async with self.subscribers_lock:
            candidate_subscriber_ids.update(self.general_kem_event_subscribers)
            if kem.id in self.kem_id_to_subscribers:
                candidate_subscriber_ids.update(self.kem_id_to_subscribers[kem.id])
            if kem.metadata:
                for meta_key, meta_value in kem.metadata.items():
                    if meta_key in self.metadata_exact_match_to_subscribers:
                        if meta_value in self.metadata_exact_match_to_subscribers[meta_key]:
                            candidate_subscriber_ids.update(self.metadata_exact_match_to_subscribers[meta_key][meta_value])

            sub_infos_to_notify: List[SubscriberInfo] = []
            for sub_id in candidate_subscriber_ids:
                if sub_id in self.subscribers: # Check if subscriber still exists
                    sub_info = self.subscribers[sub_id]
                    # If subscriber is general OR specific filters match (already implicitly handled by being in candidate_subscriber_ids from indexes)
                    # OR if the subscriber has non-indexable filters that need further checking (not implemented in this simplified indexing)
                    # For now, if they are in candidate_subscriber_ids, we assume they should get the event based on indexed filters,
                    # or they are general subscribers. More complex non-indexed filter logic would go here if needed.
                    if sub_info.subscribes_to_all_kem_lifecycle or \
                       kem.id in sub_info.parsed_filters.get("kem_id", set()) or \
                       any(kem.metadata.get(mk.split("metadata.",1)[1]) == mv_set_item
                           for mk, mv_set in sub_info.parsed_filters.items() if mk.startswith("metadata.")
                           for mv_set_item in mv_set): # This re-checks, can be optimized
                         sub_infos_to_notify.append(sub_info)


        if not sub_infos_to_notify: logger.debug(f"No matching subscribers for event on KEM ID '{kem.id}'."); return
        logger.debug(f"Dispatching event for KEM ID '{kem.id}' to {len(sub_infos_to_notify)} subscribers.")

        for sub_info in sub_infos_to_notify:
            try:
                if sub_info.event_queue.full(): logger.warning(f"Subscriber queue full for event {event_to_dispatch.event_id}. Event lost.")
                else: await sub_info.event_queue.put(event_to_dispatch)
            except Exception as e: logger.error(f"Error queueing event for subscriber: {e}", exc_info=True)

    async def PublishKEMToSWM(self, request: swm_service_pb2.PublishKEMToSWMRequest, context) -> swm_service_pb2.PublishKEMToSWMResponse: # ... (implementation as before, uses async helpers) ...
        kem_to_publish = request.kem_to_publish; logger.info(f"SWM: PublishKEMToSWM for KEM ID (suggested): '{kem_to_publish.id}'")
        kem_id_final = kem_to_publish.id or str(uuid.uuid4()); kem_to_publish.id = kem_id_final
        if not request.kem_to_publish.id: logger.info(f"SWM: No ID provided, new ID generated: '{kem_id_final}'")
        ts = Timestamp(); ts.GetCurrentTime()
        # Use asyncio.to_thread for synchronous cache access
        existing_kem_dict = await asyncio.to_thread(self.swm_cache.get, kem_id_final)
        if existing_kem_dict: kem_to_publish.created_at.CopyFrom(kem_pb2.KEM(**existing_kem_dict).created_at)
        elif not kem_to_publish.HasField("created_at"): kem_to_publish.created_at.CopyFrom(ts)
        kem_to_publish.updated_at.CopyFrom(ts)
        await self._put_kem_to_cache_async(kem_to_publish)
        published_to_swm_flag=True; persistence_triggered_flag=False; status_msg=f"KEM ID '{kem_id_final}' published to SWM."
        if request.persist_to_glm_if_new_or_updated:
            if not self.glm_stub: msg_glm=f"GLM unavailable, KEM '{kem_id_final}' not persisted.";logger.error(msg_glm);status_msg+=" "+msg_glm
            else:
                try:
                    logger.info(f"SWM: Persisting KEM '{kem_id_final}' to GLM...");glm_req=glm_service_pb2.StoreKEMRequest(kem=kem_to_publish)
                    glm_resp=await self._glm_store_kem_async(glm_req,timeout=10)
                    if glm_resp and glm_resp.kem and glm_resp.kem.id:
                        await self._put_kem_to_cache_async(glm_resp.kem); kem_id_final=glm_resp.kem.id
                        status_msg+=f" Persisted/updated in GLM with ID '{kem_id_final}'."; persistence_triggered_flag=True
                    else: msg_glm_err=f"GLM.StoreKEM no/bad response for KEM '{kem_id_final}'.";logger.error(msg_glm_err);status_msg+=" "+msg_glm_err
                except Exception as e: msg_glm_rpc_err=f"Error persisting KEM '{kem_id_final}' to GLM: {e}";logger.error(msg_glm_rpc_err,exc_info=True);status_msg+=" "+msg_glm_rpc_err
        return swm_service_pb2.PublishKEMToSWMResponse(kem_id_final,published_to_swm_flag,persistence_triggered_flag,status_msg)

    def _remove_subscriber_from_indexes(self, subscriber_id: str, original_topics: List[swm_service_pb2.SubscriptionTopic]):
        # Must be called under self.subscribers_lock
        self.general_kem_event_subscribers.discard(subscriber_id)
        if original_topics:
            for topic in original_topics:
                if topic.type == swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS and \
                   topic.filter_criteria and '=' in topic.filter_criteria:
                    key, value = topic.filter_criteria.split("=", 1)
                    key = key.strip(); value = value.strip()
                    if key == "kem_id":
                        if value in self.kem_id_to_subscribers:
                            self.kem_id_to_subscribers[value].discard(subscriber_id)
                            if not self.kem_id_to_subscribers[value]: del self.kem_id_to_subscribers[value]
                    elif key.startswith("metadata."):
                        meta_actual_key = key.split("metadata.", 1)[1]
                        if meta_actual_key and meta_actual_key in self.metadata_exact_match_to_subscribers:
                            if value in self.metadata_exact_match_to_subscribers[meta_actual_key]:
                                self.metadata_exact_match_to_subscribers[meta_actual_key][value].discard(subscriber_id)
                                if not self.metadata_exact_match_to_subscribers[meta_actual_key][value]: del self.metadata_exact_match_to_subscribers[meta_actual_key][value]
                            if not self.metadata_exact_match_to_subscribers[meta_actual_key]: del self.metadata_exact_match_to_subscribers[meta_actual_key]
        logger.debug(f"SWM: Subscriber '{subscriber_id}' removed from specific filter indexes.")

    async def SubscribeToSWMEvents(self, request: swm_service_pb2.SubscribeToSWMEventsRequest, context: grpc_aio.ServicerContext) -> AsyncGenerator[swm_service_pb2.SWMMemoryEvent, None]:
        agent_id = request.agent_id; logger.info(f"SWM: New subscriber {agent_id} for topics: {request.topics}")
        subscriber_id = agent_id if agent_id else str(uuid.uuid4())
        event_q = asyncio.Queue(maxsize=100)
        parsed_filters_for_subscriber: Dict[str, Set[str]] = {}; subscribes_to_all_kem_lifecycle_for_subscriber = False
        has_any_kem_lifecycle_topic = any(t.type == swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS for t in request.topics)
        has_specific_indexable_filter = False
        if not request.topics and has_any_kem_lifecycle_topic: subscribes_to_all_kem_lifecycle_for_subscriber = True # Should not happen if topics is empty
        elif request.topics:
            is_general_candidate = False
            for topic in request.topics:
                if topic.type == swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS:
                    is_general_candidate = True # At least one KEM_LIFECYCLE topic
                    if not topic.filter_criteria: subscribes_to_all_kem_lifecycle_for_subscriber = True; break
                    if '=' in topic.filter_criteria:
                        key, value = topic.filter_criteria.split("=", 1); key = key.strip(); value = value.strip()
                        parsed_filters_for_subscriber.setdefault(key, set()).add(value)
                        if key == "kem_id" or (key.startswith("metadata.") and key.split("metadata.",1)[1]): has_specific_indexable_filter = True
            if is_general_candidate and not has_specific_indexable_filter and not subscribes_to_all_kem_lifecycle_for_subscriber : subscribes_to_all_kem_lifecycle_for_subscriber = True

        subscriber_info = SubscriberInfo(event_q, list(request.topics), parsed_filters_for_subscriber, subscribes_to_all_kem_lifecycle_for_subscriber)
        async with self.subscribers_lock:
            if subscriber_id in self.subscribers: logger.warning(f"Sub ID '{subscriber_id}' exists. Removing old indexes."); self._remove_subscriber_from_indexes(subscriber_id, self.subscribers[subscriber_id].topics)
            self.subscribers[subscriber_id] = subscriber_info
            if subscribes_to_all_kem_lifecycle_for_subscriber: self.general_kem_event_subscribers.add(subscriber_id)
            else:
                for kem_id_val in parsed_filters_for_subscriber.get("kem_id", set()): self.kem_id_to_subscribers.setdefault(kem_id_val, set()).add(subscriber_id)
                for meta_key_filter, meta_values_set in parsed_filters_for_subscriber.items():
                    if meta_key_filter.startswith("metadata."):
                        actual_meta_key = meta_key_filter.split("metadata.",1)[1]
                        if actual_meta_key: key_idx = self.metadata_exact_match_to_subscribers.setdefault(actual_meta_key, {}); [key_idx.setdefault(mvf, set()).add(subscriber_id) for mvf in meta_values_set]
            logger.info(f"SWM: Sub '{subscriber_id}' reg/upd. Total: {len(self.subscribers)}. Gen KEM subs: {len(self.general_kem_event_subscribers)}")
        active_sub = True
        try:
            while active_sub:
                try:
                    evt = await asyncio.wait_for(event_q.get(), timeout=1.0)
                    if not context.is_active(): active_sub=False; break # type: ignore
                    yield evt; event_q.task_done()
                except asyncio.TimeoutError:
                    if not context.is_active(): active_sub=False; break # type: ignore
                except Exception as e: logger.error(f"SWM stream error for '{subscriber_id}': {e}",exc_info=True); active_sub=False; break
        except asyncio.CancelledError: logger.info(f"SWM stream for '{subscriber_id}' cancelled.")
        finally:
            logger.info(f"SWM: Cleaning up sub for '{subscriber_id}' (active: {active_sub})")
            async with self.subscribers_lock:
                removed_info = self.subscribers.pop(subscriber_id, None)
                if removed_info: logger.info(f"SWM: Sub '{subscriber_id}' removed. Rem: {len(self.subscribers)}"); self._remove_subscriber_from_indexes(subscriber_id, removed_info.topics)
                else: logger.warning(f"SWM: Attempt to remove non-existent sub '{subscriber_id}'.")

    async def QuerySWM(self, request: swm_service_pb2.QuerySWMRequest, context) -> swm_service_pb2.QuerySWMResponse: # ... (implementation as before, using to_thread for sync cache logic) ...
        query = request.query; logger.info(f"SWM: QuerySWM called with KEMQuery: {query}")
        if query.embedding_query or query.text_query: msg = "Vector/text search not in SWM cache."; logger.warning(msg); await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT, msg); return swm_service_pb2.QuerySWMResponse() # type: ignore
        def _sync_query_swm_logic():
            pg_sz = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE; offset = 0
            if request.page_token:
                try: offset = int(request.page_token)
                except ValueError: logger.warning(f"Invalid page_token for QuerySWM: '{request.page_token}', using 0.")
            kems_l:List[kem_pb2.KEM]=[]; final_ids:Optional[Set[str]]=None; idx_meta_q:Dict[str,str]={}; unidx_meta_q:Dict[str,str]={}
            if query.metadata_filters:
                for k,v in query.metadata_filters.items():
                    if k in self.swm_cache._indexed_keys: idx_meta_q[k]=v
                    else: unidx_meta_q[k]=v
            if idx_meta_q:
                intersect_ids:Optional[Set[str]]=None
                for k,v in idx_meta_q.items():
                    ids_idx=self.swm_cache.get_ids_by_metadata_filter(k,v)
                    if intersect_ids is None: intersect_ids=ids_idx if ids_idx is not None else set()
                    else: intersect_ids.intersection_update(ids_idx if ids_idx is not None else set())
                    if not intersect_ids: break
                if not intersect_ids: return [],""
                final_ids=intersect_ids
            if final_ids is not None: [ (_k:=self.swm_cache.get(k_id)) and kems_l.append(_k) for k_id in final_ids]
            else: kems_l=self.swm_cache.values()
            if query.ids: ids_s=set(query.ids); kems_l=[k for k in kems_l if k.id in ids_s]
            if unidx_meta_q:
                tmp_kems=[]; [ (all(k.metadata.get(mk)==mv for mk,mv in unidx_meta_q.items())) and tmp_kems.append(k) for k in kems_l]; kems_l=tmp_kems
            def chk_dt(k_ts,s_ts,e_ts)->bool:
                if s_ts.seconds and k_ts.ToNanoseconds()<s_ts.ToNanoseconds():return False
                if e_ts.seconds and k_ts.ToNanoseconds()>e_ts.ToNanoseconds():return False
                return True
            if query.HasField("created_at_start") or query.HasField("created_at_end"): kems_l=[k for k in kems_l if chk_dt(k.created_at,query.created_at_start,query.created_at_end)]
            if query.HasField("updated_at_start") or query.HasField("updated_at_end"): kems_l=[k for k in kems_l if chk_dt(k.updated_at,query.updated_at_start,query.updated_at_end)]
            k_pg=kems_l[offset:offset+pg_sz]; n_tok=str(offset+pg_sz) if len(kems_l)>offset+pg_sz else ""
            return k_pg, n_tok
        k_p_sync,n_t_sync = await asyncio.to_thread(_sync_query_swm_logic)
        logger.info(f"QuerySWM: Returning {len(k_p_sync)} KEMs."); return swm_service_pb2.QuerySWMResponse(kems=k_p_sync,next_page_token=n_t_sync) # type: ignore

    async def LoadKEMsFromGLM(self, request: swm_service_pb2.LoadKEMsFromGLMRequest, context) -> swm_service_pb2.LoadKEMsFromGLMResponse: # ... (implementation as before, using async helpers) ...
        logger.info(f"SWM: LoadKEMsFromGLM query: {request.query_for_glm}");
        if not self.glm_stub: msg="GLM client N/A";logger.error(msg);await context.abort(grpc_aio.StatusCode.INTERNAL,msg); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore
        req=glm_service_pb2.RetrieveKEMsRequest(query=request.query_for_glm);load_c=0;load_ids_l=[];glm_qc=0
        try:
            logger.info(f"SWM: Req GLM.RetrieveKEMs(async): {req}"); resp=await self._glm_retrieve_kems_async(req,timeout=20)
            if resp and resp.kems:
                glm_qc=len(resp.kems);[await self._put_kem_to_cache_async(k) or load_ids_l.append(k.id) for k in resp.kems]
                load_c=len(load_ids_l); msg=f"Loaded {load_c} KEMs."
            else: msg="GLM no KEMs.";logger.info(msg)
            return swm_service_pb2.LoadKEMsFromGLMResponse(glm_qc,load_c,load_ids_l,msg)
        except grpc.RpcError as e:msg=f"gRPC err GLM: {e.details()}";logger.error(msg);await context.abort(e.code(),msg); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore
        except Exception as e:msg=f"Err LoadKEMs: {e}";logger.error(msg,exc_info=True);await context.abort(grpc_aio.StatusCode.INTERNAL,msg); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore

    async def AcquireLock(self, request: swm_service_pb2.AcquireLockRequest, context) -> swm_service_pb2.AcquireLockResponse: # ... (implementation as before) ...
        rid,aid,t_ms,l_ms=request.resource_id,request.agent_id,request.timeout_ms,request.lease_duration_ms
        logger.info(f"SWM: AcquireLock '{rid}' by '{aid}', t={t_ms}, l={l_ms}");s_mono=time.monotonic()
        async with self.lock_condition:
            while True:
                now=int(time.time()*1000);lock=self.locks.get(rid)
                if lock and lock.lease_duration_ms>0 and now>=lock.lease_expires_at_unix_ms: logger.info(f"SWM: Lock '{rid}' by '{lock.agent_id}' expired.");del self.locks[rid];lock=None;self.lock_condition.notify_all()
                if not lock:
                    lid=str(uuid.uuid4());acq_at=now;exp_at=acq_at+l_ms if l_ms>0 else 0
                    self.locks[rid]=LockInfoInternal(rid,aid,lid,acq_at,l_ms,exp_at);logger.info(f"SWM: Lock '{rid}' acquired by '{aid}'.ID:{lid}.Exp:{exp_at or 'never'}")
                    return swm_service_pb2.AcquireLockResponse(rid,aid,swm_service_pb2.LockStatusValue.ACQUIRED,lid,acq_at,exp_at,"Acquired.")
                if lock.agent_id==aid:
                    logger.info(f"SWM: Lock '{rid}' already held by '{aid}'.")
                    if l_ms>0:lock.lease_expires_at_unix_ms=now+l_ms;lock.lease_duration_ms=l_ms;logger.info(f"SWM: Lease for '{rid}' updated.Exp:{lock.lease_expires_at_unix_ms}")
                    elif l_ms==0 and lock.lease_duration_ms>0:lock.lease_expires_at_unix_ms=0;lock.lease_duration_ms=0;logger.info(f"SWM: Lease for '{rid}' indefinite.")
                    return swm_service_pb2.AcquireLockResponse(rid,aid,swm_service_pb2.LockStatusValue.ALREADY_HELD_BY_YOU,lock.lock_id,lock.acquired_at_unix_ms,lock.lease_expires_at_unix_ms,"Held.")
                if t_ms==0:logger.info(f"SWM: Lock '{rid}' by '{lock.agent_id}'.No wait for '{aid}'.");return swm_service_pb2.AcquireLockResponse(rid,aid,swm_service_pb2.LockStatusValue.NOT_AVAILABLE,message=f"Locked by {lock.agent_id}.")
                el_ms=(time.monotonic()-s_mono)*1000;wait_s:Optional[float]=None
                if t_ms>0:
                    rem_ms=t_ms-el_ms
                    if rem_ms<=0:logger.info(f"SWM: Timeout({t_ms}ms) for '{rid}',ag '{aid}'.");return swm_service_pb2.AcquireLockResponse(rid,aid,swm_service_pb2.LockStatusValue.TIMEOUT,message="Timeout.")
                    wait_s=rem_ms/1000.0
                logger.debug(f"SWM: Ag '{aid}' waits for '{rid}'.Wait_s:{wait_s}")
                try:await asyncio.wait_for(self.lock_condition.wait(),timeout=wait_s)
                except asyncio.TimeoutError:logger.info(f"SWM: Cond wait timeout({wait_s}s) for '{rid}',ag '{aid}'.")
                logger.debug(f"SWM: Ag '{aid}' awakened for '{rid}'.Re-check...")
    async def ReleaseLock(self, request: swm_service_pb2.ReleaseLockRequest, context) -> swm_service_pb2.ReleaseLockResponse: # ... (implementation as before) ...
        rid,ag_id,req_lid=request.resource_id,request.agent_id,request.lock_id;logger.info(f"SWM: ReleaseLock '{rid}' by '{ag_id}',req_lid='{req_lid}'")
        async with self.lock_condition:
            lock=self.locks.get(rid)
            if not lock:logger.warning(f"SWM: Release non-exist lock '{rid}'.");return swm_service_pb2.ReleaseLockResponse(rid,swm_service_pb2.ReleaseStatusValue.NOT_HELD,"Not found.")
            now=int(time.time()*1000)
            if lock.lease_duration_ms>0 and now>=lock.lease_expires_at_unix_ms:logger.info(f"SWM: Lock '{rid}' by '{lock.agent_id}' expired.Rem.");del self.locks[rid];self.lock_condition.notify_all();return swm_service_pb2.ReleaseLockResponse(rid,swm_service_pb2.ReleaseStatusValue.NOT_HELD,"Expired.")
            if lock.agent_id!=ag_id:logger.warning(f"SWM: Ag '{ag_id}' tries release lock '{rid}' held by '{lock.agent_id}'.Denied.");return swm_service_pb2.ReleaseLockResponse(rid,swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING,"Held by other.")
            if req_lid and lock.lock_id!=req_lid:logger.warning(f"SWM: Invalid lock_id ('{req_lid}') for ReleaseLock '{rid}'.Exp '{lock.lock_id}'.Denied.");return swm_service_pb2.ReleaseLockResponse(rid,swm_service_pb2.ReleaseStatusValue.INVALID_LOCK_ID,"Invalid lock ID.")
            del self.locks[rid];logger.info(f"SWM: Lock '{rid}' released by '{ag_id}'.Notifying.");self.lock_condition.notify_all()
            return swm_service_pb2.ReleaseLockResponse(resource_id=rid,status=swm_service_pb2.ReleaseStatusValue.RELEASED,message="Released.")
    async def GetLockInfo(self, request: swm_service_pb2.GetLockInfoRequest, context) -> swm_service_pb2.LockInfo: # ... (implementation as before) ...
        rid=request.resource_id;logger.debug(f"SWM: GetLockInfo for '{rid}'")
        async with self.lock_condition:
            now=int(time.time()*1000);lock=self.locks.get(rid)
            if lock and lock.lease_duration_ms>0 and now>=lock.lease_expires_at_unix_ms:logger.info(f"SWM: Lock '{rid}' by '{lock.agent_id}' expired.Rem.");del self.locks[rid];lock=None;self.lock_condition.notify_all()
            if lock:return swm_service_pb2.LockInfo(rid,True,lock.agent_id,lock.lock_id,lock.acquired_at_unix_ms,lock.lease_expires_at_unix_ms if lock.lease_duration_ms>0 else 0)
            else:return swm_service_pb2.LockInfo(resource_id=rid,is_locked=False)
    async def IncrementCounter(self, request: swm_service_pb2.IncrementCounterRequest, context) -> swm_service_pb2.CounterValueResponse: # ... (implementation as before) ...
        cid,inc_by=request.counter_id,request.increment_by;logger.info(f"SWM: IncCounter '{cid}', by {inc_by}")
        if not cid:await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT,"counter_id empty.");return swm_service_pb2.CounterValueResponse() # type: ignore
        async with self.counters_lock:cur_val=self.counters.get(cid,0);new_val=cur_val+inc_by;self.counters[cid]=new_val;logger.info(f"SWM: Counter '{cid}' upd. Old:{cur_val},New:{new_val}");return swm_service_pb2.CounterValueResponse(cid,new_val,"Updated.")
    async def GetCounter(self, request: swm_service_pb2.DistributedCounterRequest, context) -> swm_service_pb2.CounterValueResponse: # ... (implementation as before) ...
        cid=request.counter_id;logger.info(f"SWM: GetCounter for '{cid}'")
        if not cid:await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT,"counter_id empty.");return swm_service_pb2.CounterValueResponse() # type: ignore
        async with self.counters_lock:cur_val=self.counters.get(cid)
        if cur_val is None:logger.warning(f"SWM: Counter '{cid}' not found.");return swm_service_pb2.CounterValueResponse(cid,0,f"Counter '{cid}' not found, default 0.")
        else:logger.info(f"SWM: Counter '{cid}', val: {cur_val}");return swm_service_pb2.CounterValueResponse(cid,cur_val,"Retrieved.")

async def serve(): # ... (implementation as before) ...
    server=grpc_aio.server();servicer=SharedWorkingMemoryServiceImpl()
    swm_service_pb2_grpc.add_SharedWorkingMemoryServiceServicer_to_server(servicer,server)
    await servicer.start_background_tasks()
    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS);logger.info(f"Starting SWM async on {config.GRPC_LISTEN_ADDRESS}...");await server.start()
    logger.info(f"SWM started async on {config.GRPC_LISTEN_ADDRESS}.")
    try:await server.wait_for_termination()
    except KeyboardInterrupt:logger.info("Stopping SWM (KeyboardInterrupt)...")
    except asyncio.CancelledError:logger.info("SWM server task cancelled.")
    finally:
        logger.info("SWM: Initiating graceful shutdown...");await servicer.stop_background_tasks();await server.stop(grace=5);logger.info("SWM stopped.")

if __name__=='__main__':
    try:asyncio.run(serve())
    except KeyboardInterrupt:logger.info("SWM main process interrupted.")
    except Exception as e:logger.critical(f"SWM main unhandled exception: {e}",exc_info=True)

```
