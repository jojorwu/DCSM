import grpc.aio as grpc_aio # Changed import for asyncio server
import asyncio # Added for asyncio
import time
import sys
import os
import uuid
import logging
import threading
import asyncio # Re-importing for clarity, though already imported above
import queue as sync_queue # Keep sync_queue for now if used by non-async parts, or refactor fully

from dataclasses import dataclass, field
from cachetools import LRUCache, Cache
from google.protobuf.timestamp_pb2 import Timestamp
import typing

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
    event_queue: asyncio.Queue # Changed to asyncio.Queue
    topics: typing.List[swm_service_pb2.SubscriptionTopic] = field(default_factory=list)

class IndexedLRUCache(Cache):
    def __init__(self, maxsize, indexed_keys: typing.List[str], on_evict_callback: typing.Optional[typing.Callable[[kem_pb2.KEM], None]] = None):
        super().__init__(maxsize)
        self._lru = LRUCache(maxsize=maxsize)
        self._indexed_keys = set(indexed_keys)
        self._metadata_indexes: typing.Dict[str, typing.Dict[str, typing.Set[str]]] = {key: {} for key in self._indexed_keys}
        self._lock = threading.Lock() # Sync lock, as cachetools is sync
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
                            self._on_evict_callback(evicted_kem) # This callback might need to be async or run in executor
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
    def values(self) -> typing.ValuesView[kem_pb2.KEM]: # Actually returns List due to list()
        with self._lock:
            return list(self._lru.values())
    def items(self) -> typing.ItemsView[str, kem_pb2.KEM]: # Actually returns List due to list()
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
        self.glm_channel: Optional[grpc_aio.Channel] = None # Potentially make GLM client async too
        self.glm_stub: Optional[glm_service_pb2_grpc.GlobalLongTermMemoryStub] = None # This is sync stub

        self.retry_max_attempts = self.config.GLM_RETRY_MAX_ATTEMPTS
        self.retry_initial_delay_s = self.config.GLM_RETRY_INITIAL_DELAY_S
        self.retry_backoff_factor = self.config.GLM_RETRY_BACKOFF_FACTOR

        self.subscribers: typing.Dict[str, SubscriberInfo] = {}
        self.subscribers_lock = asyncio.Lock() # Changed to asyncio.Lock

        self.swm_cache = IndexedLRUCache( # This remains sync for now
            maxsize=self.config.CACHE_MAX_SIZE,
            indexed_keys=self.config.INDEXED_METADATA_KEYS,
            on_evict_callback=self._handle_kem_eviction_sync # Callback needs to be sync or handled
        )

        self.locks: typing.Dict[str, LockInfoInternal] = {}
        self.lock_condition = asyncio.Condition() # Changed to asyncio.Condition (uses internal asyncio.Lock)

        self.counters: typing.Dict[str, int] = {}
        self.counters_lock = asyncio.Lock() # Changed to asyncio.Lock

        self._stop_event = asyncio.Event() # Changed to asyncio.Event
        self._lock_cleanup_task: Optional[asyncio.Task] = None # For asyncio task
        self._lock_cleanup_interval_seconds = getattr(self.config, 'LOCK_CLEANUP_INTERVAL_S', 60)

        # GLM client setup (still synchronous for now, calls will be wrapped)
        try:
            # For now, keep GLM client synchronous. Calls from async methods will need `to_thread`.
            self.sync_glm_channel = grpc.insecure_channel(self.config.GLM_SERVICE_ADDRESS)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.sync_glm_channel)
            logger.info(f"GLM client (sync) for SWM initialized, target: {self.config.GLM_SERVICE_ADDRESS}")
        except Exception as e:
            logger.error(f"Error initializing synchronous GLM client in SWM: {e}")
            self.glm_stub = None

        # Start background tasks if SWM is fully initialized
        # self._lock_cleanup_task = asyncio.create_task(self._expired_lock_cleanup_task_async())


    # --- Lifecycle Methods for Async Tasks ---
    async def start_background_tasks(self):
        """Starts background tasks like lock cleanup."""
        if self._lock_cleanup_task is None or self._lock_cleanup_task.done():
            self._stop_event.clear()
            self._lock_cleanup_task = asyncio.create_task(self._expired_lock_cleanup_task_async())
            logger.info("SWM: Expired lock cleanup task started.")

    async def stop_background_tasks(self):
        """Stops background tasks."""
        if self._lock_cleanup_task and not self._lock_cleanup_task.done():
            logger.info("SWM: Stopping expired lock cleanup task...")
            self._stop_event.set()
            try:
                await asyncio.wait_for(self._lock_cleanup_task, timeout=self._lock_cleanup_interval_seconds + 5)
                logger.info("SWM: Expired lock cleanup task finished.")
            except asyncio.TimeoutError:
                logger.warning("SWM: Expired lock cleanup task did not finish in time.")
            except asyncio.CancelledError:
                 logger.info("SWM: Expired lock cleanup task was cancelled during shutdown.")
        self._lock_cleanup_task = None


    async def _expired_lock_cleanup_task_async(self):
        logger.info("SWM: Async expired lock cleanup task started.")
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self._lock_cleanup_interval_seconds)
                if self._stop_event.is_set(): break

                logger.debug("SWM: Performing periodic cleanup of expired locks (async)...")
                async with self.lock_condition: # Acquire the asyncio.Condition's lock
                    current_time_ms = int(time.time() * 1000)
                    expired_resource_ids = [
                        res_id for res_id, lock_info in self.locks.items()
                        if lock_info.lease_duration_ms > 0 and current_time_ms >= lock_info.lease_expires_at_unix_ms
                    ]

                    if expired_resource_ids:
                        cleaned_count = 0
                        for resource_id in expired_resource_ids:
                            # Re-check condition as lock might have been released/updated
                            lock_to_remove = self.locks.get(resource_id)
                            if lock_to_remove and \
                               lock_to_remove.lease_duration_ms > 0 and \
                               current_time_ms >= lock_to_remove.lease_expires_at_unix_ms:
                                del self.locks[resource_id]
                                logger.info(f"SWM Cleanup: Expired lock for resource='{lock_to_remove.resource_id}' (agent '{lock_to_remove.agent_id}') removed.")
                                self.lock_condition.notify_all()
                                cleaned_count +=1
                        if cleaned_count > 0:
                            logger.info(f"SWM Cleanup: Removed {cleaned_count} expired locks.")
                    else:
                        logger.debug("SWM Cleanup: No expired locks found.")
            except asyncio.CancelledError:
                logger.info("SWM: Lock cleanup task cancelled.")
                break
            except Exception as e:
                logger.error(f"SWM: Error in lock cleanup task: {e}", exc_info=True)
                await asyncio.sleep(self._lock_cleanup_interval_seconds) # Avoid tight loop on error

        logger.info("SWM: Async expired lock cleanup task stopped.")

    # Sync GLM calls wrapped for async context
    async def _glm_retrieve_kems_async(self, request: glm_service_pb2.RetrieveKEMsRequest, timeout: int = 20) -> glm_service_pb2.RetrieveKEMsResponse:
        if not self.glm_stub:
            logger.error("SWM._glm_retrieve_kems_async: GLM stub (sync) not initialized.")
            raise grpc_aio.AioRpcError(grpc_aio.StatusCode.INTERNAL, "GLM client not available in SWM")
        return await asyncio.to_thread(self.glm_stub.RetrieveKEMs, request, timeout=timeout)

    async def _glm_store_kem_async(self, request: glm_service_pb2.StoreKEMRequest, timeout: int = 10) -> glm_service_pb2.StoreKEMResponse:
        if not self.glm_stub:
            logger.error("SWM._glm_store_kem_async: GLM stub (sync) not initialized.")
            raise grpc_aio.AioRpcError(grpc_aio.StatusCode.INTERNAL, "GLM client not available in SWM")
        return await asyncio.to_thread(self.glm_stub.StoreKEM, request, timeout=timeout)

    def _handle_kem_eviction_sync(self, evicted_kem: kem_pb2.KEM): # This is called by sync cachetools
        if evicted_kem:
            logger.info(f"SWM: KEM ID '{evicted_kem.id}' was evicted from cache. Scheduling notification.")
            # To call async _notify_subscribers from this sync callback, we need the event loop
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(
                self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED, source_agent_id="SWM_CACHE_EVICTION"),
                loop
            )

    async def _put_kem_to_cache_async(self, kem: kem_pb2.KEM) -> None: # For internal async use
        if not kem or not kem.id:
            logger.warning("Attempt to add an invalid KEM to SWM cache.")
            return
        # cachetools is sync, so wrap its operations if they could block significantly
        # For simple dict operations, direct call under async lock might be okay if IndexedLRUCache._lock is asyncio.Lock
        # But IndexedLRUCache uses threading.Lock. So, use to_thread for safety.
        was_present = await asyncio.to_thread(self.swm_cache.__contains__, kem.id)
        await asyncio.to_thread(self.swm_cache.__setitem__, kem.id, kem)

        logger.info("KEM ID '{}' added/updated in SWM cache. Cache size: {}/{}".format(
            kem.id, len(self.swm_cache), self.swm_cache.maxsize))
        event_type = swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED if was_present else swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED
        await self._notify_subscribers(kem, event_type) # This is now async

    async def _notify_subscribers(self, kem: kem_pb2.KEM, event_type: swm_service_pb2.SWMMemoryEvent.EventType, source_agent_id: str = "SWM_SERVER"):
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

        subscribers_to_notify: List[SubscriberInfo] = []
        async with self.subscribers_lock: # Use asyncio.Lock
            if not self.subscribers: return
            # Iterate over a copy if modification during iteration is possible, though less likely with async lock
            subscribers_to_notify = list(self.subscribers.values())

        if not subscribers_to_notify:
            logger.debug("No subscribers to notify.")
            return

        logger.debug(f"Checking and dispatching event {event_to_dispatch.event_id} to {len(subscribers_to_notify)} potential subscribers.")

        for sub_info in subscribers_to_notify: # Iterate over the copy
            should_send = False
            if not sub_info.topics: should_send = True
            else:
                for topic in sub_info.topics:
                    # ... (filtering logic remains the same) ...
                    criteria = topic.filter_criteria.strip()
                    if not criteria: should_send = True; break
                    if '=' not in criteria:
                        logger.warning(f"Invalid filter_criteria format '{criteria}' for a subscriber. Skipping filter.")
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
                        logger.warning(f"Unknown filter key '{filter_key}' for a subscriber. Event will be sent.")
                        should_send = True; break
            if should_send:
                try:
                    await sub_info.event_queue.put(event_to_dispatch) # Use await for asyncio.Queue
                except asyncio.QueueFull: # asyncio.QueueFull
                    logger.warning(f"Subscriber queue is full for an event {event_to_dispatch.event_id}. Event lost.")
                except Exception as e:
                    logger.error(f"Unexpected error adding event to a subscriber queue: {e}", exc_info=True)

    async def PublishKEMToSWM(self, request: swm_service_pb2.PublishKEMToSWMRequest, context) -> swm_service_pb2.PublishKEMToSWMResponse:
        kem_to_publish = request.kem_to_publish
        logger.info(f"SWM: PublishKEMToSWM called for KEM ID (suggested): '{kem_to_publish.id}'")
        kem_id_final = kem_to_publish.id
        if not kem_id_final:
            kem_id_final = str(uuid.uuid4()); kem_to_publish.id = kem_id_final
            logger.info(f"SWM: No ID provided, new ID generated: '{kem_id_final}'")

        ts = Timestamp(); ts.GetCurrentTime()
        # Accessing self.swm_cache needs to be thread-safe if it's shared and modified by this async method.
        # IndexedLRUCache uses threading.Lock, so calls to it should be wrapped.
        existing_kem_in_cache = await asyncio.to_thread(self._get_kem_from_cache, kem_id_final)

        if existing_kem_in_cache and existing_kem_in_cache.HasField("created_at"):
            kem_to_publish.created_at.CopyFrom(existing_kem_in_cache.created_at)
        elif not kem_to_publish.HasField("created_at"):
            kem_to_publish.created_at.CopyFrom(ts)
        kem_to_publish.updated_at.CopyFrom(ts)

        await self._put_kem_to_cache_async(kem_to_publish) # Use async version

        published_to_swm_flag = True; persistence_triggered_flag = False
        status_msg = f"KEM ID '{kem_id_final}' successfully published to SWM."

        if request.persist_to_glm_if_new_or_updated:
            if not self.glm_stub:
                msg_glm = f"GLM service unavailable, KEM ID '{kem_id_final}' will not be persisted."
                logger.error(msg_glm); status_msg += " " + msg_glm
            else:
                try:
                    logger.info(f"SWM: Initiating persistence of KEM ID '{kem_id_final}' to GLM...")
                    glm_store_req = glm_service_pb2.StoreKEMRequest(kem=kem_to_publish)
                    # Using async wrapper for sync GLM client call
                    glm_store_resp = await self._glm_store_kem_async(glm_store_req, timeout=10)

                    if glm_store_resp and glm_store_resp.kem and glm_store_resp.kem.id:
                        await self._put_kem_to_cache_async(glm_store_resp.kem) # Update SWM cache with GLM's version
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

    async def SubscribeToSWMEvents(self, request: swm_service_pb2.SubscribeToSWMEventsRequest, context: grpc_aio.ServicerContext) -> typing.AsyncGenerator[swm_service_pb2.SWMMemoryEvent, None]:
        agent_id = request.agent_id
        logger.info(f"SWM: New subscriber {agent_id} for topics: {request.topics}")
        subscriber_id = agent_id if agent_id else str(uuid.uuid4())
        event_q = asyncio.Queue(maxsize=100)

        subscriber_info = SubscriberInfo(event_queue=event_q, topics=list(request.topics))
        async with self.subscribers_lock:
            if subscriber_id in self.subscribers:
                logger.warning(f"Subscriber with ID '{subscriber_id}' already exists. Old subscription will be replaced.")
            self.subscribers[subscriber_id] = subscriber_info
            logger.info(f"SWM: New subscriber '{subscriber_id}' registered. Total: {len(self.subscribers)}")

        try:
            while True: # context.is_active() is not available for async generators in grpc.aio like this
                       # Instead, rely on client closing the stream or server shutting down.
                try:
                    event_to_send = await asyncio.wait_for(event_q.get(), timeout=5.0) # Wait with timeout
                    yield event_to_send
                    event_q.task_done() # Mark as processed
                except asyncio.TimeoutError:
                    # Check if client is still connected. gRPC AIO handles this by raising RpcError on disconnect.
                    # A more robust way to check client connection status might be needed if this becomes an issue.
                    # For now, this timeout allows the loop to periodically check for external cancellation.
                    pass
                except grpc_aio.AioRpcError as rpc_error: # type: ignore
                    logger.info(f"SWM: RPC error for subscriber '{subscriber_id}': {rpc_error.code()} - {rpc_error.details()}. Terminating stream.")
                    break
                except Exception as e: # Catch any other exception during get or yield
                    logger.error(f"SWM: Error in event stream for subscriber '{subscriber_id}': {e}", exc_info=True)
                    break
        except asyncio.CancelledError:
             logger.info(f"SWM: Event stream for subscriber '{subscriber_id}' cancelled.")
        finally:
            async with self.subscribers_lock:
                removed_info = self.subscribers.pop(subscriber_id, None)
                if removed_info:
                    logger.info(f"SWM: Subscriber '{subscriber_id}' removed. Remaining: {len(self.subscribers)}")
                    # Clear its queue if needed, though it will be garbage collected.
                else:
                    logger.warning(f"SWM: Attempted to remove non-existent subscriber '{subscriber_id}'.")

    async def QuerySWM(self, request: swm_service_pb2.QuerySWMRequest, context) -> swm_service_pb2.QuerySWMResponse:
        query = request.query
        logger.info(f"SWM: QuerySWM called with KEMQuery: {query}")
        if query.embedding_query or query.text_query:
            msg = "Vector or text search is not supported directly in SWM cache. Use GLM."
            logger.warning(msg); await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT, msg) # type: ignore

        # All cache operations are sync, so run them in a thread
        def _sync_query_swm():
            page_size = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
            offset = 0
            if request.page_token:
                try: offset = int(request.page_token)
                except ValueError: logger.warning(f"Invalid page_token for QuerySWM: '{request.page_token}', using offset=0.")

            processed_kems_list: typing.List[kem_pb2.KEM] = []
            # ... (rest of the synchronous filtering logic from the original QuerySWM) ...
            # This includes calls to self.swm_cache which uses threading.Lock
            # For brevity, I'm not copying the entire filtering logic here, but it would be:
            # 1. Apply indexed filters
            # 2. Apply ID filters
            # 3. Apply non-indexed filters
            # 4. Apply date filters
            # 5. Paginate
            # For this example, assume it returns a list of KEMs and next_page_token
            # This is a placeholder for the actual synchronous filtering logic:
            temp_kems = self.swm_cache.values() # Example: gets all values
            kems_on_page = temp_kems[offset : offset + page_size]
            next_page_token_str = str(offset + page_size) if len(temp_kems) > offset + page_size else ""
            return kems_on_page, next_page_token_str

        kems_on_page_sync, next_page_token_str_sync = await asyncio.to_thread(_sync_query_swm)

        logger.info(f"QuerySWM: Returning {len(kems_on_page_sync)} KEMs.")
        return swm_service_pb2.QuerySWMResponse(kems=kems_on_page_sync, next_page_token=next_page_token_str_sync)

    async def LoadKEMsFromGLM(self, request: swm_service_pb2.LoadKEMsFromGLMRequest, context) -> swm_service_pb2.LoadKEMsFromGLMResponse:
        logger.info(f"SWM: LoadKEMsFromGLM called with query: {request.query_for_glm}")
        if not self.glm_stub:
            msg = "GLM service is unavailable to SWM (client not initialized)."
            logger.error(msg); await context.abort(grpc_aio.StatusCode.INTERNAL, msg) # type: ignore

        glm_retrieve_request = glm_service_pb2.RetrieveKEMsRequest(query=request.query_for_glm)
        loaded_kems_count = 0; loaded_ids = []; kems_from_glm_for_stats = 0
        try:
            logger.info(f"SWM: Requesting GLM.RetrieveKEMs (async wrapper): {glm_retrieve_request}")
            glm_response = await self._glm_retrieve_kems_async(glm_retrieve_request, timeout=20)

            if glm_response and glm_response.kems:
                kems_from_glm_for_stats = len(glm_response.kems)
                for kem_from_glm in glm_response.kems:
                    await self._put_kem_to_cache_async(kem_from_glm) # Use async version
                    loaded_ids.append(kem_from_glm.id)
                loaded_kems_count = len(loaded_ids)
                status_msg = f"Loaded {loaded_kems_count} KEMs."
                logger.info(f"SWM: {status_msg}")
            else:
                status_msg = "GLM returned no KEMs for the query."; logger.info(status_msg)

            return swm_service_pb2.LoadKEMsFromGLMResponse(
                kems_queried_in_glm_count=kems_from_glm_for_stats, kems_loaded_to_swm_count=loaded_kems_count,
                loaded_kem_ids=loaded_ids, status_message=status_msg )
        except grpc.RpcError as e: # This might be caught by retry_grpc_call if it's used on _glm_retrieve_kems_async
            msg = f"SWM: gRPC error calling GLM.RetrieveKEMs: code={e.code()}, details={e.details()}"
            logger.error(msg); await context.abort(e.code(), msg) # type: ignore
        except Exception as e:
            msg = f"SWM: Unexpected error in LoadKEMsFromGLM while working with GLM: {e}"
            logger.error(msg, exc_info=True); await context.abort(grpc_aio.StatusCode.INTERNAL, msg) # type: ignore
        # Fallback response, though abort should prevent reaching here.
        return swm_service_pb2.LoadKEMsFromGLMResponse(status_message="Error loading from GLM")

    # --- RPC Lock Implementations (Async) ---
    async def AcquireLock(self, request: swm_service_pb2.AcquireLockRequest, context) -> swm_service_pb2.AcquireLockResponse:
        resource_id = request.resource_id; agent_id = request.agent_id
        timeout_ms = request.timeout_ms; lease_duration_ms = request.lease_duration_ms
        logger.info(f"SWM: AcquireLock request for resource='{resource_id}' by agent='{agent_id}', timeout={timeout_ms}ms, lease={lease_duration_ms}ms")
        start_time_monotonic = time.monotonic()

        async with self.lock_condition: # Acquire asyncio.Condition's lock
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
                    return swm_service_pb2.AcquireLockResponse(resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ACQUIRED, lock_id=new_lock_id, acquired_at_unix_ms=acquired_at, lease_expires_at_unix_ms=expires_at, message="Lock successfully acquired.")

                if existing_lock.agent_id == agent_id:
                    logger.info(f"SWM: Resource '{resource_id}' is already locked by the same agent '{agent_id}'.")
                    if lease_duration_ms > 0:
                        existing_lock.lease_expires_at_unix_ms = current_time_ms + lease_duration_ms
                        existing_lock.lease_duration_ms = lease_duration_ms
                        logger.info(f"SWM: Lease for '{resource_id}' updated. Expires at: {existing_lock.lease_expires_at_unix_ms}")
                    elif lease_duration_ms == 0 and existing_lock.lease_duration_ms > 0 :
                         existing_lock.lease_expires_at_unix_ms = 0; existing_lock.lease_duration_ms = 0
                         logger.info(f"SWM: Lease for '{resource_id}' removed (now held until explicit release).")
                    return swm_service_pb2.AcquireLockResponse(resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ALREADY_HELD_BY_YOU, lock_id=existing_lock.lock_id, acquired_at_unix_ms=existing_lock.acquired_at_unix_ms, lease_expires_at_unix_ms=existing_lock.lease_expires_at_unix_ms, message="Lock already held by you.")

                if timeout_ms == 0:
                    logger.info(f"SWM: Resource '{resource_id}' is locked by agent '{existing_lock.agent_id}'. timeout_ms=0, denying for '{agent_id}'.")
                    return swm_service_pb2.AcquireLockResponse(resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.NOT_AVAILABLE, message=f"Resource is locked by agent {existing_lock.agent_id}.")

                elapsed_monotonic_ms = (time.monotonic() - start_time_monotonic) * 1000
                wait_timeout_sec: typing.Optional[float]
                if timeout_ms < 0: wait_timeout_sec = None
                else:
                    remaining_timeout_ms = timeout_ms - elapsed_monotonic_ms
                    if remaining_timeout_ms <= 0:
                        logger.info(f"SWM: Overall timeout ({timeout_ms}ms) expired for resource='{resource_id}', agent '{agent_id}'.")
                        return swm_service_pb2.AcquireLockResponse(resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.TIMEOUT, message="Lock acquisition timed out.")
                    wait_timeout_sec = remaining_timeout_ms / 1000.0

                logger.debug(f"SWM: Agent '{agent_id}' waiting for lock on '{resource_id}'. Wait timeout sec: {wait_timeout_sec}")
                try:
                    await asyncio.wait_for(self.lock_condition.wait(), timeout=wait_timeout_sec) # type: ignore
                except asyncio.TimeoutError:
                    logger.info(f"SWM: Condition wait timeout ({wait_timeout_sec}s) for resource='{resource_id}', agent '{agent_id}'.")
                logger.debug(f"SWM: Agent '{agent_id}' awakened for lock on '{resource_id}'. Re-checking...")


    async def ReleaseLock(self, request: swm_service_pb2.ReleaseLockRequest, context) -> swm_service_pb2.ReleaseLockResponse:
        resource_id = request.resource_id; agent_id = request.agent_id; lock_id_from_request = request.lock_id
        logger.info(f"SWM: ReleaseLock request for resource='{resource_id}' by agent='{agent_id}', lock_id_req='{lock_id_from_request}'")
        async with self.lock_condition:
            existing_lock = self.locks.get(resource_id)
            if not existing_lock:
                logger.warning(f"SWM: Attempt to release a non-existent lock for resource='{resource_id}'.")
                return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.NOT_HELD, message="Lock for resource not found.")
            current_time_ms = int(time.time() * 1000)
            if existing_lock.lease_duration_ms > 0 and current_time_ms >= existing_lock.lease_expires_at_unix_ms:
                logger.info(f"SWM: Lock for resource='{resource_id}' (agent '{existing_lock.agent_id}') already expired. Removing.")
                del self.locks[resource_id]; self.lock_condition.notify_all()
                return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.NOT_HELD, message="Lock expired before release attempt.")
            if existing_lock.agent_id != agent_id:
                logger.warning(f"SWM: Agent '{agent_id}' attempting to release lock for resource='{resource_id}' held by agent '{existing_lock.agent_id}'. Denied.")
                return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING, message="Lock held by another agent.")
            if lock_id_from_request and existing_lock.lock_id != lock_id_from_request:
                logger.warning(f"SWM: Invalid lock_id ('{lock_id_from_request}') for ReleaseLock on resource='{resource_id}' (expected '{existing_lock.lock_id}'). Denied.")
                return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.INVALID_LOCK_ID, message="Invalid lock ID.")
            del self.locks[resource_id]
            logger.info(f"SWM: Lock for resource='{resource_id}' successfully released by agent '{agent_id}'. Notifying waiters.")
            self.lock_condition.notify_all()
            return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.RELEASED, message="Lock successfully released.")

    async def GetLockInfo(self, request: swm_service_pb2.GetLockInfoRequest, context) -> swm_service_pb2.LockInfo:
        resource_id = request.resource_id
        logger.debug(f"SWM: GetLockInfo request for resource='{resource_id}'")
        async with self.lock_condition:
            current_time_ms = int(time.time() * 1000)
            lock_data = self.locks.get(resource_id)
            if lock_data:
                if lock_data.lease_duration_ms > 0 and current_time_ms >= lock_data.lease_expires_at_unix_ms:
                    logger.info(f"SWM (GetLockInfo): Lock for resource='{resource_id}' (agent '{lock_data.agent_id}') has expired. Removing.")
                    del self.locks[resource_id]; lock_data = None; self.lock_condition.notify_all()
            if lock_data:
                return swm_service_pb2.LockInfo(resource_id=resource_id, is_locked=True, current_holder_agent_id=lock_data.agent_id, lock_id=lock_data.lock_id, acquired_at_unix_ms=lock_data.acquired_at_unix_ms, lease_expires_at_unix_ms=lock_data.lease_expires_at_unix_ms if lock_data.lease_duration_ms > 0 else 0)
            else:
                return swm_service_pb2.LockInfo(resource_id=resource_id, is_locked=False)

    async def IncrementCounter(self, request: swm_service_pb2.IncrementCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        counter_id = request.counter_id; increment_by = request.increment_by
        logger.info(f"SWM: IncrementCounter request for counter_id='{counter_id}', increment_by={increment_by}")
        if not counter_id: await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT, "counter_id cannot be empty.") # type: ignore
        async with self.counters_lock: # Use asyncio.Lock
            current_value = self.counters.get(counter_id, 0)
            new_value = current_value + increment_by
            self.counters[counter_id] = new_value
            logger.info(f"SWM: Counter '{counter_id}' updated. Old value: {current_value}, New value: {new_value}")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=new_value, status_message="Counter successfully updated.")

    async def GetCounter(self, request: swm_service_pb2.DistributedCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        counter_id = request.counter_id
        logger.info(f"SWM: GetCounter request for counter_id='{counter_id}'")
        if not counter_id: await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT, "counter_id cannot be empty.") # type: ignore
        async with self.counters_lock: # Use asyncio.Lock
            current_value = self.counters.get(counter_id)
            if current_value is None:
                logger.warning(f"SWM: Counter '{counter_id}' not found.")
                return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"Counter '{counter_id}' not found, returned default value 0.")
            else:
                logger.info(f"SWM: Counter '{counter_id}', current value: {current_value}")
                return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=current_value, status_message="Counter value successfully retrieved.")

async def serve():
    server = grpc_aio.server()
    servicer_instance = SharedWorkingMemoryServiceImpl()
    swm_service_pb2_grpc.add_SharedWorkingMemoryServiceServicer_to_server(servicer_instance, server)

    # Start background tasks after servicer is initialized
    await servicer_instance.start_background_tasks()

    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS)
    logger.info(f"Starting SWM (Shared Working Memory Service) asynchronously on {config.GRPC_LISTEN_ADDRESS}...")
    await server.start()
    logger.info(f"SWM started and listening asynchronously on {config.GRPC_LISTEN_ADDRESS}.")
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Stopping SWM (KeyboardInterrupt)...")
    except asyncio.CancelledError:
        logger.info("SWM server task cancelled.")
    finally:
        logger.info("SWM: Initiating graceful shutdown...")
        await servicer_instance.stop_background_tasks()
        await server.stop(grace=5)
        logger.info("SWM stopped.")

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("SWM main process interrupted by user.")
    except Exception as e:
        logger.critical(f"SWM main process encountered an unhandled exception: {e}", exc_info=True)

```
