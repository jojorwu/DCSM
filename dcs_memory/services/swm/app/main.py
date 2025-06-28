import grpc
import grpc.aio as grpc_aio
import asyncio
import time
import sys
import os
import uuid
import logging
import threading # Still needed for IndexedLRUCache's internal lock if not changed to RWLock from an async-compatible library
import random

from dataclasses import dataclass, field
from cachetools import LRUCache, Cache # Used by IndexedLRUCache
from google.protobuf.timestamp_pb2 import Timestamp
from readerwriterlock import rwlock # Assuming this is the chosen RWLock library

import typing
from typing import Optional, List, Set, Dict, Callable, AsyncGenerator, Tuple

from .config import SWMConfig
from .managers import SubscriptionManager, DistributedLockManager, DistributedCounterManager # Import managers

logger = logging.getLogger(__name__)
# BasicConfig removed, assuming it's set once at the top level or by the framework

# --- gRPC Code Import Block ---
# Assuming paths are correctly set by the execution environment or a common entry point
from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2 # For KEMQuery if used by GLM client parts
from generated_grpc import glm_service_pb2_grpc
from generated_grpc import swm_service_pb2
from generated_grpc import swm_service_pb2_grpc
# --- End gRPC Code Import Block ---

# IndexedLRUCache remains here as it's tightly coupled with SWM's core KEM caching logic
# If it were more generic, it could be in common/utils or similar.
class IndexedLRUCache(Cache):
    def __init__(self, maxsize, indexed_keys: typing.List[str], on_evict_callback: typing.Optional[typing.Callable[[kem_pb2.KEM], None]] = None):
        super().__init__(maxsize)
        self._lru = LRUCache(maxsize=maxsize)
        self._indexed_keys = set(indexed_keys)
        self._metadata_indexes: typing.Dict[str, typing.Dict[str, typing.Set[str]]] = {key: {} for key in self._indexed_keys}
        self._rw_lock = rwlock.RWLockFair()
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
                    if not self._metadata_indexes[meta_key][original_value]: del self._metadata_indexes[meta_key][original_value]
                    if not self._metadata_indexes[meta_key]: del self._metadata_indexes[meta_key]

    def __setitem__(self, kem_id: str, kem: kem_pb2.KEM):
        with self._rw_lock.gen_wlock():
            evicted_kem = None
            if kem_id in self._lru:
                old_kem = self._lru[kem_id]
                self._remove_from_metadata_indexes(old_kem)
            elif len(self._lru) >= self._lru.maxsize:
                _evicted_id, evicted_kem = self._lru.popitem()
                if evicted_kem:
                    self._remove_from_metadata_indexes(evicted_kem)
                    if self._on_evict_callback:
                        try: self._on_evict_callback(evicted_kem)
                        except Exception as e_cb: logger.error(f"Error in on_evict_callback for KEM ID '{_evicted_id}': {e_cb}", exc_info=True)
            self._lru[kem_id] = kem
            self._add_to_metadata_indexes(kem)

    def __getitem__(self, kem_id: str) -> kem_pb2.KEM:
        with self._rw_lock.gen_rlock(): return self._lru[kem_id]
    def __delitem__(self, kem_id: str):
        with self._rw_lock.gen_wlock():
            if kem_id in self._lru: kem_to_remove = self._lru.pop(kem_id); self._remove_from_metadata_indexes(kem_to_remove)
            else: raise KeyError(kem_id)
    def get(self, kem_id: str, default=None) -> typing.Optional[kem_pb2.KEM]:
        with self._rw_lock.gen_rlock(): return self._lru.get(kem_id, default)
    def pop(self, kem_id: str, default=object()) -> kem_pb2.KEM:
        with self._rw_lock.gen_wlock():
            if kem_id in self._lru: kem = self._lru.pop(kem_id); self._remove_from_metadata_indexes(kem); return kem
            elif default is not object(): return default # type: ignore
            else: raise KeyError(kem_id)
    def __len__(self) -> int:
        with self._rw_lock.gen_rlock(): return len(self._lru)
    def __contains__(self, kem_id: str) -> bool:
        with self._rw_lock.gen_rlock(): return kem_id in self._lru
    def values(self) -> List[kem_pb2.KEM]:
        with self._rw_lock.gen_rlock(): return list(self._lru.values())
    def items(self) -> List[Tuple[str, kem_pb2.KEM]]:
        with self._rw_lock.gen_rlock(): return list(self._lru.items())
    def clear(self):
        with self._rw_lock.gen_wlock(): self._lru.clear(); self._metadata_indexes.clear(); self._metadata_indexes = {key: {} for key in self._indexed_keys}
    @property
    def maxsize(self): return self._lru.maxsize
    def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Set[str]:
        if meta_key not in self._indexed_keys: return set()
        with self._rw_lock.gen_rlock(): return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()

class SharedWorkingMemoryServiceImpl(swm_service_pb2_grpc.SharedWorkingMemoryServiceServicer):
    def __init__(self):
        self.config = SWMConfig()
        logger.setLevel(self.config.get_log_level_int())
        logger.info(f"Initializing SharedWorkingMemoryServiceImpl... Cache size: {self.config.CACHE_MAX_SIZE}, Indexed keys: {self.config.INDEXED_METADATA_KEYS}")

        self.aio_glm_channel: Optional[grpc_aio.Channel] = None
        self.aio_glm_stub: Optional[glm_service_pb2_grpc.GlobalLongTermMemoryStub] = None
        self.retryable_error_codes = (
            grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.INTERNAL, grpc.StatusCode.RESOURCE_EXHAUSTED
        )
        try:
            if self.config.GLM_SERVICE_ADDRESS: # Only init if address is configured
                self.aio_glm_channel = grpc_aio.insecure_channel(self.config.GLM_SERVICE_ADDRESS)
                self.aio_glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.aio_glm_channel)
                logger.info(f"GLM client (async) for SWM initialized, target: {self.config.GLM_SERVICE_ADDRESS}")
            else:
                logger.warning("GLM_SERVICE_ADDRESS not configured. GLM features will be unavailable in SWM.")
        except Exception as e:
            logger.error(f"Error initializing asynchronous GLM client in SWM: {e}", exc_info=True)
            self.aio_glm_stub = None
            if self.aio_glm_channel: logger.warning("SWM: GLM async channel was created but stub initialization failed.")

        self.swm_cache = IndexedLRUCache(
            maxsize=self.config.CACHE_MAX_SIZE,
            indexed_keys=self.config.INDEXED_METADATA_KEYS,
            on_evict_callback=self._handle_kem_eviction_for_notification # Changed callback name
        )

        # Initialize managers
        self.subscription_manager = SubscriptionManager(self.config)
        self.lock_manager = DistributedLockManager(self.config)
        self.counter_manager = DistributedCounterManager(self.config)

        self._stop_event = asyncio.Event()
        self._glm_persistence_worker_task: Optional[asyncio.Task] = None
        self.glm_persistence_queue: asyncio.Queue[kem_pb2.KEM] = asyncio.Queue(
            maxsize=self.config.GLM_PERSISTENCE_QUEUE_MAX_SIZE
        )
        logger.info("SharedWorkingMemoryServiceImpl initialized with managers.")

    async def start_background_tasks(self):
        self._stop_event.clear()
        await self.lock_manager.start_cleanup_task() # Delegate to manager

        if self._glm_persistence_worker_task is None or self._glm_persistence_worker_task.done():
            self._glm_persistence_worker_task = asyncio.create_task(self._glm_persistence_worker())
            logger.info("SWM: GLM persistence worker task started.")

    async def stop_background_tasks(self):
        logger.info("SWM: Stopping background tasks...")
        self._stop_event.set()
        await self.lock_manager.stop_cleanup_task() # Delegate to manager

        if self._glm_persistence_worker_task and not self._glm_persistence_worker_task.done():
            logger.info("SWM: Waiting for GLM persistence worker task to stop...")
            try:
                await asyncio.wait_for(self._glm_persistence_worker_task, timeout=self.config.GLM_PERSISTENCE_FLUSH_INTERVAL_S + 5)
            except asyncio.TimeoutError:
                logger.warning("SWM: GLM persistence worker task did not finish processing queue in time during shutdown.")
                self._glm_persistence_worker_task.cancel()
                try: await self._glm_persistence_worker_task
                except asyncio.CancelledError: logger.info("SWM: GLM persistence worker task was cancelled during shutdown after timeout.")
            except asyncio.CancelledError: logger.info("SWM: GLM persistence worker task was cancelled by stop_event.")
        self._glm_persistence_worker_task = None

        if self.aio_glm_channel:
            logger.info("SWM: Closing GLM client (async) channel.")
            try: await self.aio_glm_channel.close()
            except Exception as e: logger.error(f"SWM: Error closing GLM async channel: {e}", exc_info=True)
            self.aio_glm_channel = None
            self.aio_glm_stub = None

    # --- GLM Client Methods (Retry logic remains here, could be part of a GLMClient class later) ---
    async def _glm_retrieve_kems_async(self, request: glm_service_pb2.RetrieveKEMsRequest, timeout: int = 20) -> glm_service_pb2.RetrieveKEMsResponse:
        if not self.aio_glm_stub:
            logger.error("SWM: GLM async client not available for RetrieveKEMs.")
            raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "GLM client not available")
        current_delay_s = self.config.GLM_RETRY_INITIAL_DELAY_S
        for attempt in range(1, self.config.GLM_RETRY_MAX_ATTEMPTS + 1):
            try:
                logger.debug(f"Attempt {attempt} to call GLM RetrieveKEMs (async).")
                return await self.aio_glm_stub.RetrieveKEMs(request, timeout=timeout)
            except grpc_aio.AioRpcError as e:
                if e.code() in self.retryable_error_codes:
                    if attempt == self.config.GLM_RETRY_MAX_ATTEMPTS:
                        logger.error(f"GLM call RetrieveKEMs failed after {self.config.GLM_RETRY_MAX_ATTEMPTS} attempts. Last error: {e.code()} - {e.details()}", exc_info=False); raise
                    jitter_fraction = 0.1; jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                    actual_delay_s = max(0, current_delay_s + jitter_value)
                    logger.warning(f"GLM call RetrieveKEMs failed (attempt {attempt}/{self.config.GLM_RETRY_MAX_ATTEMPTS}) with status {e.code()}. Retrying in {actual_delay_s:.2f}s. Details: {e.details()}", exc_info=False)
                    await asyncio.sleep(actual_delay_s)
                    current_delay_s *= self.config.GLM_RETRY_BACKOFF_FACTOR
                else: logger.error(f"GLM call RetrieveKEMs failed with non-retryable status {e.code()}. Details: {e.details()}", exc_info=True); raise
            except Exception as e_generic:
                logger.error(f"A non-gRPC error occurred during GLM call RetrieveKEMs (attempt {attempt}/{self.config.GLM_RETRY_MAX_ATTEMPTS}): {e_generic}", exc_info=True)
                if attempt == self.config.GLM_RETRY_MAX_ATTEMPTS: raise
                jitter_fraction = 0.1; jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                actual_delay_s = max(0, current_delay_s + jitter_value)
                logger.info(f"Retrying GLM RetrieveKEMs after non-gRPC error in {actual_delay_s:.2f}s.")
                await asyncio.sleep(actual_delay_s); current_delay_s *= self.config.GLM_RETRY_BACKOFF_FACTOR
        logger.error("GLM RetrieveKEMs exhausted attempts unexpectedly."); raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "GLM RetrieveKEMs exhausted attempts")

    async def _glm_store_kem_async(self, request: glm_service_pb2.StoreKEMRequest, timeout: int = 10) -> glm_service_pb2.StoreKEMResponse:
        if not self.aio_glm_stub:
            logger.error("SWM: GLM async client not available for StoreKEM."); raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "GLM client not available")
        current_delay_s = self.config.GLM_RETRY_INITIAL_DELAY_S
        for attempt in range(1, self.config.GLM_RETRY_MAX_ATTEMPTS + 1):
            try:
                logger.debug(f"Attempt {attempt} to call GLM StoreKEM (async).")
                return await self.aio_glm_stub.StoreKEM(request, timeout=timeout)
            except grpc_aio.AioRpcError as e:
                if e.code() in self.retryable_error_codes:
                    if attempt == self.config.GLM_RETRY_MAX_ATTEMPTS:
                        logger.error(f"GLM call StoreKEM failed after {self.config.GLM_RETRY_MAX_ATTEMPTS} attempts. Last error: {e.code()} - {e.details()}", exc_info=False); raise
                    jitter_fraction = 0.1; jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                    actual_delay_s = max(0, current_delay_s + jitter_value)
                    logger.warning(f"GLM call StoreKEM failed (attempt {attempt}/{self.config.GLM_RETRY_MAX_ATTEMPTS}) with status {e.code()}. Retrying in {actual_delay_s:.2f}s. Details: {e.details()}", exc_info=False)
                    await asyncio.sleep(actual_delay_s); current_delay_s *= self.config.GLM_RETRY_BACKOFF_FACTOR
                else: logger.error(f"GLM call StoreKEM failed with non-retryable status {e.code()}. Details: {e.details()}", exc_info=True); raise
            except Exception as e_generic:
                logger.error(f"A non-gRPC error occurred during GLM call StoreKEM (attempt {attempt}/{self.config.GLM_RETRY_MAX_ATTEMPTS}): {e_generic}", exc_info=True)
                if attempt == self.config.GLM_RETRY_MAX_ATTEMPTS: raise
                jitter_fraction = 0.1; jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                actual_delay_s = max(0, current_delay_s + jitter_value)
                logger.info(f"Retrying GLM StoreKEM after non-gRPC error in {actual_delay_s:.2f}s.")
                await asyncio.sleep(actual_delay_s); current_delay_s *= self.config.GLM_RETRY_BACKOFF_FACTOR
        logger.error("GLM StoreKEM exhausted attempts unexpectedly."); raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "GLM StoreKEM exhausted attempts")

    async def _glm_batch_store_kems_async(self, kems_batch: List[kem_pb2.KEM], timeout: int = 30) -> Optional[glm_service_pb2.BatchStoreKEMsResponse]:
        if not self.aio_glm_stub: logger.error("SWM: GLM async client not available for BatchStoreKEMs."); return None
        if not kems_batch: return None
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_batch)
        try:
            logger.info(f"SWM: Calling GLM BatchStoreKEMs with {len(kems_batch)} KEMs.")
            response = await self.aio_glm_stub.BatchStoreKEMs(request, timeout=timeout)
            if response.failed_kem_references: logger.warning(f"SWM: GLM BatchStoreKEMs reported {len(response.failed_kem_references)} failures. Refs: {response.failed_kem_references}")
            logger.info(f"SWM: GLM BatchStoreKEMs processed. Success: {len(response.successfully_stored_kems)}, Failures: {len(response.failed_kem_references)}")
            return response
        except grpc_aio.AioRpcError as e: logger.error(f"SWM: gRPC error during GLM BatchStoreKEMs: {e.code()} - {e.details()}", exc_info=True)
        except Exception as e_generic: logger.error(f"SWM: Non-gRPC error during GLM BatchStoreKEMs: {e_generic}", exc_info=True)
        return None

    async def _glm_persistence_worker(self):
        logger.info("SWM: GLM Persistence Worker started.")
        while not self._stop_event.is_set():
            batch_to_persist: List[kem_pb2.KEM] = []
            try:
                first_kem = await asyncio.wait_for(self.glm_persistence_queue.get(),timeout=self.config.GLM_PERSISTENCE_FLUSH_INTERVAL_S)
                batch_to_persist.append(first_kem); self.glm_persistence_queue.task_done()
                while len(batch_to_persist) < self.config.GLM_PERSISTENCE_BATCH_SIZE:
                    try: kem = self.glm_persistence_queue.get_nowait(); batch_to_persist.append(kem); self.glm_persistence_queue.task_done()
                    except asyncio.QueueEmpty: break
            except asyncio.TimeoutError: pass
            except asyncio.CancelledError: logger.info("SWM: GLM Persistence Worker cancelled during queue get."); break
            except Exception as e_q_get: logger.error(f"SWM: GLM Persistence Worker error getting from queue: {e_q_get}", exc_info=True); await asyncio.sleep(1); continue
            if batch_to_persist:
                logger.info(f"SWM: GLM Persistence Worker processing batch of {len(batch_to_persist)} KEMs.")
                if self.aio_glm_stub:
                    glm_response = await self._glm_batch_store_kems_async(batch_to_persist)
                    if glm_response:
                        # TODO: Update SWM cache from glm_response.successfully_stored_kems if IDs/timestamps changed
                        pass
                    else:
                        logger.error(f"SWM: GLM Persistence Worker: Batch of {len(batch_to_persist)} KEMs failed to persist or no response from GLM.")
                        logger.warning(f"SWM: GLM Persistence Worker: Re-queueing {len(batch_to_persist)} KEMs due to GLM processing failure.")
                        for kem_to_requeue in reversed(batch_to_persist):
                             if not self.glm_persistence_queue.full(): await self.glm_persistence_queue.put(kem_to_requeue)
                             else: logger.error(f"SWM: GLM Persistence Worker: Failed to re-queue KEM ID '{kem_to_requeue.id}', persistence queue full."); break
                else:
                    logger.error("SWM: GLM Persistence Worker: GLM stub not available. Batch not persisted.")
            if self._stop_event.is_set() and self.glm_persistence_queue.empty():
                 logger.info("SWM: GLM Persistence Worker stopping as stop event is set and queue is empty."); break
        if not self.glm_persistence_queue.empty():
            logger.info(f"SWM: GLM Persistence Worker - processing remaining {self.glm_persistence_queue.qsize()} items on shutdown...")
            final_batch: List[kem_pb2.KEM] = []
            while not self.glm_persistence_queue.empty():
                try: final_batch.append(self.glm_persistence_queue.get_nowait()); self.glm_persistence_queue.task_done()
                except asyncio.QueueEmpty: break
            if final_batch and self.aio_glm_stub:
                logger.info(f"SWM: GLM Persistence Worker - sending final batch of {len(final_batch)} KEMs to GLM.")
                await self._glm_batch_store_kems_async(final_batch)
            elif final_batch: logger.error(f"SWM: GLM Persistence Worker - GLM stub not available for final batch of {len(final_batch)} KEMs. Data may be lost.")
        logger.info("SWM: GLM Persistence Worker stopped.")

    # --- Cache Interaction and Event Notification ---
    def _handle_kem_eviction_for_notification(self, evicted_kem: kem_pb2.KEM): # Renamed
        if evicted_kem: logger.info(f"SWM: KEM ID '{evicted_kem.id}' evicted from cache. Scheduling notification.");
        try: asyncio.run_coroutine_threadsafe(self.subscription_manager.notify_kem_event(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED, "SWM_CACHE"), asyncio.get_running_loop())
        except RuntimeError: logger.warning(f"SWM: No running asyncio loop for evicted KEM '{evicted_kem.id}' notification.")

    async def _put_kem_to_cache_and_notify_async(self, kem: kem_pb2.KEM) -> None: # Renamed
        if not kem or not kem.id: logger.warning("SWM: Invalid KEM provided to _put_kem_to_cache_and_notify_async."); return

        # Determine if it's an update or new publish for event type
        # This needs to be done BEFORE putting it into cache, as __contains__ itself would update LRU order
        # However, the lock in IndexedLRUCache makes this tricky for perfect atomicity.
        # A simpler approach: check presence, then put, then notify. Small window for race if another thread modifies.
        # Given IndexedLRUCache uses threading.Lock, to_thread calls are serialized for cache access.
        was_present_in_cache = await asyncio.to_thread(self.swm_cache.__contains__, kem.id)

        await asyncio.to_thread(self.swm_cache.__setitem__, kem.id, kem) # This might trigger eviction callback

        logger.info(f"SWM: KEM ID '{kem.id}' put/updated in SWM cache. Current cache size: {len(self.swm_cache)}/{self.swm_cache.maxsize}")

        event_type = swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED if was_present_in_cache else swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED
        await self.subscription_manager.notify_kem_event(kem, event_type)

    # --- gRPC Service Methods ---
    async def PublishKEMToSWM(self, request: swm_service_pb2.PublishKEMToSWMRequest, context) -> swm_service_pb2.PublishKEMToSWMResponse:
        kem_to_publish = request.kem_to_publish
        kem_id_final = kem_to_publish.id or str(uuid.uuid4())
        kem_to_publish.id = kem_id_final

        if not request.kem_to_publish.id: logger.info(f"SWM: No ID provided for KEM to publish, new ID generated: '{kem_id_final}'")

        ts = Timestamp(); ts.GetCurrentTime()

        # Check if KEM exists in cache to preserve created_at, before calling _put_kem_to_cache_and_notify_async
        # This read operation should be under the same lock domain if strict consistency is needed before write.
        # IndexedLRUCache.get() uses read lock.
        existing_kem_in_cache = await asyncio.to_thread(self.swm_cache.get, kem_id_final)

        if existing_kem_in_cache:
            kem_to_publish.created_at.CopyFrom(existing_kem_in_cache.created_at)
        elif not kem_to_publish.HasField("created_at") or kem_to_publish.created_at.seconds == 0:
            kem_to_publish.created_at.CopyFrom(ts)
        kem_to_publish.updated_at.CopyFrom(ts)

        await self._put_kem_to_cache_and_notify_async(kem_to_publish)

        published_to_swm_flag=True
        persistence_status_message = "Persistence to GLM not requested."
        queued_for_glm_persistence = False

        if request.persist_to_glm_if_new_or_updated:
            if not self.aio_glm_stub:
                persistence_status_message = f"GLM client not available. KEM ID '{kem_id_final}' not queued for persistence."
                logger.error(persistence_status_message)
            else:
                try:
                    if self.glm_persistence_queue.full():
                        persistence_status_message = f"GLM persistence queue is full. KEM ID '{kem_id_final}' not queued."
                        logger.error(persistence_status_message)
                    else:
                        await self.glm_persistence_queue.put(kem_to_publish)
                        queued_for_glm_persistence = True
                        persistence_status_message = f"KEM ID '{kem_id_final}' queued for persistence to GLM."
                        logger.info(persistence_status_message)
                except Exception as e_queue:
                    persistence_status_message = f"Error queueing KEM ID '{kem_id_final}' for GLM persistence: {e_queue}"
                    logger.error(persistence_status_message, exc_info=True)

        status_msg=f"KEM ID '{kem_id_final}' published to SWM. {persistence_status_message}"
        return swm_service_pb2.PublishKEMToSWMResponse(
            kem_id_swm=kem_id_final,
            published_to_swm=published_to_swm_flag,
            queued_for_glm_persistence=queued_for_glm_persistence,
            status_message=status_msg
        )

    async def SubscribeToSWMEvents(self, request: swm_service_pb2.SubscribeToSWMEventsRequest, context: grpc_aio.ServicerContext) -> AsyncGenerator[swm_service_pb2.SWMMemoryEvent, None]:
        agent_id_str = request.agent_id or str(uuid.uuid4()) # Ensure agent_id is never empty for map keys

        event_q = await self.subscription_manager.add_subscriber(
            subscriber_id=agent_id_str,
            topics=request.topics,
            requested_q_size=request.requested_queue_size
        )
        logger.info(f"SWM: Subscriber '{agent_id_str}' connected for event stream.")

        idle_timeouts = 0
        try:
            while context.is_active(): # type: ignore
                try:
                    event = await asyncio.wait_for(event_q.get(), timeout=self.config.SUBSCRIBER_IDLE_CHECK_INTERVAL_S)
                    yield event
                    event_q.task_done()
                    idle_timeouts = 0
                except asyncio.TimeoutError:
                    if not context.is_active(): logger.info(f"SWM stream for '{agent_id_str}': gRPC context inactive during idle check."); break # type: ignore
                    idle_timeouts += 1
                    logger.debug(f"SWM stream for '{agent_id_str}': idle timeout #{idle_timeouts} (threshold: {self.config.SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD}).")
                    if idle_timeouts >= self.config.SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD:
                        logger.warning(f"SWM stream for '{agent_id_str}': disconnecting due to inactivity (idle threshold reached).")
                        break
                except Exception as e_stream: # Catch any other error from queue or yield
                    logger.error(f"SWM stream error for subscriber '{agent_id_str}': {e_stream}", exc_info=True)
                    break
        except asyncio.CancelledError:
            logger.info(f"SWM stream for subscriber '{agent_id_str}' cancelled by client or server shutdown.")
        finally:
            logger.info(f"SWM: Cleaning up subscriber '{agent_id_str}'.")
            await self.subscription_manager.remove_subscriber(agent_id_str)
            logger.info(f"SWM: Subscriber '{agent_id_str}' cleanup complete.")

    async def QuerySWM(self, request: swm_service_pb2.QuerySWMRequest, context) -> swm_service_pb2.QuerySWMResponse:
        query = request.query
        logger.info(f"SWM: QuerySWM called with KEMQuery: {query}")
        if query.embedding_query or query.text_query:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Vector/text search not supported directly in SWM cache.") # type: ignore
            return swm_service_pb2.QuerySWMResponse()

        # This logic can remain largely the same but uses self.swm_cache which is now RWLock protected
        def _sync_filter_logic():
            page_s = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
            off = 0
            if request.page_token:
                try: off = int(request.page_token)
                except ValueError: logger.warning(f"Invalid page_token for QuerySWM: '{request.page_token}', using 0.")

            # Initial candidate KEMs from cache (potentially filtered by indexed metadata)
            candidate_kems: List[kem_pb2.KEM] = []

            # Apply indexed metadata filters first
            indexed_meta_filters = {k: v for k, v in query.metadata_filters.items() if k in self.swm_cache._indexed_keys}
            non_indexed_meta_filters = {k: v for k, v in query.metadata_filters.items() if k not in self.swm_cache._indexed_keys}

            if indexed_meta_filters:
                intersected_ids: Optional[Set[str]] = None
                for k_idx, v_idx in indexed_meta_filters.items():
                    ids_from_index = self.swm_cache.get_ids_by_metadata_filter(k_idx, v_idx)
                    if intersected_ids is None: intersected_ids = ids_from_index
                    else: intersected_ids.intersection_update(ids_from_index)
                    if not intersected_ids: break # No common KEMs

                if intersected_ids is not None: # Can be empty set if no common KEMs
                    for kem_id_from_idx in intersected_ids:
                        kem_obj = self.swm_cache.get(kem_id_from_idx)
                        if kem_obj: candidate_kems.append(kem_obj)
                # If intersected_ids is empty or None after this, candidate_kems will be empty.
            else: # No indexed filters, start with all cache values
                candidate_kems = self.swm_cache.values()

            # Further filter by IDs if provided
            if query.ids:
                ids_set = set(query.ids)
                candidate_kems = [k for k in candidate_kems if k.id in ids_set]

            # Filter by non-indexed metadata
            if non_indexed_meta_filters:
                candidate_kems = [
                    k for k in candidate_kems
                    if all(k.metadata.get(mk) == mv for mk, mv in non_indexed_meta_filters.items())
                ]

            # Filter by dates
            def check_ts_range(kem_ts_proto, start_ts_proto, end_ts_proto) -> bool:
                # Make sure kem_ts_proto is valid before ToNanoseconds()
                if not kem_ts_proto or (kem_ts_proto.seconds == 0 and kem_ts_proto.nanos == 0): return True # No date on KEM, treat as match? Or False? Depends on desired logic. For now, True.

                if start_ts_proto and (start_ts_proto.seconds > 0 or start_ts_proto.nanos > 0):
                    if kem_ts_proto.ToNanoseconds() < start_ts_proto.ToNanoseconds(): return False
                if end_ts_proto and (end_ts_proto.seconds > 0 or end_ts_proto.nanos > 0):
                    if kem_ts_proto.ToNanoseconds() > end_ts_proto.ToNanoseconds(): return False
                return True

            if query.HasField("created_at_start") or query.HasField("created_at_end"):
                candidate_kems = [k for k in candidate_kems if check_ts_range(k.created_at, query.created_at_start, query.created_at_end)]
            if query.HasField("updated_at_start") or query.HasField("updated_at_end"):
                candidate_kems = [k for k in candidate_kems if check_ts_range(k.updated_at, query.updated_at_start, query.updated_at_end)]

            # Sort (default by updated_at desc, not implemented here, assumes client sorts or doesn't care for SWM)
            # For simplicity, SWM cache query does not implement server-side sorting beyond what LRU provides implicitly.
            # If sorting is needed, it should be added. For now, order is somewhat arbitrary after filtering.

            paginated_kems = candidate_kems[off : off + page_s]
            next_tok_str = str(off + page_s) if len(candidate_kems) > off + page_s else ""
            return paginated_kems, next_tok_str

        kems_page_sync, next_token_sync = await asyncio.to_thread(_sync_filter_logic)
        logger.info(f"QuerySWM: Returning {len(kems_page_sync)} KEMs from cache query.");
        return swm_service_pb2.QuerySWMResponse(kems=kems_page_sync, next_page_token=next_token_sync)

    async def LoadKEMsFromGLM(self, request: swm_service_pb2.LoadKEMsFromGLMRequest, context) -> swm_service_pb2.LoadKEMsFromGLMResponse:
        logger.info(f"SWM: LoadKEMsFromGLM request with GLM query: {request.query_for_glm}")
        if not self.aio_glm_stub:
            msg = "GLM client not available in SWM for LoadKEMsFromGLM."
            logger.error(msg); await context.abort(grpc.StatusCode.INTERNAL, msg); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore

        glm_req = glm_service_pb2.RetrieveKEMsRequest(query=request.query_for_glm)
        loaded_count = 0; queried_in_glm_count = 0; loaded_ids = []
        status_message = "GLM query initiated."

        try:
            logger.info(f"SWM: Requesting GLM.RetrieveKEMs with: {glm_req}")
            glm_response = await self._glm_retrieve_kems_async(glm_req, timeout=30) # Increased timeout for potentially large GLM retrievals

            if glm_response and glm_response.kems:
                queried_in_glm_count = len(glm_response.kems)
                logger.info(f"SWM: Received {queried_in_glm_count} KEMs from GLM.")
                for kem_from_glm in glm_response.kems:
                    await self._put_kem_to_cache_and_notify_async(kem_from_glm) # This will update cache and notify SWM subs
                    loaded_ids.append(kem_from_glm.id)
                loaded_count = len(loaded_ids)
                status_message = f"Successfully loaded {loaded_count} KEMs from GLM into SWM cache."
                if queried_in_glm_count != loaded_count : # Should ideally not happen if all KEMs from GLM are valid
                    status_message += f" ({queried_in_glm_count} KEMs were retrieved from GLM)."
            else:
                status_message = "No KEMs returned from GLM for the given query."
                logger.info(status_message)

            return swm_service_pb2.LoadKEMsFromGLMResponse(
                kems_queried_in_glm_count=queried_in_glm_count,
                kems_loaded_to_swm_count=loaded_count,
                loaded_kem_ids=loaded_ids,
                status_message=status_message
            )
        except grpc_aio.AioRpcError as e:
            status_message = f"gRPC error during LoadKEMsFromGLM from GLM: {e.code()} - {e.details()}"
            logger.error(status_message, exc_info=True)
            await context.abort(e.code(), status_message); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore
        except Exception as e_load:
            status_message = f"Unexpected error during LoadKEMsFromGLM: {e_load}"
            logger.error(status_message, exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, status_message); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore

    # --- Distributed Primitives ---
    async def AcquireLock(self, request: swm_service_pb2.AcquireLockRequest, context) -> swm_service_pb2.AcquireLockResponse:
        return await self.lock_manager.acquire_lock(request.resource_id, request.agent_id, request.timeout_ms, request.lease_duration_ms)

    async def ReleaseLock(self, request: swm_service_pb2.ReleaseLockRequest, context) -> swm_service_pb2.ReleaseLockResponse:
        return await self.lock_manager.release_lock(request.resource_id, request.agent_id, request.lock_id)

    async def GetLockInfo(self, request: swm_service_pb2.GetLockInfoRequest, context) -> swm_service_pb2.LockInfo:
        return await self.lock_manager.get_lock_info(request.resource_id)

    async def IncrementCounter(self, request: swm_service_pb2.IncrementCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        if not request.counter_id: # Basic validation in servicer
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "counter_id cannot be empty.") # type: ignore
            return swm_service_pb2.CounterValueResponse()
        return await self.counter_manager.increment_counter(request.counter_id, request.increment_by)

    async def GetCounter(self, request: swm_service_pb2.DistributedCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        if not request.counter_id: # Basic validation in servicer
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "counter_id cannot be empty.") # type: ignore
            return swm_service_pb2.CounterValueResponse()
        return await self.counter_manager.get_counter(request.counter_id)

async def serve():
    module_cfg = SWMConfig()
    logging.basicConfig( # Setup root logger once here, or ensure it's done if app has multiple entry points
        level=module_cfg.get_log_level_int(),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True # Force reconfig if already configured by another import
    )

    server=grpc_aio.server()
    servicer_instance=SharedWorkingMemoryServiceImpl()
    swm_service_pb2_grpc.add_SharedWorkingMemoryServiceServicer_to_server(servicer_instance,server)

    # Start background tasks associated with the servicer instance
    # Ensure these tasks are managed correctly for startup and shutdown.
    # Creating a task that runs start_background_tasks which itself creates other tasks.
    # Need to ensure servicer_instance is fully initialized before calling this.
    # loop = asyncio.get_event_loop()
    # loop.create_task(servicer_instance.start_background_tasks())
    # Better: Call it before server.start() if it doesn't block indefinitely

    listen_addr = servicer_instance.config.GRPC_LISTEN_ADDRESS
    server.add_insecure_port(listen_addr)

    logger.info(f"Starting SWM async server on {listen_addr}...")
    await server.start()

    # Start background tasks after server has started, or ensure they don't block server.start()
    # If start_background_tasks is quick and just schedules tasks, it's fine before server.start()
    # But if it involves waiting or long setup, it should be managed carefully.
    # The current implementation of start_background_tasks is async and should be awaited.
    await servicer_instance.start_background_tasks()

    logger.info(f"SWM server started and listening on {listen_addr}.")
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt: logger.info("SWM server stopping via KeyboardInterrupt...")
    except asyncio.CancelledError: logger.info("SWM server task cancelled (e.g. by Docker stop).")
    finally:
        logger.info("SWM server: Initiating graceful shutdown of servicer components...")
        await servicer_instance.stop_background_tasks()
        logger.info("SWM server: Stopping gRPC server...")
        await server.stop(grace=5) # Allow 5 seconds for ongoing RPCs to complete
        logger.info("SWM server stopped.")

if __name__=='__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt: logger.info("SWM main process interrupted by user.")
    except Exception as e_main: logger.critical(f"SWM main unhandled exception: {e_main}",exc_info=True)
```
