import grpc
import grpc.aio as grpc_aio
import asyncio
import time
import sys
import os
import uuid
import logging
import threading
import asyncio # asyncio was imported twice, removing one
import random # For jitter in retry logic
# import queue as sync_queue # Unused

from dataclasses import dataclass, field
from cachetools import LRUCache, Cache
from google.protobuf.timestamp_pb2 import Timestamp
from readerwriterlock import rwlock # Предполагаем наличие этой библиотеки
import typing
from typing import Optional, List, Set, Dict, Callable, AsyncGenerator, Tuple

# Import the SWMConfig class
from .config import SWMConfig

# Create a module-level logger instance.
# The level will be set properly in SharedWorkingMemoryServiceImpl.__init__
logger = logging.getLogger(__name__)
logging.basicConfig( # Basic config for root logger, can be overridden by specific loggers
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.INFO # Default level, will be adjusted by SWMConfig in the servicer
)


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
        self._rw_lock = rwlock.RWLockFair() # Using RWLockFair for fairness between readers and writers
        self._on_evict_callback = on_evict_callback

    def _add_to_metadata_indexes(self, kem: kem_pb2.KEM):
        # This method is internal and should be called under a write lock
        if not kem or not kem.id: return
        for meta_key in self._indexed_keys:
            if meta_key in kem.metadata:
                value = kem.metadata[meta_key]
                self._metadata_indexes.setdefault(meta_key, {}).setdefault(value, set()).add(kem.id)

    def _remove_from_metadata_indexes(self, kem: kem_pb2.KEM):
        # This method is internal and should be called under a write lock
        if not kem or not kem.id: return
        for meta_key in self._indexed_keys:
            if meta_key in kem.metadata:
                original_value = kem.metadata[meta_key]
                if meta_key in self._metadata_indexes and original_value in self._metadata_indexes[meta_key]:
                    self._metadata_indexes[meta_key][original_value].discard(kem.id)
                    if not self._metadata_indexes[meta_key][original_value]: del self._metadata_indexes[meta_key][original_value]
                    if not self._metadata_indexes[meta_key]: del self._metadata_indexes[meta_key]

    def __setitem__(self, kem_id: str, kem: kem_pb2.KEM):
        with self._rw_lock.gen_wlock(): # Acquire write lock
            evicted_kem = None
            if kem_id in self._lru:
                old_kem = self._lru[kem_id] # Get old KEM before it's replaced in LRU
                self._remove_from_metadata_indexes(old_kem)
            elif len(self._lru) >= self._lru.maxsize:
                _evicted_id, evicted_kem = self._lru.popitem() # LRU eviction
                if evicted_kem:
                    self._remove_from_metadata_indexes(evicted_kem)
                    if self._on_evict_callback:
                        try:
                            # Callback might do I/O or take time, consider if it should be outside the lock
                            # or made async and scheduled if this becomes a bottleneck.
                            # For now, keeping it simple as it was with threading.Lock.
                            self._on_evict_callback(evicted_kem)
                        except Exception as e_cb:
                            logger.error(f"Error in on_evict_callback for KEM ID '{_evicted_id}': {e_cb}", exc_info=True)

            self._lru[kem_id] = kem # Actual update in LRU cache
            self._add_to_metadata_indexes(kem)

    def __getitem__(self, kem_id: str) -> kem_pb2.KEM:
        with self._rw_lock.gen_rlock(): # Acquire read lock
            return self._lru[kem_id]

    def __delitem__(self, kem_id: str):
        with self._rw_lock.gen_wlock(): # Acquire write lock
            if kem_id in self._lru:
                kem_to_remove = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem_to_remove)
            else:
                raise KeyError(kem_id)

    def get(self, kem_id: str, default=None) -> typing.Optional[kem_pb2.KEM]:
        with self._rw_lock.gen_rlock(): # Acquire read lock
            return self._lru.get(kem_id, default)

    def pop(self, kem_id: str, default=object()) -> kem_pb2.KEM:
        with self._rw_lock.gen_wlock(): # Acquire write lock
            if kem_id in self._lru:
                kem = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem)
                return kem
            elif default is not object():
                return default # type: ignore
            else:
                raise KeyError(kem_id)

    def __len__(self) -> int:
        with self._rw_lock.gen_rlock(): # Acquire read lock
            return len(self._lru)

    def __contains__(self, kem_id: str) -> bool:
        with self._rw_lock.gen_rlock(): # Acquire read lock
            return kem_id in self._lru

    def values(self) -> List[kem_pb2.KEM]:
        with self._rw_lock.gen_rlock(): # Acquire read lock
            return list(self._lru.values())

    def items(self) -> List[Tuple[str, kem_pb2.KEM]]:
        with self._rw_lock.gen_rlock(): # Acquire read lock
            return list(self._lru.items())

    def clear(self):
        with self._rw_lock.gen_wlock(): # Acquire write lock
            self._lru.clear()
            self._metadata_indexes.clear()
            self._metadata_indexes = {key: {} for key in self._indexed_keys}

    @property
    def maxsize(self):
        # maxsize is immutable for cachetools.LRUCache, so no lock needed for reading this property
        return self._lru.maxsize

    def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Set[str]:
        if meta_key not in self._indexed_keys:
            return set()
        with self._rw_lock.gen_rlock(): # Acquire read lock
            return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()

@dataclass
class LockInfoInternal:
    resource_id: str; agent_id: str; lock_id: str
    acquired_at_unix_ms: int; lease_duration_ms: int; lease_expires_at_unix_ms: int

class SharedWorkingMemoryServiceImpl(swm_service_pb2_grpc.SharedWorkingMemoryServiceServicer):
    def __init__(self):
        self.config = SWMConfig() # Servicer instance gets its own config
        logger.setLevel(self.config.get_log_level_int()) # Set logger level from this instance's config

        logger.info(f"Initializing SharedWorkingMemoryServiceImpl... Cache size: {self.config.CACHE_MAX_SIZE}, Indexed keys: {self.config.INDEXED_METADATA_KEYS}")

        self.aio_glm_channel: Optional[grpc_aio.Channel] = None
        self.aio_glm_stub: Optional[glm_service_pb2_grpc.GlobalLongTermMemoryStub] = None

        # Retry parameters will be taken from self.config directly in the methods
        self.retryable_error_codes = (
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.RESOURCE_EXHAUSTED
        )

        self.subscribers_lock = asyncio.Lock()
        self.swm_cache = IndexedLRUCache(
            maxsize=self.config.CACHE_MAX_SIZE,
            indexed_keys=self.config.INDEXED_METADATA_KEYS,
            on_evict_callback=self._handle_kem_eviction_sync
        )
        self.locks: Dict[str, LockInfoInternal] = {}; self.lock_condition = asyncio.Condition()
        self.counters: Dict[str, int] = {}; self.counters_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._lock_cleanup_task: Optional[asyncio.Task] = None
        self._glm_persistence_worker_task: Optional[asyncio.Task] = None
        self._lock_cleanup_interval_seconds = self.config.LOCK_CLEANUP_INTERVAL_S

        self.glm_persistence_queue: asyncio.Queue[kem_pb2.KEM] = asyncio.Queue(
            maxsize=self.config.GLM_PERSISTENCE_QUEUE_MAX_SIZE
        )

        self.subscribers: Dict[str, SubscriberInfo] = {}
        self.kem_id_to_subscribers: Dict[str, Set[str]] = {}
        self.metadata_exact_match_to_subscribers: Dict[str, Dict[str, Set[str]]] = {}
        self.general_kem_event_subscribers: Set[str] = set()

        try:
            # Initialize asynchronous GLM client
            self.aio_glm_channel = grpc_aio.insecure_channel(self.config.GLM_SERVICE_ADDRESS)
            self.aio_glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.aio_glm_channel)
            logger.info(f"GLM client (async) for SWM initialized, target: {self.config.GLM_SERVICE_ADDRESS}")
        except Exception as e:
            logger.error(f"Error initializing asynchronous GLM client in SWM: {e}", exc_info=True)
            self.aio_glm_stub = None
            if self.aio_glm_channel:
                try:
                    # This is problematic in a non-async __init__.
                    # The channel should ideally be closed in an async cleanup method.
                    # For now, just log and it will be handled in stop_background_tasks.
                    logger.warning("SWM: GLM async channel was created but stub initialization failed. Channel will be closed on shutdown.")
                except Exception as close_e:
                    logger.error(f"SWM: Exception while trying to handle GLM channel close during __init__ failure: {close_e}", exc_info=True)

    async def start_background_tasks(self):
        if self._lock_cleanup_task is None or self._lock_cleanup_task.done():
            self._stop_event.clear() # Clear stop event for all tasks
            self._lock_cleanup_task = asyncio.create_task(self._expired_lock_cleanup_task_async())
            logger.info("SWM: Expired lock cleanup task started.")

        if self._glm_persistence_worker_task is None or self._glm_persistence_worker_task.done():
            # self._stop_event should already be cleared or clear it again if tasks are managed independently for stop
            self._glm_persistence_worker_task = asyncio.create_task(self._glm_persistence_worker())
            logger.info("SWM: GLM persistence worker task started.")

    async def stop_background_tasks(self):
        logger.info("SWM: Stopping background tasks...")
        self._stop_event.set() # Signal all tasks to stop

        if self._lock_cleanup_task and not self._lock_cleanup_task.done():
            logger.info("SWM: Waiting for expired lock cleanup task to stop...")
            try: await asyncio.wait_for(self._lock_cleanup_task, timeout=self._lock_cleanup_interval_seconds + 2) # type: ignore
            except asyncio.TimeoutError: logger.warning("SWM: Expired lock cleanup task did not finish in time during shutdown.")
            except asyncio.CancelledError: logger.info("SWM: Expired lock cleanup task cancelled during shutdown.")
        self._lock_cleanup_task = None

        if self._glm_persistence_worker_task and not self._glm_persistence_worker_task.done():
            logger.info("SWM: Waiting for GLM persistence worker task to stop...")
            try:
                # Attempt to process remaining items in the queue before fully stopping
                # This is a simple way; a more robust way might involve a special sentinel or flushing.
                await asyncio.wait_for(self._glm_persistence_worker_task, timeout=self.config.GLM_PERSISTENCE_FLUSH_INTERVAL_S + 5)
            except asyncio.TimeoutError:
                logger.warning("SWM: GLM persistence worker task did not finish processing queue in time during shutdown.")
                self._glm_persistence_worker_task.cancel() # Force cancel if timeout
                try:
                    await self._glm_persistence_worker_task
                except asyncio.CancelledError:
                    logger.info("SWM: GLM persistence worker task was cancelled during shutdown after timeout.")
            except asyncio.CancelledError: # If it was cancelled by stop_event already
                 logger.info("SWM: GLM persistence worker task was cancelled by stop_event.")
        self._glm_persistence_worker_task = None

        if self.aio_glm_channel:
            logger.info("SWM: Closing GLM client (async) channel.")
            try:
                await self.aio_glm_channel.close()
            except Exception as e:
                logger.error(f"SWM: Error closing GLM async channel: {e}", exc_info=True)
            self.aio_glm_channel = None
            self.aio_glm_stub = None

    async def _expired_lock_cleanup_task_async(self):
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
            except Exception as e: logger.error(f"SWM: Error in lock cleanup task: {e}", exc_info=True); await asyncio.sleep(self._lock_cleanup_interval_seconds)
        logger.info("SWM: Async expired lock cleanup task stopped.")

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
                        logger.error(f"GLM call RetrieveKEMs failed after {self.config.GLM_RETRY_MAX_ATTEMPTS} attempts. Last error: {e.code()} - {e.details()}", exc_info=False)
                        raise

                    jitter_fraction = 0.1
                    jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                    actual_delay_s = max(0, current_delay_s + jitter_value)

                    logger.warning(f"GLM call RetrieveKEMs failed (attempt {attempt}/{self.config.GLM_RETRY_MAX_ATTEMPTS}) with status {e.code()}. Retrying in {actual_delay_s:.2f}s. Details: {e.details()}", exc_info=False)
                    await asyncio.sleep(actual_delay_s)
                    current_delay_s *= self.config.GLM_RETRY_BACKOFF_FACTOR
                else:
                    logger.error(f"GLM call RetrieveKEMs failed with non-retryable status {e.code()}. Details: {e.details()}", exc_info=True)
                    raise
            except Exception as e_generic:
                logger.error(f"A non-gRPC error occurred during GLM call RetrieveKEMs (attempt {attempt}/{self.config.GLM_RETRY_MAX_ATTEMPTS}): {e_generic}", exc_info=True)
                if attempt == self.config.GLM_RETRY_MAX_ATTEMPTS: raise
                jitter_fraction = 0.1
                jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                actual_delay_s = max(0, current_delay_s + jitter_value)
                logger.info(f"Retrying GLM RetrieveKEMs after non-gRPC error in {actual_delay_s:.2f}s.")
                await asyncio.sleep(actual_delay_s)
                current_delay_s *= self.config.GLM_RETRY_BACKOFF_FACTOR
        logger.error("GLM RetrieveKEMs exhausted attempts unexpectedly without raising an error.")
        raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "GLM RetrieveKEMs exhausted attempts unexpectedly.")


    async def _glm_store_kem_async(self, request: glm_service_pb2.StoreKEMRequest, timeout: int = 10) -> glm_service_pb2.StoreKEMResponse:
        if not self.aio_glm_stub:
            logger.error("SWM: GLM async client not available for StoreKEM.")
            raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "GLM client not available")

        current_delay_s = self.config.GLM_RETRY_INITIAL_DELAY_S
        for attempt in range(1, self.config.GLM_RETRY_MAX_ATTEMPTS + 1):
            try:
                logger.debug(f"Attempt {attempt} to call GLM StoreKEM (async).")
                return await self.aio_glm_stub.StoreKEM(request, timeout=timeout)
            except grpc_aio.AioRpcError as e:
                if e.code() in self.retryable_error_codes:
                    if attempt == self.config.GLM_RETRY_MAX_ATTEMPTS:
                        logger.error(f"GLM call StoreKEM failed after {self.config.GLM_RETRY_MAX_ATTEMPTS} attempts. Last error: {e.code()} - {e.details()}", exc_info=False)
                        raise

                    jitter_fraction = 0.1
                    jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                    actual_delay_s = max(0, current_delay_s + jitter_value)

                    logger.warning(f"GLM call StoreKEM failed (attempt {attempt}/{self.config.GLM_RETRY_MAX_ATTEMPTS}) with status {e.code()}. Retrying in {actual_delay_s:.2f}s. Details: {e.details()}", exc_info=False)
                    await asyncio.sleep(actual_delay_s)
                    current_delay_s *= self.config.GLM_RETRY_BACKOFF_FACTOR
                else:
                    logger.error(f"GLM call StoreKEM failed with non-retryable status {e.code()}. Details: {e.details()}", exc_info=True)
                    raise
            except Exception as e_generic:
                logger.error(f"A non-gRPC error occurred during GLM call StoreKEM (attempt {attempt}/{self.config.GLM_RETRY_MAX_ATTEMPTS}): {e_generic}", exc_info=True)
                if attempt == self.config.GLM_RETRY_MAX_ATTEMPTS: raise
                jitter_fraction = 0.1
                jitter_value = random.uniform(-jitter_fraction, jitter_fraction) * current_delay_s
                actual_delay_s = max(0, current_delay_s + jitter_value)
                logger.info(f"Retrying GLM StoreKEM after non-gRPC error in {actual_delay_s:.2f}s.")
                await asyncio.sleep(actual_delay_s)
                current_delay_s *= self.config.GLM_RETRY_BACKOFF_FACTOR
        logger.error("GLM StoreKEM exhausted attempts unexpectedly without raising an error.")
        raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "GLM StoreKEM exhausted attempts unexpectedly.")

    async def _glm_batch_store_kems_async(self, kems_batch: List[kem_pb2.KEM], timeout: int = 30) -> Optional[glm_service_pb2.BatchStoreKEMsResponse]:
        if not self.aio_glm_stub:
            logger.error("SWM: GLM async client not available for BatchStoreKEMs.")
            # Do not raise here, allow worker to handle this potentially by requeueing or logging failure
            return None
        if not kems_batch:
            return None

        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_batch)

        # Using a simplified retry logic for batch, as partial success/failure is complex with retries
        # For a more robust solution, individual KEM failures from BatchStoreKEMsResponse
        # might need to be re-queued or handled specifically.
        try:
            logger.info(f"SWM: Calling GLM BatchStoreKEMs with {len(kems_batch)} KEMs.")
            response = await self.aio_glm_stub.BatchStoreKEMs(request, timeout=timeout)
            if response.failed_kem_references:
                logger.warning(f"SWM: GLM BatchStoreKEMs reported {len(response.failed_kem_references)} failures. Refs: {response.failed_kem_references}")
            logger.info(f"SWM: GLM BatchStoreKEMs processed. Success: {len(response.successfully_stored_kems)}, Failures: {len(response.failed_kem_references)}")
            return response
        except grpc_aio.AioRpcError as e:
            logger.error(f"SWM: gRPC error during GLM BatchStoreKEMs: {e.code()} - {e.details()}", exc_info=True)
            # TODO: Consider re-queueing the entire batch or individual KEMs upon certain retryable errors.
            # For now, the batch is considered failed on gRPC error.
        except Exception as e_generic:
            logger.error(f"SWM: Non-gRPC error during GLM BatchStoreKEMs: {e_generic}", exc_info=True)
        return None # Indicate failure or no response

    async def _glm_persistence_worker(self):
        logger.info("SWM: GLM Persistence Worker started.")
        while not self._stop_event.is_set():
            batch_to_persist: List[kem_pb2.KEM] = []
            try:
                # Wait for the first item or timeout
                first_kem = await asyncio.wait_for(
                    self.glm_persistence_queue.get(),
                    timeout=self.config.GLM_PERSISTENCE_FLUSH_INTERVAL_S
                )
                batch_to_persist.append(first_kem)
                self.glm_persistence_queue.task_done()

                # Try to fill the rest of the batch without waiting too long
                while len(batch_to_persist) < self.config.GLM_PERSISTENCE_BATCH_SIZE:
                    try:
                        kem = self.glm_persistence_queue.get_nowait()
                        batch_to_persist.append(kem)
                        self.glm_persistence_queue.task_done()
                    except asyncio.QueueEmpty:
                        break # Queue is empty, flush what we have

            except asyncio.TimeoutError:
                # Flush interval reached, but queue might have been empty.
                # No items in batch_to_persist yet if timeout occurred on first_kem.
                pass # Continue to see if batch_to_persist has items from previous non-blocking gets (unlikely here)
            except asyncio.CancelledError:
                logger.info("SWM: GLM Persistence Worker cancelled during queue get.")
                break # Exit if worker is cancelled
            except Exception as e_q_get:
                logger.error(f"SWM: GLM Persistence Worker error getting from queue: {e_q_get}", exc_info=True)
                await asyncio.sleep(1) # Brief pause before retrying queue operations
                continue


            if batch_to_persist:
                logger.info(f"SWM: GLM Persistence Worker processing batch of {len(batch_to_persist)} KEMs.")
                # In a real scenario, you might want to handle the response,
                # especially failed_kem_references, potentially re-queueing them.
                # For this initial implementation, we'll just log.
                # Also, consider what to do if aio_glm_stub is None (GLM connection failed at startup)
                if self.aio_glm_stub:
                    glm_response = await self._glm_batch_store_kems_async(batch_to_persist)
                    if glm_response:
                        # TODO: Handle successfully_stored_kems if SWM cache needs update from GLM's final version (e.g. server-gen IDs)
                        # This would require matching response KEMs to originals, which is complex.
                        # For now, assume SWM's version is what we intended to persist.
                        pass
                    else: # _glm_batch_store_kems_async itself logged the error
                        logger.error(f"SWM: GLM Persistence Worker: Batch of {len(batch_to_persist)} KEMs failed to persist or no response from GLM.")
                        # Basic re-queue of the whole batch for simplicity. A more robust system
                        # might have a dead-letter queue or individual KEM retry logic.
                        # This could lead to infinite retries if GLM is permanently down.
                        logger.warning(f"SWM: GLM Persistence Worker: Re-queueing {len(batch_to_persist)} KEMs due to GLM processing failure.")
                        for kem_to_requeue in reversed(batch_to_persist): # Add to front if possible, or just re-add
                             if not self.glm_persistence_queue.full():
                                 await self.glm_persistence_queue.put(kem_to_requeue)
                             else:
                                 logger.error(f"SWM: GLM Persistence Worker: Failed to re-queue KEM ID '{kem_to_requeue.id}', persistence queue full.")
                                 break # Stop trying to re-queue this batch
                else:
                    logger.error("SWM: GLM Persistence Worker: GLM stub not available. Batch not persisted.")
                    # Re-queue logic similar to above could be applied here too.

            if self._stop_event.is_set() and self.glm_persistence_queue.empty():
                 logger.info("SWM: GLM Persistence Worker stopping as stop event is set and queue is empty.")
                 break

        # Final attempt to clear the queue on shutdown if not empty
        if not self.glm_persistence_queue.empty():
            logger.info(f"SWM: GLM Persistence Worker - processing remaining {self.glm_persistence_queue.qsize()} items on shutdown...")
            final_batch: List[kem_pb2.KEM] = []
            while not self.glm_persistence_queue.empty():
                try:
                    final_batch.append(self.glm_persistence_queue.get_nowait())
                    self.glm_persistence_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            if final_batch and self.aio_glm_stub:
                logger.info(f"SWM: GLM Persistence Worker - sending final batch of {len(final_batch)} KEMs to GLM.")
                await self._glm_batch_store_kems_async(final_batch)
            elif final_batch:
                 logger.error(f"SWM: GLM Persistence Worker - GLM stub not available for final batch of {len(final_batch)} KEMs. Data may be lost.")


        logger.info("SWM: GLM Persistence Worker stopped.")


    def _handle_kem_eviction_sync(self, evicted_kem: kem_pb2.KEM):
        if evicted_kem: logger.info(f"SWM: KEM ID '{evicted_kem.id}' evicted. Scheduling notification.");
        try: asyncio.run_coroutine_threadsafe(self._notify_subscribers(evicted_kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED, "SWM_CACHE"), asyncio.get_running_loop())
        except RuntimeError: logger.warning(f"SWM: No running loop for evicted KEM '{evicted_kem.id}' notification.")

    async def _put_kem_to_cache_async(self, kem: kem_pb2.KEM) -> None:
        if not kem or not kem.id: logger.warning("Invalid KEM to cache."); return
        was_present = await asyncio.to_thread(self.swm_cache.__contains__, kem.id)
        await asyncio.to_thread(self.swm_cache.__setitem__, kem.id, kem)
        logger.info(f"KEM ID '{kem.id}' put/updated in SWM cache. Size: {len(self.swm_cache)}/{self.swm_cache.maxsize}")
        await self._notify_subscribers(kem, swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED if was_present else swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

    async def _notify_subscribers(self, kem: kem_pb2.KEM, event_type: swm_service_pb2.SWMMemoryEvent.EventType, source_agent_id: str = "SWM_SERVER"):
        if not kem or not kem.id: logger.warning("_notify_subscribers called with an invalid KEM."); return
        evt_to_dispatch = swm_service_pb2.SWMMemoryEvent(event_id=str(uuid.uuid4()),event_type=event_type,kem_payload=kem,event_time=Timestamp(seconds=int(time.time())),source_agent_id=source_agent_id,details=f"Evt {event_type} KEM {kem.id}")
        targeted_sub_ids: Set[str] = set()
        async with self.subscribers_lock:
            targeted_sub_ids.update(self.general_kem_event_subscribers)
            if kem.id in self.kem_id_to_subscribers: targeted_sub_ids.update(self.kem_id_to_subscribers[kem.id])
            if kem.metadata:
                for mk, mv in kem.metadata.items():
                    if mk in self.metadata_exact_match_to_subscribers and mv in self.metadata_exact_match_to_subscribers[mk]:
                        targeted_sub_ids.update(self.metadata_exact_match_to_subscribers[mk][mv])
            sub_infos_to_notify = [self.subscribers[sid] for sid in targeted_sub_ids if sid in self.subscribers]
        if not sub_infos_to_notify: logger.debug(f"No subs for KEM '{kem.id}'."); return

        # Need to find subscriber_id to log it.
        # Create a reverse mapping from queue object to subscriber_id for logging purposes,
        # or iterate self.subscribers if performance is not critical here.
        # For simplicity now, will iterate if needed, but ideally, sub_id should be available.
        # The sub_info objects are SubscriberInfo, they don't store the sub_id key directly.
        # Let's find sub_id by searching self.subscribers by sub_info object instance or queue.

        logger.debug(f"Dispatching event for KEM '{kem.id}' (event_id: {evt_to_dispatch.event_id}) to {len(sub_infos_to_notify)} subscribers.")
        for sub_info in sub_infos_to_notify:
            subscriber_id_for_log = "unknown_subscriber" # Default if not found
            # This is inefficient, consider adding sub_id to SubscriberInfo if this logging is frequent & critical path
            for sid, s_info_iter in self.subscribers.items():
                if s_info_iter is sub_info:
                    subscriber_id_for_log = sid
                    break
            try:
                if sub_info.event_queue.full():
                    logger.warning(f"Subscriber queue full for subscriber_id='{subscriber_id_for_log}', event_id='{evt_to_dispatch.event_id}' (KEM_ID='{kem.id}') lost. Queue size: {sub_info.event_queue.qsize()}/{sub_info.event_queue.maxsize}")
                else:
                    await sub_info.event_queue.put(evt_to_dispatch)
            except Exception as e:
                logger.error(f"Error queueing event for subscriber_id='{subscriber_id_for_log}', event_id='{evt_to_dispatch.event_id}': {e}", exc_info=True)

    async def PublishKEMToSWM(self, request: swm_service_pb2.PublishKEMToSWMRequest, context) -> swm_service_pb2.PublishKEMToSWMResponse:
        kem_to_publish = request.kem_to_publish; logger.info(f"SWM: PublishKEMToSWM for KEM ID (suggested): '{kem_to_publish.id}'")
        kem_id_final = kem_to_publish.id or str(uuid.uuid4()); kem_to_publish.id = kem_id_final
        # The original kem_to_publish.id (from request.kem_to_publish.id) might be empty.
        # We log the final ID used (kem_id_final). If it was generated, the original was empty.
        if not request.kem_to_publish.id and kem_id_final: # Check if ID was generated
             logger.info(f"SWM: No ID provided by client, new ID generated: '{kem_id_final}'")
        ts = Timestamp(); ts.GetCurrentTime()
        existing_kem_in_cache = await asyncio.to_thread(self.swm_cache.get, kem_id_final) # This is a kem_pb2.KEM object
        if existing_kem_in_cache: # If KEM already exists in cache, preserve its created_at
            kem_to_publish.created_at.CopyFrom(existing_kem_in_cache.created_at)
        elif not kem_to_publish.HasField("created_at"): # If new and no created_at provided, set it
            kem_to_publish.created_at.CopyFrom(ts)
        # Always update/set updated_at
        kem_to_publish.updated_at.CopyFrom(ts)
        await self._put_kem_to_cache_async(kem_to_publish) # This notifies subscribers too

        published_to_swm_flag=True
        persistence_status_message = "Persistence to GLM not requested."
        # The persistence_triggered_flag might be better named, as it's now async.
        # It now means "queued for persistence" rather than "attempted direct persistence".
        queued_for_glm_persistence = False

        if request.persist_to_glm_if_new_or_updated:
            if not self.aio_glm_stub: # Check async stub
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
        # Note: kem_id_final might change if GLM generates a new ID and SWM updates its cache from GLM response.
        # This asynchronous persistence makes it harder to return the *final* GLM ID here.
        # The current response returns the ID known to SWM at the time of publishing to SWM.
        return swm_service_pb2.PublishKEMToSWMResponse(
            kem_id_swm=kem_id_final, # Renamed field to clarify it's the ID known to SWM
            published_to_swm=published_to_swm_flag,
            queued_for_glm_persistence=queued_for_glm_persistence, # Renamed field
            status_message=status_msg
        )

    def _remove_subscriber_from_indexes(self, subscriber_id: str, original_topics: List[swm_service_pb2.SubscriptionTopic]):
        # This method must be called under self.subscribers_lock
        self.general_kem_event_subscribers.discard(subscriber_id)
        if original_topics:
            for topic in original_topics:
                if topic.type == swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS and \
                   topic.filter_criteria and '=' in topic.filter_criteria:
                    key, value = topic.filter_criteria.split("=", 1); key = key.strip(); value = value.strip()
                    if key == "kem_id":
                        if value in self.kem_id_to_subscribers:
                            self.kem_id_to_subscribers[value].discard(subscriber_id)
                            if not self.kem_id_to_subscribers[value]: del self.kem_id_to_subscribers[value]
                    elif key.startswith("metadata."):
                        meta_actual_key = key.split("metadata.",1)[1]
                        if meta_actual_key and meta_actual_key in self.metadata_exact_match_to_subscribers:
                            if value in self.metadata_exact_match_to_subscribers[meta_actual_key]:
                                self.metadata_exact_match_to_subscribers[meta_actual_key][value].discard(subscriber_id)
                                if not self.metadata_exact_match_to_subscribers[meta_actual_key][value]: del self.metadata_exact_match_to_subscribers[meta_actual_key][value]
                            if not self.metadata_exact_match_to_subscribers[meta_actual_key]: del self.metadata_exact_match_to_subscribers[meta_actual_key]
        logger.debug(f"SWM: Subscriber '{subscriber_id}' removed from specific filter indexes.")

    async def SubscribeToSWMEvents(self, request: swm_service_pb2.SubscribeToSWMEventsRequest, context: grpc_aio.ServicerContext) -> AsyncGenerator[swm_service_pb2.SWMMemoryEvent, None]:
        ag_id = request.agent_id
        sub_id = ag_id or str(uuid.uuid4())

        # Determine queue size using values from SWMConfig
        requested_q_size = request.requested_queue_size
        if requested_q_size <= 0:
            actual_q_size = self.config.SUBSCRIBER_DEFAULT_QUEUE_SIZE
        else:
            actual_q_size = max(self.config.SUBSCRIBER_MIN_QUEUE_SIZE, min(requested_q_size, self.config.SUBSCRIBER_MAX_QUEUE_SIZE))

        logger.info(f"SWM: New subscriber '{sub_id}' (agent_id: '{ag_id}'). Requested Q size: {requested_q_size}, Actual Q size: {actual_q_size}. Topics: {request.topics}")
        q = asyncio.Queue(maxsize=actual_q_size)

        p_filters:Dict[str,Set[str]]={}; sub_all_kem=False; has_kem_topic=any(t.type==swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS for t in request.topics)
        has_spec_idx_filter=False
        if not request.topics and has_kem_topic: sub_all_kem=True
        elif request.topics:
            is_gen_cand=False
            for t in request.topics:
                if t.type==swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS:
                    is_gen_cand=True
                    if not t.filter_criteria: sub_all_kem=True;break
                    if '=' in t.filter_criteria:
                        k,v=t.filter_criteria.split("=",1);k=k.strip();v=v.strip();p_filters.setdefault(k,set()).add(v)
                        if k=="kem_id" or (k.startswith("metadata.") and k.split("metadata.",1)[1]): has_spec_idx_filter=True
            if is_gen_cand and not has_spec_idx_filter and not sub_all_kem:sub_all_kem=True

        sub_info=SubscriberInfo(q,list(request.topics),p_filters,sub_all_kem)
        async with self.subscribers_lock:
            if sub_id in self.subscribers:logger.warning(f"Sub ID '{sub_id}' exists.Removing old idx.");self._remove_subscriber_from_indexes(sub_id,self.subscribers[sub_id].topics)
            self.subscribers[sub_id]=sub_info
            if sub_all_kem:self.general_kem_event_subscribers.add(sub_id)
            else:
                for kid_val in p_filters.get("kem_id",set()):self.kem_id_to_subscribers.setdefault(kid_val,set()).add(sub_id)
                for mkf,mvs in p_filters.items():
                    if mkf.startswith("metadata."):
                        amk=mkf.split("metadata.",1)[1]
                        if amk:k_idx=self.metadata_exact_match_to_subscribers.setdefault(amk,{});[k_idx.setdefault(mvf,set()).add(sub_id) for mvf in mvs]
            logger.info(f"SWM: Sub '{sub_id}' reg/upd.Total:{len(self.subscribers)}.Gen KEM:{len(self.general_kem_event_subscribers)}")
        active=True
        idle_timeouts_count = 0
        try:
            while active:
                try:
                    evt=await asyncio.wait_for(q.get(),timeout=self.config.SUBSCRIBER_IDLE_CHECK_INTERVAL_S)
                    if not context.is_active(): # type: ignore
                        logger.info(f"SWM stream for '{sub_id}': gRPC context inactive.")
                        active=False;break
                    yield evt
                    q.task_done()
                    idle_timeouts_count = 0 # Reset on successful get
                except asyncio.TimeoutError:
                    if not context.is_active(): # type: ignore
                        logger.info(f"SWM stream for '{sub_id}': gRPC context inactive during idle check.")
                        active=False;break
                    idle_timeouts_count += 1
                    logger.debug(f"SWM stream for '{sub_id}': idle timeout #{idle_timeouts_count} (threshold: {self.config.SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD}).")
                    if idle_timeouts_count >= self.config.SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD:
                        logger.warning(f"SWM stream for '{sub_id}': disconnecting due to inactivity (idle threshold reached).")
                        active=False;break
                except Exception as e:
                    logger.error(f"SWM stream error for subscriber '{sub_id}': {e}",exc_info=True)
                    active=False;break
        except asyncio.CancelledError:
            logger.info(f"SWM stream for subscriber '{sub_id}' cancelled by client or server shutdown.")
        finally:
            logger.info(f"SWM: Cleaning up subscriber '{sub_id}' (final active state: {active}).")
            async with self.subscribers_lock:
                rem_info=self.subscribers.pop(sub_id,None)
                if rem_info:
                    logger.info(f"SWM: Subscriber '{sub_id}' removed from active list. Remaining subscribers: {len(self.subscribers)}")
                    self._remove_subscriber_from_indexes(sub_id,rem_info.topics)
                else:logger.warning(f"SWM:Attempt remove non-exist sub '{sub_id}'.")
    async def QuerySWM(self, request: swm_service_pb2.QuerySWMRequest, context) -> swm_service_pb2.QuerySWMResponse:
        query = request.query; logger.info(f"SWM: QuerySWM called with KEMQuery: {query}")
        if query.embedding_query or query.text_query: msg = "Vector/text search not in SWM cache."; logger.warning(msg); await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT, msg); return swm_service_pb2.QuerySWMResponse() # type: ignore
        def _sync_query_swm_logic():
            pg_sz = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE; offset = 0
            if request.page_token:
                try: offset = int(request.page_token)
                except ValueError: logger.warning(f"Invalid page_token for QuerySWM: '{request.page_token}', using 0.")
            kems_l:List[kem_pb2.KEM]=[]; final_ids:Optional[Set[str]]=None; idx_meta_q:Dict[str,str]={}; unidx_meta_q:Dict[str,str]={}
            if query.metadata_filters:
                for k,v in query.metadata_filters.items(): (idx_meta_q if k in self.swm_cache._indexed_keys else unidx_meta_q)[k]=v
            if idx_meta_q:
                intersect_ids:Optional[Set[str]]=None
                for k,v in idx_meta_q.items():
                    ids_idx=self.swm_cache.get_ids_by_metadata_filter(k,v)
                    if intersect_ids is None: intersect_ids=ids_idx if ids_idx is not None else set()
                    else: intersect_ids.intersection_update(ids_idx if ids_idx is not None else set())
                    if not intersect_ids: break
                if not intersect_ids: return [],""
                final_ids=intersect_ids
            if final_ids is not None: [ (_k:=self.swm_cache.get(k_id)) and kems_l.append(_k) for k_id in final_ids] # type: ignore
            else: kems_l=self.swm_cache.values()
            if query.ids: ids_s=set(query.ids); kems_l=[k for k in kems_l if k.id in ids_s]
            if unidx_meta_q:
                tmp_kems=[]; [ (all(k.metadata.get(mk)==mv for mk,mv in unidx_meta_q.items())) and tmp_kems.append(k) for k in kems_l]; kems_l=tmp_kems
            def chk_dt(k_ts,s_ts,e_ts)->bool:
                if s_ts and (s_ts.seconds>0 or s_ts.nanos>0) and k_ts.ToNanoseconds()<s_ts.ToNanoseconds():return False
                if e_ts and (e_ts.seconds>0 or e_ts.nanos>0) and k_ts.ToNanoseconds()>e_ts.ToNanoseconds():return False
                return True
            if query.HasField("created_at_start") or query.HasField("created_at_end"): kems_l=[k for k in kems_l if chk_dt(k.created_at,query.created_at_start,query.created_at_end)]
            if query.HasField("updated_at_start") or query.HasField("updated_at_end"): kems_l=[k for k in kems_l if chk_dt(k.updated_at,query.updated_at_start,query.updated_at_end)]
            k_pg=kems_l[offset:offset+pg_sz]; n_tok=str(offset+pg_sz) if len(kems_l)>offset+pg_sz else ""
            return k_pg, n_tok
        k_p_sync,n_t_sync = await asyncio.to_thread(_sync_query_swm_logic)
        logger.info(f"QuerySWM: Returning {len(k_p_sync)} KEMs."); return swm_service_pb2.QuerySWMResponse(kems=k_p_sync,next_page_token=n_t_sync)
    async def LoadKEMsFromGLM(self, request: swm_service_pb2.LoadKEMsFromGLMRequest, context) -> swm_service_pb2.LoadKEMsFromGLMResponse:
        logger.info(f"SWM: LoadKEMsFromGLM query: {request.query_for_glm}");
        if not self.glm_stub: msg="GLM client N/A";logger.error(msg);await context.abort(grpc_aio.StatusCode.INTERNAL,msg); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore
        req=glm_service_pb2.RetrieveKEMsRequest(query=request.query_for_glm);load_c=0;load_ids_l=[];glm_qc=0
        try:
            logger.info(f"SWM: Req GLM.RetrieveKEMs(async): {req}"); resp=await self._glm_retrieve_kems_async(req,timeout=20)
            if resp and resp.kems:
                glm_qc=len(resp.kems);[await self._put_kem_to_cache_async(k) or load_ids_l.append(k.id) for k in resp.kems]
                load_c=len(load_ids_l); msg=f"Loaded {load_c} KEMs."
            else: msg="GLM no KEMs.";logger.info(msg)
            return swm_service_pb2.LoadKEMsFromGLMResponse(kems_queried_in_glm_count=glm_qc, kems_loaded_to_swm_count=load_c, loaded_kem_ids=load_ids_l, status_message=msg)
        except grpc.RpcError as e:msg=f"gRPC err GLM: {e.details()}";logger.error(msg);await context.abort(e.code(),msg); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore
        except Exception as e:msg=f"Err LoadKEMs: {e}";logger.error(msg,exc_info=True);await context.abort(grpc_aio.StatusCode.INTERNAL,msg); return swm_service_pb2.LoadKEMsFromGLMResponse() # type: ignore
    async def AcquireLock(self, request: swm_service_pb2.AcquireLockRequest, context) -> swm_service_pb2.AcquireLockResponse:
        rid,aid,t_ms,l_ms=request.resource_id,request.agent_id,request.timeout_ms,request.lease_duration_ms
        logger.info(f"SWM: AcquireLock '{rid}' by '{aid}', t={t_ms}, l={l_ms}");s_mono=time.monotonic()
        async with self.lock_condition:
            while True:
                now=int(time.time()*1000);lock=self.locks.get(rid)
                if lock and lock.lease_duration_ms>0 and now>=lock.lease_expires_at_unix_ms: logger.info(f"SWM: Lock '{rid}' by '{lock.agent_id}' expired. Removing.");del self.locks[rid];lock=None;self.lock_condition.notify_all()
                if not lock:
                    lid=str(uuid.uuid4());acq_at=now;exp_at = acq_at + l_ms if l_ms > 0 else 0
                    self.locks[rid]=LockInfoInternal(rid,aid,lid,acq_at,l_ms,exp_at);logger.info(f"SWM: Lock on '{rid}' acquired by '{aid}'.ID:{lid}.Exp:{exp_at or 'never'}")
                    return swm_service_pb2.AcquireLockResponse(resource_id=rid,agent_id=aid,status=swm_service_pb2.LockStatusValue.ACQUIRED,lock_id=lid,acquired_at_unix_ms=acq_at,lease_expires_at_unix_ms=exp_at,message="Acquired.")
                if lock.agent_id==aid:
                    logger.info(f"SWM: Lock on '{rid}' already held by '{aid}'.")
                    if l_ms>0:lock.lease_expires_at_unix_ms=now+l_ms;lock.lease_duration_ms=l_ms;logger.info(f"SWM: Lease for '{rid}' updated.Exp:{lock.lease_expires_at_unix_ms}")
                    elif l_ms==0 and lock.lease_duration_ms>0:lock.lease_expires_at_unix_ms=0;lock.lease_duration_ms=0;logger.info(f"SWM: Lease for '{rid}' indefinite.")
                    return swm_service_pb2.AcquireLockResponse(resource_id=rid,agent_id=aid,status=swm_service_pb2.LockStatusValue.ALREADY_HELD_BY_YOU,lock_id=lock.lock_id,acquired_at_unix_ms=lock.acquired_at_unix_ms,lease_expires_at_unix_ms=lock.lease_expires_at_unix_ms,message="Held.")
                if t_ms==0:logger.info(f"SWM: Lock on '{rid}' held by '{lock.agent_id}'.No wait for '{aid}'.");return swm_service_pb2.AcquireLockResponse(resource_id=rid,agent_id=aid,status=swm_service_pb2.LockStatusValue.NOT_AVAILABLE,message=f"Locked by {lock.agent_id}.")
                el_ms=(time.monotonic()-s_mono)*1000;wait_s:Optional[float]=None
                if t_ms>0:
                    rem_ms=t_ms-el_ms
                    if rem_ms<=0:logger.info(f"SWM: Timeout({t_ms}ms) for '{rid}',ag '{aid}'.");return swm_service_pb2.AcquireLockResponse(resource_id=rid,agent_id=aid,status=swm_service_pb2.LockStatusValue.TIMEOUT,message="Timeout.")
                    wait_s=rem_ms/1000.0
                logger.debug(f"SWM: Ag '{aid}' waits for '{rid}'.Wait_s:{wait_s}")
                try: await asyncio.wait_for(self.lock_condition.wait(),timeout=wait_s)
                except asyncio.TimeoutError: logger.info(f"SWM: Cond wait timeout for '{rid}', agent '{aid}'.")
                logger.debug(f"SWM: Ag '{aid}' awakened for '{rid}'.Re-check...")
    async def ReleaseLock(self, request: swm_service_pb2.ReleaseLockRequest, context) -> swm_service_pb2.ReleaseLockResponse:
        rid,ag_id,req_lid=request.resource_id,request.agent_id,request.lock_id;logger.info(f"SWM: ReleaseLock '{rid}' by '{ag_id}',req_lid='{req_lid}'")
        async with self.lock_condition:
            lock=self.locks.get(rid)
            if not lock:logger.warning(f"SWM: Release non-exist lock '{rid}'.");return swm_service_pb2.ReleaseLockResponse(resource_id=rid,status=swm_service_pb2.ReleaseStatusValue.NOT_HELD,message="Not found.")
            now=int(time.time()*1000)
            if lock.lease_duration_ms>0 and now>=lock.lease_expires_at_unix_ms:logger.info(f"SWM: Lock '{rid}' by '{lock.agent_id}' expired.Rem.");del self.locks[rid];self.lock_condition.notify_all();return swm_service_pb2.ReleaseLockResponse(resource_id=rid,status=swm_service_pb2.ReleaseStatusValue.NOT_HELD,message="Expired.")
            if lock.agent_id!=ag_id:logger.warning(f"SWM: Ag '{ag_id}' tries release lock '{rid}' held by '{lock.agent_id}'.Denied.");return swm_service_pb2.ReleaseLockResponse(resource_id=rid,status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING,message="Held by other.")
            if req_lid and lock.lock_id!=req_lid:logger.warning(f"SWM: Invalid lock_id ('{req_lid}') for ReleaseLock '{rid}'.Exp '{lock.lock_id}'.Denied.");return swm_service_pb2.ReleaseLockResponse(resource_id=rid,status=swm_service_pb2.ReleaseStatusValue.INVALID_LOCK_ID,message="Invalid lock ID.")
            del self.locks[rid];logger.info(f"SWM: Lock for '{rid}' released by '{ag_id}'.Notifying.");self.lock_condition.notify_all()
            return swm_service_pb2.ReleaseLockResponse(resource_id=rid,status=swm_service_pb2.ReleaseStatusValue.RELEASED,message="Released.")
    async def GetLockInfo(self, request: swm_service_pb2.GetLockInfoRequest, context) -> swm_service_pb2.LockInfo:
        rid=request.resource_id;logger.debug(f"SWM: GetLockInfo for '{rid}'")
        async with self.lock_condition:
            now=int(time.time()*1000);lock=self.locks.get(rid)
            if lock and lock.lease_duration_ms>0 and now>=lock.lease_expires_at_unix_ms:logger.info(f"SWM: Lock '{rid}' by '{lock.agent_id}' expired.Rem.");del self.locks[rid];lock=None;self.lock_condition.notify_all()
            if lock:return swm_service_pb2.LockInfo(resource_id=rid,is_locked=True,current_holder_agent_id=lock.agent_id,lock_id=lock.lock_id,acquired_at_unix_ms=lock.acquired_at_unix_ms,lease_expires_at_unix_ms=lock.lease_expires_at_unix_ms if lock.lease_duration_ms>0 else 0)
            else:return swm_service_pb2.LockInfo(resource_id=rid,is_locked=False)
    async def IncrementCounter(self, request: swm_service_pb2.IncrementCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        cid,inc_by=request.counter_id,request.increment_by;logger.info(f"SWM: IncCounter '{cid}', by {inc_by}")
        if not cid:await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT,"counter_id empty.");return swm_service_pb2.CounterValueResponse() # type: ignore
        async with self.counters_lock:cur_val=self.counters.get(cid,0);new_val=cur_val+inc_by;self.counters[cid]=new_val;logger.info(f"SWM: Counter '{cid}' upd. Old:{cur_val},New:{new_val}");return swm_service_pb2.CounterValueResponse(counter_id=cid,current_value=new_val,status_message="Updated.")
    async def GetCounter(self, request: swm_service_pb2.DistributedCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        cid=request.counter_id;logger.info(f"SWM: GetCounter for '{cid}'")
        if not cid:await context.abort(grpc_aio.StatusCode.INVALID_ARGUMENT,"counter_id empty.");return swm_service_pb2.CounterValueResponse() # type: ignore
        async with self.counters_lock:cur_val=self.counters.get(cid)
        if cur_val is None:logger.warning(f"SWM: Counter '{cid}' not found.");return swm_service_pb2.CounterValueResponse(counter_id=cid,current_value=0,status_message=f"Counter '{cid}' not found, default 0.")
        else:logger.info(f"SWM: Counter '{cid}', val: {cur_val}");return swm_service_pb2.CounterValueResponse(counter_id=cid,current_value=cur_val,status_message="Retrieved.")

async def serve():
    # Global config instance for serve function, if needed for listen address etc.
    # Or pass config to servicer instance if it needs it beyond __init__
    module_config = SWMConfig() # Load config for the serve function scope
    logger.setLevel(module_config.get_log_level_int()) # Ensure logger level is set based on loaded config

    server=grpc_aio.server();servicer=SharedWorkingMemoryServiceImpl() # Servicer will init its own config
    swm_service_pb2_grpc.add_SharedWorkingMemoryServiceServicer_to_server(servicer,server)
    await servicer.start_background_tasks() # Start tasks associated with the servicer instance

    listen_addr = servicer.config.GRPC_LISTEN_ADDRESS # Use config from servicer instance
    server.add_insecure_port(listen_addr);logger.info(f"Starting SWM async on {listen_addr}...");await server.start()
    logger.info(f"SWM started async on {listen_addr}.")
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
