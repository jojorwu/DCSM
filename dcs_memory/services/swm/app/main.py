import grpc
import grpc.aio as grpc_aio
import asyncio
import time
import sys
import os
import uuid
import logging
# import threading # No longer needed if IndexedLRUCache is fully replaced by Redis logic
import random

from dataclasses import dataclass, field
# from cachetools import LRUCache, Cache # No longer needed
# from dcs_memory.common.rw_lock import RWLockFair # No longer needed

import typing
from typing import Optional, List, Set, Dict, Callable, AsyncGenerator, Tuple

from dcs_memory.common.grpc_utils import async_retry_grpc_call # Import async retry decorator

# Attempt to import aioredis
try:
    import aioredis # type: ignore
except ImportError:
    aioredis = None # type: ignore
    logging.getLogger(__name__).warning("aioredis library not found. RedisKemCache will not be available.")


from .config import SWMConfig
from .managers import SubscriptionManager, DistributedLockManager, DistributedCounterManager
# Import RedisKemCache only if aioredis is available, or handle its absence
if aioredis:
    from .redis_kem_cache import RedisKemCache
else:
    RedisKemCache = None # type: ignore

logger = logging.getLogger(__name__)

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2
from generated_grpc import glm_service_pb2_grpc
from generated_grpc import swm_service_pb2
from generated_grpc import swm_service_pb2_grpc

# IndexedLRUCache class definition is removed.

class SharedWorkingMemoryServiceImpl(swm_service_pb2_grpc.SharedWorkingMemoryServiceServicer):
    def __init__(self):
        self.config = SWMConfig()
        logger.setLevel(self.config.get_log_level_int())
        logger.info(f"Initializing SharedWorkingMemoryServiceImpl...")

        self.aio_glm_channel: Optional[grpc_aio.Channel] = None
        self.aio_glm_stub: Optional[glm_service_pb2_grpc.GlobalLongTermMemoryStub] = None
        # Retryable error codes are handled by the async_retry_grpc_call decorator by default.
        # If specific codes are needed for SWM's GLM calls, they can be configured and passed.
        try:
            if self.config.GLM_SERVICE_ADDRESS:
                self.aio_glm_channel = grpc_aio.insecure_channel(self.config.GLM_SERVICE_ADDRESS)
                self.aio_glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.aio_glm_channel)
                logger.info(f"GLM client (async) for SWM initialized, target: {self.config.GLM_SERVICE_ADDRESS}")
            else:
                logger.warning("GLM_SERVICE_ADDRESS not configured. GLM features will be unavailable in SWM.")
        except Exception as e:
            logger.error(f"Error initializing asynchronous GLM client in SWM: {e}", exc_info=True)
            self.aio_glm_stub = None
            if self.aio_glm_channel: logger.warning("SWM: GLM async channel was created but stub initialization failed.")

        self.redis_client: Optional[aioredis.Redis] = None # type: ignore
        self.redis_kem_cache: Optional[RedisKemCache] = None # type: ignore
        self._redis_keyspace_listener_task: Optional[asyncio.Task] = None

        # Initialize Redis client and RedisKemCache.
        # Note: SWMConfig.CACHE_MAX_SIZE is not directly used by RedisKemCache;
        # Redis's own 'maxmemory' policies handle eviction.
        # SWMConfig.INDEXED_METADATA_KEYS is used by RedisKemCache for setting up secondary indexes.
        if aioredis and RedisKemCache:
            try:
                redis_url = f"redis://{self.config.SWM_REDIS_HOST}:{self.config.SWM_REDIS_PORT}/{self.config.SWM_REDIS_DB}"
                if self.config.SWM_REDIS_PASSWORD:
                    redis_url = f"redis://:{self.config.SWM_REDIS_PASSWORD}@{self.config.SWM_REDIS_HOST}:{self.config.SWM_REDIS_PORT}/{self.config.SWM_REDIS_DB}"
                self.redis_client = aioredis.from_url(redis_url, encoding="utf-8", decode_responses=False)
                self.redis_kem_cache = RedisKemCache(self.redis_client, self.config) # type: ignore
                logger.info(f"RedisKemCache initialized for SWM. Target: {redis_url}")
            except AttributeError:
                 logger.critical(f"SWM: CRITICAL ERROR - aioredis.from_url not found. Ensure aioredis v2+ is installed.", exc_info=True)
                 self.redis_client = None; self.redis_kem_cache = None
                 raise SystemExit("SWM: Failed to initialize Redis client due to aioredis version/attribute issue.")
            except Exception as e_redis:
                logger.critical(f"SWM: CRITICAL ERROR - Failed to connect to Redis or initialize RedisKemCache: {e_redis}", exc_info=True)
                self.redis_client = None; self.redis_kem_cache = None
                raise SystemExit(f"SWM: Failed to initialize Redis connection: {e_redis}")
        else:
            logger.critical("SWM: aioredis library or RedisKemCache not available. SWM cannot start without a cache backend.")
            raise SystemExit("SWM: Missing Redis dependencies.")

        self.subscription_manager = SubscriptionManager(self.config)
        self.lock_manager = DistributedLockManager(self.config)
        self.counter_manager = DistributedCounterManager(self.config)

        self._stop_event = asyncio.Event()
        self._glm_persistence_worker_task: Optional[asyncio.Task] = None
        self.glm_persistence_queue: asyncio.Queue[Tuple[kem_pb2.KEM, int]] = asyncio.Queue(
            maxsize=self.config.GLM_PERSISTENCE_QUEUE_MAX_SIZE
        )
        logger.info("SharedWorkingMemoryServiceImpl core components initialized.")

    async def start_background_tasks(self):
        self._stop_event.clear()
        await self.lock_manager.start_cleanup_task()

        if self._glm_persistence_worker_task is None or self._glm_persistence_worker_task.done():
            self._glm_persistence_worker_task = asyncio.create_task(self._glm_persistence_worker())
            logger.info("SWM: GLM persistence worker task started.")

        if self.redis_client and RedisKemCache and self.redis_kem_cache and \
           (self._redis_keyspace_listener_task is None or self._redis_keyspace_listener_task.done()):
            self._redis_keyspace_listener_task = asyncio.create_task(self._listen_for_redis_evictions())
        elif not self.redis_client or not RedisKemCache or not self.redis_kem_cache:
             logger.warning("SWM: Redis client/cache not available, keyspace listener not started.")

    async def stop_background_tasks(self):
        logger.info("SWM: Stopping background tasks...")
        self._stop_event.set()
        await self.lock_manager.stop_cleanup_task()

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

        if self._redis_keyspace_listener_task and not self._redis_keyspace_listener_task.done():
            logger.info("SWM: Stopping Redis keyspace listener task...")
            self._redis_keyspace_listener_task.cancel()
            try: await self._redis_keyspace_listener_task
            except asyncio.CancelledError: logger.info("SWM: Redis keyspace listener task was cancelled during shutdown.")
            except Exception as e_stop_listener: logger.error(f"SWM: Error stopping Redis keyspace listener: {e_stop_listener}", exc_info=True)
        self._redis_keyspace_listener_task = None

        if self.aio_glm_channel:
            logger.info("SWM: Closing GLM client (async) channel.")
            try: await self.aio_glm_channel.close()
            except Exception as e: logger.error(f"SWM: Error closing GLM async channel: {e}", exc_info=True)
            self.aio_glm_channel = None; self.aio_glm_stub = None

        if self.redis_client:
            logger.info("SWM: Closing Redis client connection.")
            try: await self.redis_client.close()
            except Exception as e: logger.error(f"SWM: Error closing Redis client: {e}", exc_info=True)
            self.redis_client = None; self.redis_kem_cache = None

    async def _listen_for_redis_evictions(self):
        if not self.redis_client or not RedisKemCache or not self.redis_kem_cache:
            logger.error("SWM Redis Eviction Listener: Prerequisites not met. Listener cannot start.")
            return

        # This listener subscribes to Redis Keyspace Notifications, specifically for 'evicted' events.
        # For this to work, Redis server must have `notify-keyspace-events` configured
        # to include at least 'E' (keyevent events, published with __keyevent@<db>__)
        # and 'x' (expired events, for TTL-based evictions if used) or 'e' (evicted events, for maxmemory policies).
        # A common setting is 'Ex' or 'KEA' (Keyspace, Keyevent, All - for broadness, though more specific is fine).
        pubsub = None
        evicted_channel = f"__keyevent@{self.config.SWM_REDIS_DB}__:evicted"
        logger.info(f"SWM Redis Eviction Listener: Task started. Attempting to subscribe to '{evicted_channel}'. "
                    "Ensure Redis `notify-keyspace-events` is correctly configured (e.g., with 'Ex' or 'KEA').")

        consecutive_subscribe_errors = 0
        # Use a config value if available, otherwise a sensible default
        max_consecutive_subscribe_errors = getattr(self.config, 'SWM_REDIS_MAX_PUBSUB_RETRIES', 5)
        reconnect_delay = getattr(self.config, 'SWM_REDIS_RECONNECT_DELAY_S', 5.0)

        while not self._stop_event.is_set():
            try:
                if pubsub is None:
                    if not self.redis_client:
                        logger.error("SWM Redis Eviction Listener: Main Redis client is None. Cannot subscribe.")
                        await asyncio.sleep(reconnect_delay)
                        continue
                    try:
                        await self.redis_client.ping() # Check main client connection
                    except (aioredis.exceptions.ConnectionError, ConnectionRefusedError, aioredis.exceptions.RedisError) as ping_e: # type: ignore
                        logger.error(f"SWM Redis Eviction Listener: Redis ping failed: {ping_e}. Waiting to reconnect.")
                        consecutive_subscribe_errors += 1
                        if consecutive_subscribe_errors > max_consecutive_subscribe_errors:
                            logger.critical(f"SWM Redis Eviction Listener: Exceeded max ping errors ({max_consecutive_subscribe_errors}). Stopping listener.")
                            break
                        await asyncio.sleep(reconnect_delay)
                        continue

                    try:
                        pubsub = self.redis_client.pubsub()
                        await pubsub.subscribe(evicted_channel)
                        logger.info(f"SWM Redis Eviction Listener: Successfully subscribed to '{evicted_channel}'")
                        consecutive_subscribe_errors = 0
                    except (aioredis.exceptions.ConnectionError, aioredis.exceptions.RedisError, ConnectionRefusedError) as sub_e: # type: ignore
                        logger.error(f"SWM Redis Eviction Listener: Failed to subscribe to '{evicted_channel}': {sub_e}. Will retry.")
                        if pubsub: # Try to clean up potentially partially initialized pubsub
                            try: await pubsub.unsubscribe(evicted_channel)
                            except: pass
                        pubsub = None
                        consecutive_subscribe_errors += 1
                        if consecutive_subscribe_errors > max_consecutive_subscribe_errors:
                            logger.critical(f"SWM Redis Eviction Listener: Exceeded max subscribe errors ({max_consecutive_subscribe_errors}). Stopping listener.")
                            break
                        await asyncio.sleep(reconnect_delay)
                        continue

                # If pubsub is still None here, it means all attempts to subscribe failed, and we should exit or wait more.
                # The loop structure with `continue` should handle this.
                if pubsub is None: # Should ideally not be reached if max_consecutive_subscribe_errors causes break
                    logger.error("SWM Redis Eviction Listener: PubSub object is None, cannot get message. Waiting.")
                    await asyncio.sleep(reconnect_delay)
                    continue

                message = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0), timeout=1.1)

                if message and message.get('type') == 'message':
                    evicted_key_bytes = message.get('data')
                    if isinstance(evicted_key_bytes, bytes):
                        evicted_key_str = evicted_key_bytes.decode('utf-8', errors='ignore')
                        if evicted_key_str.startswith(self.redis_kem_cache.kem_key_prefix):
                            kem_id = evicted_key_str[len(self.redis_kem_cache.kem_key_prefix):]
                            logger.info(f"SWM Redis Eviction Listener: Received eviction for KEM ID: {kem_id}")
                            evicted_kem_stub = kem_pb2.KEM(id=kem_id)
                            await self.subscription_manager.notify_kem_event(
                                kem=evicted_kem_stub,
                                event_type=swm_service_pb2.SWMMemoryEvent.EventType.KEM_EVICTED,
                                source_agent_id="SWM_REDIS_EVICTION"
                            )
            except asyncio.TimeoutError:
                if self._stop_event.is_set(): logger.info("SWM Redis Eviction Listener: Stop event detected during get_message timeout."); break
                continue
            except asyncio.CancelledError:
                logger.info("SWM Redis Eviction Listener: Task cancelled.")
                break
            except (aioredis.exceptions.ConnectionError, aioredis.exceptions.RedisError) as e_conn_loop: # type: ignore
                logger.error(f"SWM Redis Eviction Listener: Redis PubSub connection error in loop: {e_conn_loop}. Attempting to reset.", exc_info=False)
                if pubsub:
                    try: await pubsub.unsubscribe(evicted_channel)
                    except: pass
                pubsub = None
                consecutive_subscribe_errors += 1 # Count this as a subscribe error for retry limiting
                if consecutive_subscribe_errors > max_consecutive_subscribe_errors:
                    logger.critical(f"SWM Redis Eviction Listener: Exceeded max consecutive connection errors in loop ({max_consecutive_subscribe_errors}). Stopping listener.")
                    break
                if self._stop_event.is_set(): break
                await asyncio.sleep(reconnect_delay)
            except Exception as e:
                logger.error(f"SWM Redis Eviction Listener: Unexpected error processing message: {e}", exc_info=True)
                if self._stop_event.is_set(): break
                await asyncio.sleep(1)

        logger.info("SWM Redis Eviction Listener: Initiating shutdown of listener.")
        if pubsub:
            try:
                await pubsub.unsubscribe(evicted_channel)
                logger.info(f"SWM Redis Eviction Listener: Unsubscribed from {evicted_channel}.")
            except Exception as e_final_unsub:
                 logger.error(f"SWM Redis Eviction Listener: Error during final pubsub cleanup: {e_final_unsub}", exc_info=True)
        logger.info("SWM Redis Eviction Listener: Stopped.")

    # --- GLM Client Methods (using async_retry_grpc_call decorator) ---
    @async_retry_grpc_call(
        max_attempts=config.RETRY_MAX_ATTEMPTS,
        initial_delay_s=config.RETRY_INITIAL_DELAY_S,
        backoff_factor=config.RETRY_BACKOFF_FACTOR,
        jitter_fraction=config.RETRY_JITTER_FRACTION
    )
    async def _glm_retrieve_kems_async_with_retry(self, request: glm_service_pb2.RetrieveKEMsRequest, timeout: int) -> glm_service_pb2.RetrieveKEMsResponse:
        if not self.aio_glm_stub:
            logger.error("SWM: GLM async client not available for RetrieveKEMs.")
            raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "SWM Internal Error: GLM client not available")
        logger.debug(f"Calling GLM RetrieveKEMs (async, with retry). Timeout: {timeout}s")
        return await self.aio_glm_stub.RetrieveKEMs(request, timeout=timeout)

    @async_retry_grpc_call(
        max_attempts=config.RETRY_MAX_ATTEMPTS,
        initial_delay_s=config.RETRY_INITIAL_DELAY_S,
        backoff_factor=config.RETRY_BACKOFF_FACTOR,
        jitter_fraction=config.RETRY_JITTER_FRACTION
    )
    async def _glm_store_kem_async_with_retry(self, request: glm_service_pb2.StoreKEMRequest, timeout: int) -> glm_service_pb2.StoreKEMResponse:
        if not self.aio_glm_stub:
            logger.error("SWM: GLM async client not available for StoreKEM.");
            raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "SWM Internal Error: GLM client not available")
        logger.debug(f"Calling GLM StoreKEM (async, with retry). Timeout: {timeout}s")
        return await self.aio_glm_stub.StoreKEM(request, timeout=timeout)

    @async_retry_grpc_call(
        max_attempts=config.RETRY_MAX_ATTEMPTS,
        initial_delay_s=config.RETRY_INITIAL_DELAY_S,
        backoff_factor=config.RETRY_BACKOFF_FACTOR,
        jitter_fraction=config.RETRY_JITTER_FRACTION
    )
    async def _glm_batch_store_kems_async_with_retry(self, request: glm_service_pb2.BatchStoreKEMsRequest, timeout: int) -> glm_service_pb2.BatchStoreKEMsResponse:
        if not self.aio_glm_stub:
            logger.error("SWM: GLM async client not available for BatchStoreKEMs.");
            raise grpc_aio.AioRpcError(grpc.StatusCode.INTERNAL, "SWM Internal Error: GLM client not available")
        logger.debug(f"Calling GLM BatchStoreKEMs (async, with retry). KEMs: {len(request.kems)}, Timeout: {timeout}s")
        # The decorator handles retries, so the direct try-except for RpcError here is less critical
        # unless for very specific non-retryable error handling within the method itself.
        # For now, let the decorator handle it.
        response = await self.aio_glm_stub.BatchStoreKEMs(request, timeout=timeout)
        if response and response.failed_kem_references: # Check if response is not None
             logger.warning(f"SWM: GLM BatchStoreKEMs (after retry) reported {len(response.failed_kem_references)} failures. Refs: {response.failed_kem_references}")
        elif response:
             logger.info(f"SWM: GLM BatchStoreKEMs (after retry) processed. Success: {len(response.successfully_stored_kems)}, Failures: {len(response.failed_kem_references if response.failed_kem_references else [])}")
        # If decorator exhausts retries, it will raise an exception, which will be caught by the caller of this method.
        return response


    async def _glm_persistence_worker(self):
        logger.info("SWM: GLM Persistence Worker started.")
        while not self._stop_event.is_set():
            current_batch_items_with_retry: List[Tuple[kem_pb2.KEM, int]] = []
            try:
                first_item_tuple = await asyncio.wait_for(
                    self.glm_persistence_queue.get(),
                    timeout=self.config.GLM_PERSISTENCE_FLUSH_INTERVAL_S
                )
                current_batch_items_with_retry.append(first_item_tuple)
                self.glm_persistence_queue.task_done()

                while len(current_batch_items_with_retry) < self.config.GLM_PERSISTENCE_BATCH_SIZE:
                    try:
                        item_tuple = self.glm_persistence_queue.get_nowait()
                        current_batch_items_with_retry.append(item_tuple)
                        self.glm_persistence_queue.task_done()
                    except asyncio.QueueEmpty:
                        break
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                logger.info("SWM: GLM Persistence Worker cancelled during queue get.")
                for kem_proto, retry_c in reversed(current_batch_items_with_retry):
                    if not self.glm_persistence_queue.full(): await self.glm_persistence_queue.put((kem_proto, retry_c))
                break
            except Exception as e_q_get:
                logger.error(f"SWM: GLM Persistence Worker error getting from queue: {e_q_get}", exc_info=True)
                await asyncio.sleep(1)
                continue

            if current_batch_items_with_retry:
                kems_for_glm_call = [item[0] for item in current_batch_items_with_retry]
                logger.info(f"SWM: GLM Persistence Worker processing batch of {len(kems_for_glm_call)} KEMs (Retry counts: {[item[1] for item in current_batch_items_with_retry]})")

                glm_batch_call_successful = False
                glm_response = None
                request_for_glm_batch = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_for_glm_call)

                if self.aio_glm_stub:
                    try:
                        glm_response = await self._glm_batch_store_kems_async_with_retry( # Use retry method
                            request_for_glm_batch,
                            timeout=self.config.GLM_BATCH_STORE_TIMEOUT_S # Use configured timeout
                        )
                        if glm_response: # Check if response itself is not None
                            glm_batch_call_successful = True # Indicates the call completed, check content for partial fails
                            successfully_processed_in_glm_ids = {k.id for k in glm_response.successfully_stored_kems}
                        # If glm_response is None (can happen if decorator returns None on exhaustion, though it should raise)
                        # or if an exception occurred that wasn't an RpcError handled by decorator,
                        # glm_batch_call_successful remains False.
                    except grpc_aio.AioRpcError as e_batch_call:
                        logger.error(f"SWM: GLM Persistence Worker: BatchStoreKEMs call failed with gRPC error: {e_batch_call.code()} - {e_batch_call.details()}")
                        # glm_batch_call_successful remains False
                    except Exception as e_generic_batch_call:
                        logger.error(f"SWM: GLM Persistence Worker: BatchStoreKEMs call failed with non-gRPC error: {e_generic_batch_call}", exc_info=True)
                        # glm_batch_call_successful remains False

                    if glm_batch_call_successful and glm_response:

                        if glm_response.successfully_stored_kems:
                            logger.info(f"SWM: GLM Worker: {len(glm_response.successfully_stored_kems)} KEMs confirmed persisted by GLM. Updating SWM cache.")
                            for kem_from_glm in glm_response.successfully_stored_kems:
                                try:
                                    await self._put_kem_to_cache_and_notify_async(kem_from_glm)
                                    logger.debug(f"SWM: Cache updated with GLM version for KEM ID '{kem_from_glm.id}'.")
                                except Exception as e_cache_upd:
                                    logger.error(f"SWM: GLM Worker: Error updating SWM cache for KEM ID '{kem_from_glm.id}': {e_cache_upd}", exc_info=True)

                        if glm_response.failed_kem_references:
                            failed_ids_from_glm = set(glm_response.failed_kem_references)
                            logger.warning(f"SWM: GLM Worker: BatchStoreKEMs reported {len(failed_ids_from_glm)} failed KEM IDs: {failed_ids_from_glm}.")
                            items_to_requeue_from_failed_refs = []
                            for item_kem_proto, item_retry_count in current_batch_items_with_retry:
                                if item_kem_proto.id in failed_ids_from_glm and item_kem_proto.id not in successfully_processed_in_glm_ids:
                                    if item_retry_count < self.config.GLM_PERSISTENCE_BATCH_MAX_RETRIES:
                                        items_to_requeue_from_failed_refs.append((item_kem_proto, item_retry_count + 1))
                                        logger.info(f"SWM: GLM Worker: Marking KEM ID '{item_kem_proto.id}' for re-queue (GLM specific fail, next attempt {item_retry_count + 1}).")
                                    else:
                                        logger.error(f"SWM: GLM Worker: KEM ID '{item_kem_proto.id}' (GLM specific fail) reached max retries ({self.config.GLM_PERSISTENCE_BATCH_MAX_RETRIES}). Discarding.")

                            for item_to_requeue in reversed(items_to_requeue_from_failed_refs):
                                if not self.glm_persistence_queue.full(): await self.glm_persistence_queue.put(item_to_requeue)
                                else: logger.error(f"SWM: GLM Worker: Failed to re-queue KEM ID '{item_to_requeue[0].id}' (GLM specific fail), queue full. KEM may be lost."); break

                if not glm_batch_call_successful:
                    # This block handles cases where the BatchStoreKEMs call itself failed (e.g. network, GLM unavailable)
                    # or if self.aio_glm_stub was None to begin with.
                    if not self.aio_glm_stub:
                        logger.error("SWM: GLM Persistence Worker: GLM stub not available. Batch not persisted.")
                    else: # Implies call failed or glm_response was None/empty
                        logger.error(f"SWM: GLM Persistence Worker: Batch of {len(kems_for_glm_call)} KEMs potentially failed GLM processing (call failed or empty/error response).")

                    logger.warning(f"SWM: GLM Persistence Worker: Re-queueing {len(current_batch_items_with_retry)} items from failed/unprocessed batch.")
                    for kem_proto_retry, retry_count_retry in reversed(current_batch_items_with_retry):
                        if retry_count_retry < self.config.GLM_PERSISTENCE_BATCH_MAX_RETRIES:
                            if not self.glm_persistence_queue.full():
                                await self.glm_persistence_queue.put((kem_proto_retry, retry_count_retry + 1))
                                logger.info(f"SWM: GLM Persistence Worker: Re-queued KEM ID '{kem_proto_retry.id}' (batch processing failure, next attempt {retry_count_retry + 1}).")
                            else:
                                logger.error(f"SWM: GLM Persistence Worker: Failed to re-queue KEM ID '{kem_proto_retry.id}', queue full. KEM may be lost.")
                                break # Stop trying to re-queue if queue is full
                        else:
                            logger.error(f"SWM: GLM Persistence Worker: KEM ID '{kem_proto_retry.id}' (batch processing failure) reached max retries ({self.config.GLM_PERSISTENCE_BATCH_MAX_RETRIES}). Discarding.")

            if self._stop_event.is_set() and self.glm_persistence_queue.empty():
                 logger.info("SWM: GLM Persistence Worker stopping as stop event is set and queue is empty.")
                 break

        if not self.glm_persistence_queue.empty():
            logger.info(f"SWM: GLM Persistence Worker - processing remaining {self.glm_persistence_queue.qsize()} items on shutdown...")
            final_batch_to_send: List[kem_pb2.KEM] = []
            processed_during_shutdown_count = 0
            while not self.glm_persistence_queue.empty():
                try:
                    kem_tuple_final = self.glm_persistence_queue.get_nowait()
                    final_batch_to_send.append(kem_tuple_final[0])
                    self.glm_persistence_queue.task_done(); processed_during_shutdown_count +=1
                    if len(final_batch_to_send) >= self.config.GLM_PERSISTENCE_BATCH_SIZE:
                        if self.aio_glm_stub:
                            logger.info(f"SWM (Shutdown): Sending batch of {len(final_batch_to_send)} KEMs.");
                            final_req = glm_service_pb2.BatchStoreKEMsRequest(kems=final_batch_to_send)
                            try:
                                await self._glm_batch_store_kems_async_with_retry(final_req, timeout=self.config.GLM_BATCH_STORE_TIMEOUT_S)
                            except Exception as e_final_batch:
                                logger.error(f"SWM (Shutdown): Error sending final batch: {e_final_batch}", exc_info=True)
                        else:
                            logger.error(f"SWM (Shutdown): GLM stub gone, cannot send {len(final_batch_to_send)} KEMs.")
                        final_batch_to_send.clear()
                except asyncio.QueueEmpty: break

            if final_batch_to_send and self.aio_glm_stub:
                logger.info(f"SWM: GLM Persistence Worker - sending final batch of {len(final_batch_to_send)} KEMs to GLM during shutdown.")
                final_req_on_exit = glm_service_pb2.BatchStoreKEMsRequest(kems=final_batch_to_send)
                try:
                    await self._glm_batch_store_kems_async_with_retry(final_req_on_exit, timeout=self.config.GLM_BATCH_STORE_TIMEOUT_S)
                except Exception as e_final_batch_exit:
                    logger.error(f"SWM (Shutdown): Error sending final batch on exit: {e_final_batch_exit}", exc_info=True)
            elif final_batch_to_send:
                logger.error(f"SWM: GLM Persistence Worker - GLM stub not available for final batch of {len(final_batch_to_send)} KEMs during shutdown. Data may be lost.")
            logger.info(f"SWM: GLM Persistence Worker processed {processed_during_shutdown_count} items during shutdown flush.")
        logger.info("SWM: GLM Persistence Worker stopped.")

    async def _put_kem_to_cache_and_notify_async(self, kem: kem_pb2.KEM) -> None:
        if not kem or not kem.id:
            logger.warning("SWM: Invalid KEM (no ID or None) provided to _put_kem_to_cache_and_notify_async.")
            return

        if not self.redis_kem_cache:
            logger.error("SWM: RedisKemCache not available in _put_kem_to_cache_and_notify_async. Cannot process KEM.")
            return

        try:
            was_present_in_cache = await self.redis_kem_cache.contains(kem.id)
            await self.redis_kem_cache.set(kem.id, kem)
            logger.info(f"SWM: KEM ID '{kem.id}' set in RedisKemCache.")

            event_type = swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED if was_present_in_cache else swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED
            await self.subscription_manager.notify_kem_event(kem, event_type, "SWM_CACHE")
        except NotImplementedError:
             logger.error(f"SWM: RedisKemCache method not implemented while processing KEM ID '{kem.id}'. Caching/notification might be incomplete.", exc_info=True)
        except Exception as e:
            logger.error(f"SWM: Error during Redis cache operation or notification for KEM ID '{kem.id}': {e}", exc_info=True)

    async def PublishKEMToSWM(self, request: swm_service_pb2.PublishKEMToSWMRequest, context) -> swm_service_pb2.PublishKEMToSWMResponse:
        kem_to_publish = request.kem_to_publish
        kem_id_final = kem_to_publish.id or str(uuid.uuid4())
        kem_to_publish.id = kem_id_final

        if not request.kem_to_publish.id: logger.info(f"SWM: No ID provided for KEM to publish, new ID generated: '{kem_id_final}'")
        ts = Timestamp(); ts.GetCurrentTime()

        existing_kem_in_cache: Optional[kem_pb2.KEM] = None
        if self.redis_kem_cache:
            try:
                existing_kem_in_cache = await self.redis_kem_cache.get(kem_id_final)
            except NotImplementedError:
                logger.warning(f"SWM: RedisKemCache.get() not implemented, cannot check for existing KEM '{kem_id_final}' to preserve created_at.")
            except Exception as e_get_cache:
                logger.error(f"SWM: Error getting KEM '{kem_id_final}' from RedisKemCache: {e_get_cache}", exc_info=True)
        else:
            logger.error("SWM: RedisKemCache not available in PublishKEMToSWM. Cannot reliably set created_at.")

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
            elif not self.redis_kem_cache:
                persistence_status_message = f"SWM Cache (Redis) not available. KEM ID '{kem_id_final}' not queued for persistence."
                logger.error(persistence_status_message)
            else:
                try:
                    if self.glm_persistence_queue.full():
                        persistence_status_message = f"GLM persistence queue is full. KEM ID '{kem_id_final}' not queued."
                        logger.error(persistence_status_message)
                    else:
                        await self.glm_persistence_queue.put((kem_to_publish, 0))
                        queued_for_glm_persistence = True
                        persistence_status_message = f"KEM ID '{kem_id_final}' queued for persistence to GLM (attempt 0)."
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
        agent_id_str = request.agent_id or str(uuid.uuid4())
        event_q = await self.subscription_manager.add_subscriber(
            subscriber_id=agent_id_str, topics=request.topics, requested_q_size=request.requested_queue_size
        )
        logger.info(f"SWM: Subscriber '{agent_id_str}' connected for event stream.")
        idle_timeouts = 0
        try:
            while context.is_active():
                try:
                    event = await asyncio.wait_for(event_q.get(), timeout=self.config.SUBSCRIBER_IDLE_CHECK_INTERVAL_S)
                    yield event
                    event_q.task_done()
                    idle_timeouts = 0
                except asyncio.TimeoutError:
                    if not context.is_active(): logger.info(f"SWM stream for '{agent_id_str}': gRPC context inactive during idle check."); break
                    idle_timeouts += 1
                    logger.debug(f"SWM stream for '{agent_id_str}': idle timeout #{idle_timeouts} (threshold: {self.config.SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD}).")
                    if idle_timeouts >= self.config.SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD:
                        logger.warning(f"SWM stream for '{agent_id_str}': disconnecting due to inactivity (idle threshold reached).")
                        break
                except Exception as e_stream:
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
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Vector/text search not supported directly in SWM cache.")
            return swm_service_pb2.QuerySWMResponse()

        if not self.redis_kem_cache:
            logger.error("SWM: RedisKemCache not available for QuerySWM. Querying not possible.")
            await context.abort(grpc.StatusCode.INTERNAL, "SWM Cache (Redis) not available.")
            return swm_service_pb2.QuerySWMResponse()

        page_s = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
        page_tok = request.page_token

        try:
            kems_page, next_page_tok_str = await self.redis_kem_cache.query_by_filters(query, page_s, page_tok)
            logger.info(f"QuerySWM: Returning {len(kems_page)} KEMs from RedisKemCache query.");
            return swm_service_pb2.QuerySWMResponse(kems=kems_page, next_page_token=next_page_tok_str)
        except NotImplementedError:
            logger.error("SWM: RedisKemCache.query_by_filters() is not yet implemented.")
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Query filtering via Redis is not fully implemented.")
            return swm_service_pb2.QuerySWMResponse()
        except Exception as e_query_redis:
            logger.error(f"SWM: Error during QuerySWM from RedisKemCache: {e_query_redis}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, "Error querying SWM cache.")
            return swm_service_pb2.QuerySWMResponse()

    async def LoadKEMsFromGLM(self, request: swm_service_pb2.LoadKEMsFromGLMRequest, context) -> swm_service_pb2.LoadKEMsFromGLMResponse:
        logger.info(f"SWM: LoadKEMsFromGLM request with GLM query: {request.query_for_glm}")
        if not self.aio_glm_stub:
            msg = "GLM client not available in SWM for LoadKEMsFromGLM."
            logger.error(msg); await context.abort(grpc.StatusCode.INTERNAL, msg); return swm_service_pb2.LoadKEMsFromGLMResponse()

        if not self.redis_kem_cache:
             msg = "SWM cache (Redis) not available for LoadKEMsFromGLM."
             logger.error(msg); await context.abort(grpc.StatusCode.INTERNAL, msg); return swm_service_pb2.LoadKEMsFromGLMResponse()

        glm_req = glm_service_pb2.RetrieveKEMsRequest(query=request.query_for_glm)
        loaded_count = 0; queried_in_glm_count = 0; loaded_ids = []
        status_message = "GLM query initiated."

        try:
            logger.info(f"SWM: Requesting GLM.RetrieveKEMs with: {glm_req}")
        glm_response = await self._glm_retrieve_kems_async_with_retry( # Use retry method
            glm_req,
            timeout=self.config.GLM_RETRIEVE_TIMEOUT_S # Use configured timeout
        )

            if glm_response and glm_response.kems:
                queried_in_glm_count = len(glm_response.kems)
                logger.info(f"SWM: Received {queried_in_glm_count} KEMs from GLM.")
                for kem_from_glm in glm_response.kems:
                    await self._put_kem_to_cache_and_notify_async(kem_from_glm)
                    loaded_ids.append(kem_from_glm.id)
                loaded_count = len(loaded_ids)
                status_message = f"Successfully loaded {loaded_count} KEMs from GLM into SWM cache (Redis)."
                if queried_in_glm_count != loaded_count :
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
            await context.abort(e.code(), status_message); return swm_service_pb2.LoadKEMsFromGLMResponse()
        except Exception as e_load:
            status_message = f"Unexpected error during LoadKEMsFromGLM: {e_load}"
            logger.error(status_message, exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, status_message); return swm_service_pb2.LoadKEMsFromGLMResponse()

    async def AcquireLock(self, request: swm_service_pb2.AcquireLockRequest, context) -> swm_service_pb2.AcquireLockResponse:
        return await self.lock_manager.acquire_lock(request.resource_id, request.agent_id, request.timeout_ms, request.lease_duration_ms)

    async def ReleaseLock(self, request: swm_service_pb2.ReleaseLockRequest, context) -> swm_service_pb2.ReleaseLockResponse:
        return await self.lock_manager.release_lock(request.resource_id, request.agent_id, request.lock_id)

    async def GetLockInfo(self, request: swm_service_pb2.GetLockInfoRequest, context) -> swm_service_pb2.LockInfo:
        return await self.lock_manager.get_lock_info(request.resource_id)

    async def IncrementCounter(self, request: swm_service_pb2.IncrementCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        if not request.counter_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "counter_id cannot be empty.")
            return swm_service_pb2.CounterValueResponse()
        return await self.counter_manager.increment_counter(request.counter_id, request.increment_by)

    async def GetCounter(self, request: swm_service_pb2.DistributedCounterRequest, context) -> swm_service_pb2.CounterValueResponse:
        if not request.counter_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "counter_id cannot be empty.")
            return swm_service_pb2.CounterValueResponse()
        return await self.counter_manager.get_counter(request.counter_id)

async def serve():
    module_cfg = SWMConfig()
    logging.basicConfig(
        level=module_cfg.get_log_level_int(),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True
    )

    server=grpc_aio.server()
    try:
        servicer_instance=SharedWorkingMemoryServiceImpl()
    except SystemExit as e_init_fail:
        logger.critical(f"SWM Servicer initialization failed critically, cannot start server: {e_init_fail}")
        return

    swm_service_pb2_grpc.add_SharedWorkingMemoryServiceServicer_to_server(servicer_instance,server)

    listen_addr = servicer_instance.config.GRPC_LISTEN_ADDRESS
    server.add_insecure_port(listen_addr)

    logger.info(f"Starting SWM async server on {listen_addr}...")
    try:
        await server.start()
        if servicer_instance.redis_kem_cache:
             await servicer_instance.start_background_tasks()
        else: # Fallback if Redis is not available but we didn't SystemExit in __init__
            logger.warning("SWM: Redis not available, some background tasks (like eviction listener) not started.")
            # Start non-Redis dependent tasks
            await servicer_instance.lock_manager.start_cleanup_task()
            if servicer_instance._glm_persistence_worker_task is None or servicer_instance._glm_persistence_worker_task.done():
                 servicer_instance._glm_persistence_worker_task = asyncio.create_task(servicer_instance._glm_persistence_worker())
                 logger.info("SWM: GLM persistence worker task started (Redis not available).")

        logger.info(f"SWM server started and listening on {listen_addr}.")
        await server.wait_for_termination()
    except SystemExit:
         logger.critical("SWM server startup aborted due to critical initialization error (caught in serve).")
    except KeyboardInterrupt: logger.info("SWM server stopping via KeyboardInterrupt...")
    except asyncio.CancelledError: logger.info("SWM server task cancelled (e.g. by Docker stop).")
    except Exception as e_serve_start:
        logger.critical(f"SWM server failed to start or run properly: {e_serve_start}", exc_info=True)
    finally:
        logger.info("SWM server: Initiating graceful shutdown of servicer components...")
        if 'servicer_instance' in locals() and servicer_instance:
            await servicer_instance.stop_background_tasks()
        logger.info("SWM server: Stopping gRPC server...")
        # Use configured grace period, ensure servicer_instance exists for config access
        grace_period = servicer_instance.config.GRPC_SERVER_SHUTDOWN_GRACE_S if 'servicer_instance' in locals() and servicer_instance else 5
        await server.stop(grace=grace_period)
        logger.info("SWM server stopped.")

if __name__=='__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt: logger.info("SWM main process interrupted by user.")
    except SystemExit as e_sysexit: logger.info(f"SWM main process exited: {e_sysexit}")
    except Exception as e_main: logger.critical(f"SWM main unhandled exception: {e_main}",exc_info=True)

```
