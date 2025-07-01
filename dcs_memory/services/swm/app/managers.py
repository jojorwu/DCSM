# Placeholder for SWM Managers
import asyncio
import logging
import time
import uuid
from typing import Dict, Set, List, Optional, AsyncGenerator

from .config import SWMConfig
from generated_grpc import kem_pb2, swm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp

# Assuming SubscriberInfo dataclass might be defined here or imported if it's common
# For now, let's assume it will be part of SubscriptionManager or defined locally for it.

logger = logging.getLogger(__name__)

@dataclass
class SubscriberInfoInternal: # Renamed to avoid conflict if SubscriberInfo is also a proto message
    event_queue: asyncio.Queue
    original_topics: List[swm_service_pb2.SubscriptionTopic] # Store original topics for removal logic
    # Parsed filters might be more complex and stored internally by the manager
    # For now, we can keep it simple or let the manager handle parsing internally.
    # parsed_filters: Dict[str, Set[str]]
    subscribes_to_all_kem_lifecycle: bool = False
    subscriber_id: str # Added for easier reference


class SubscriptionManager:
    def __init__(self, config: SWMConfig):
        self.config = config
        self.subscribers_lock = asyncio.Lock()
        self.subscribers: Dict[str, SubscriberInfoInternal] = {}

        # Modified index structures to support event type filtering per filter criteria.
        # For a specific filter (e.g., kem_id="X" or metadata.key="Y"), store which event types the subscriber wants.
        # If desired_event_types is empty for a filter, it means all event types for that filter.
        # Map: filter_key (e.g. "kem_id:X") -> subscriber_id -> Set[EventType] (empty set means all types)
        self.specific_filters_to_subscribers: Dict[str, Dict[str, Set[swm_service_pb2.SWMMemoryEvent.EventType]]] = {}

        # For subscribers to ALL KEM_LIFECYCLE_EVENTS (no specific filter_criteria or empty filter_criteria)
        # Map: subscriber_id -> Set[EventType] (empty set means all types for "all KEMs")
        self.general_kem_lifecycle_subscribers: Dict[str, Set[swm_service_pb2.SWMMemoryEvent.EventType]] = {}
        logger.info("SubscriptionManager initialized with new index structures for event type filtering.")

    def _parse_filter_key(self, key_from_criteria: str, value_from_criteria: str) -> Optional[str]:
        """Helper to create a unique key for specific_filters_to_subscribers map."""
        if key_from_criteria == "kem_id":
            return f"kem_id:{value_from_criteria}"
        elif key_from_criteria.startswith("metadata."):
            actual_meta_key = key_from_criteria.split("metadata.", 1)[1]
            if actual_meta_key:
                return f"metadata:{actual_meta_key}:{value_from_criteria}"
        return None

    async def add_subscriber(self, subscriber_id: str, topics: List[swm_service_pb2.SubscriptionTopic], requested_q_size: int) -> asyncio.Queue:
        actual_q_size = max(self.config.SUBSCRIBER_MIN_QUEUE_SIZE,
                            min(requested_q_size if requested_q_size > 0 else self.config.SUBSCRIBER_DEFAULT_QUEUE_SIZE,
                                self.config.SUBSCRIBER_MAX_QUEUE_SIZE))
        event_queue = asyncio.Queue(maxsize=actual_q_size)

        parsed_filters: Dict[str, Set[str]] = {}
        sub_all_kem = False # Default: not subscribed to all KEM events

        has_at_least_one_kem_topic_with_filter = False

        if not topics: # No topics provided at all
            # If the default behavior for an empty topic list for KEM_LIFECYCLE_EVENTS
            # (if such a concept exists implicitly) means "subscribe to all", this needs to be handled.
            # For now, no topics means no KEM subscriptions.
            pass
        else:
            for topic in topics:
                if topic.type == swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS:
                    if not topic.filter_criteria or topic.filter_criteria.strip() == "":
                        # Explicit subscription to all KEM events via empty filter on KEM_LIFECYCLE_EVENTS topic
                        sub_all_kem = True
                        parsed_filters.clear() # No specific filters needed if subscribed to all
                        break # Found "subscribe to all" condition

                    # Parse filter_criteria (simple key=value for now)
                    if '=' in topic.filter_criteria:
                        key, value = topic.filter_criteria.split("=", 1)
                        key = key.strip()
                        value = value.strip()
                        if key and value: # Ensure key and value are not empty after strip
                            parsed_filters.setdefault(key, set()).add(value)
                            if key == "kem_id" or (key.startswith("metadata.") and key.split("metadata.",1)[1]):
                                has_at_least_one_kem_topic_with_filter = True
                        else:
                            logger.warning(f"SubscriptionManager: Malformed filter_criteria '{topic.filter_criteria}' for subscriber '{subscriber_id}'. Skipped.")
                    else:
                        logger.warning(f"SubscriptionManager: Unparseable filter_criteria '{topic.filter_criteria}' (no '=') for subscriber '{subscriber_id}'. Skipped.")

            if not sub_all_kem and not has_at_least_one_kem_topic_with_filter and \
               any(t.type == swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS for t in topics):
                # This case means there were KEM_LIFECYCLE_EVENTS topics, but none had valid specific filters,
                # and none were an explicit "subscribe to all" (empty filter).
                # Defaulting to "subscribe to all" in this ambiguous case might be too broad.
                # Let's make it so that if there are KEM topics but no valid filters, it's NOT a sub_all_kem.
                # It means they might have provided malformed filters.
                # If the intent was to subscribe to all, filter_criteria should be empty.
                # Thus, sub_all_kem remains False unless explicitly set by an empty filter_criteria.
                pass


        sub_info = SubscriberInfoInternal(
            event_queue=event_queue,
            original_topics=list(topics), # Store a copy
            subscribes_to_all_kem_lifecycle=sub_all_kem,
            subscriber_id=subscriber_id
        )

        async with self.subscribers_lock:
            if subscriber_id in self.subscribers:
                logger.warning(f"SubscriptionManager: Subscriber ID '{subscriber_id}' already exists. Removing old index entries before re-registering.")
                self._remove_subscriber_from_indexes_internal(subscriber_id, self.subscribers[subscriber_id].original_topics) # Use original_topics from stored sub_info

            self.subscribers[subscriber_id] = sub_info

            if sub_info.subscribes_to_all_kem_lifecycle:
                self.general_kem_event_subscribers.add(subscriber_id)
                logger.debug(f"SubscriptionManager: Subscriber '{subscriber_id}' added to general KEM event subscribers.")
            elif parsed_filters: # Only add to specific indexes if not sub_all_kem and there are parsed_filters
                for key, values in parsed_filters.items():
                    if key == "kem_id":
                        for value in values:
                            self.kem_id_to_subscribers.setdefault(value, set()).add(subscriber_id)
                            logger.debug(f"SubscriptionManager: Subscriber '{subscriber_id}' indexed for kem_id='{value}'.")
                    elif key.startswith("metadata."):
                        actual_meta_key = key.split("metadata.", 1)[1]
                        if actual_meta_key: # Ensure there's a key after "metadata."
                            meta_key_index = self.metadata_exact_match_to_subscribers.setdefault(actual_meta_key, {})
                            for value in values:
                                meta_key_index.setdefault(value, set()).add(subscriber_id)
                                logger.debug(f"SubscriptionManager: Subscriber '{subscriber_id}' indexed for metadata.{actual_meta_key}='{value}'.")
            # If not sub_all_kem and no valid parsed_filters for KEMs, they won't receive KEM events.
            logger.info(f"SubscriptionManager: Subscriber '{subscriber_id}' added/updated. Total subscribers: {len(self.subscribers)}. General KEM subs: {len(self.general_kem_event_subscribers)}.")
        return event_queue

    async def remove_subscriber(self, subscriber_id: str):
        async with self.subscribers_lock:
            removed_info = self.subscribers.pop(subscriber_id, None)
            if removed_info:
                logger.info(f"SubscriptionManager: Subscriber '{subscriber_id}' removed from list. Remaining: {len(self.subscribers)}")
                self._remove_subscriber_from_indexes_internal(subscriber_id, removed_info.original_topics) # Use original_topics from the removed info
            else:
                logger.warning(f"SubscriptionManager: Attempted to remove non-existent subscriber '{subscriber_id}'.")

    def _remove_subscriber_from_indexes_internal(self, subscriber_id: str, original_topics: List[swm_service_pb2.SubscriptionTopic]):
        # This method must be called under self.subscribers_lock by the caller
        self.general_kem_event_subscribers.discard(subscriber_id)

        # Re-parse filters from original_topics to ensure correct removal from indexes
        # This avoids relying on potentially stale parsed_filters in a SubscriberInfo if it was modified elsewhere (though it shouldn't be)
        filters_to_remove: Dict[str, Set[str]] = {}
        for topic in original_topics:
            if topic.type == swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS and \
               topic.filter_criteria and '=' in topic.filter_criteria:
                key, value = topic.filter_criteria.split("=", 1)
                key = key.strip(); value = value.strip()
                filters_to_remove.setdefault(key, set()).add(value)

        for kem_id_val in filters_to_remove.get("kem_id", set()):
            if kem_id_val in self.kem_id_to_subscribers:
                self.kem_id_to_subscribers[kem_id_val].discard(subscriber_id)
                if not self.kem_id_to_subscribers[kem_id_val]:
                    del self.kem_id_to_subscribers[kem_id_val]

        for meta_filter_key, meta_filter_values in filters_to_remove.items():
            if meta_filter_key.startswith("metadata."):
                actual_meta_key = meta_filter_key.split("metadata.",1)[1]
                if actual_meta_key and actual_meta_key in self.metadata_exact_match_to_subscribers:
                    value_map = self.metadata_exact_match_to_subscribers[actual_meta_key]
                    for meta_value in meta_filter_values:
                        if meta_value in value_map:
                            value_map[meta_value].discard(subscriber_id)
                            if not value_map[meta_value]:
                                del value_map[meta_value]
                    if not value_map: # If the map for this actual_meta_key is now empty
                        del self.metadata_exact_match_to_subscribers[actual_meta_key]
        logger.debug(f"SubscriptionManager: Subscriber '{subscriber_id}' removed from specific filter indexes.")

    async def notify_kem_event(self, kem: kem_pb2.KEM, event_type: swm_service_pb2.SWMMemoryEvent.EventType, source_agent_id: str = "SWM_SERVER"):
        if not kem or not kem.id:
            logger.warning("SubscriptionManager: notify_kem_event called with invalid KEM.")
            return

        event_to_dispatch = swm_service_pb2.SWMMemoryEvent(
            event_id=str(uuid.uuid4()), event_type=event_type, kem_payload=kem,
            event_time=Timestamp(seconds=int(time.time())), source_agent_id=source_agent_id,
            details=f"Event type {event_type} for KEM ID {kem.id}"
        )

        subscribers_to_notify_map: Dict[str, SubscriberInfoInternal] = {} # Use dict to avoid duplicate notifications if sub matches multiple ways

        async with self.subscribers_lock: # Lock for reading subscriber indexes and list
            # 1. General subscribers
            for sub_id in self.general_kem_event_subscribers:
                if sub_id in self.subscribers and sub_id not in subscribers_to_notify_map:
                     subscribers_to_notify_map[sub_id] = self.subscribers[sub_id]

            # 2. Subscribers by KEM ID
            if kem.id in self.kem_id_to_subscribers:
                for sub_id in self.kem_id_to_subscribers[kem.id]:
                    if sub_id in self.subscribers and sub_id not in subscribers_to_notify_map:
                         subscribers_to_notify_map[sub_id] = self.subscribers[sub_id]

            # 3. Subscribers by metadata
            if kem.metadata:
                for meta_key, meta_value_kem in kem.metadata.items():
                    if meta_key in self.metadata_exact_match_to_subscribers:
                        value_map = self.metadata_exact_match_to_subscribers[meta_key]
                        if meta_value_kem in value_map:
                            for sub_id in value_map[meta_value_kem]:
                                if sub_id in self.subscribers and sub_id not in subscribers_to_notify_map:
                                     subscribers_to_notify_map[sub_id] = self.subscribers[sub_id]

        if not subscribers_to_notify_map:
            logger.debug(f"SubscriptionManager: No subscribers for KEM event on '{kem.id}' (type: {event_type}).")
            return

        logger.debug(f"SubscriptionManager: Dispatching event for KEM '{kem.id}' (type: {event_type}) to {len(subscribers_to_notify_map)} subscribers.")
        for sub_id, sub_info_item in subscribers_to_notify_map.items():
            try:
                if sub_info_item.event_queue.full():
                    logger.warning(f"Subscriber queue full for subscriber_id='{sub_id}', event_id='{event_to_dispatch.event_id}' (KEM_ID='{kem.id}') lost. Queue size: {sub_info_item.event_queue.qsize()}/{sub_info_item.event_queue.maxsize}")
                else:
                    await sub_info_item.event_queue.put(event_to_dispatch)
            except Exception as e: # Catch broad exceptions during queue put
                logger.error(f"SubscriptionManager: Error queueing event for subscriber_id='{sub_id}', event_id='{event_to_dispatch.event_id}': {e}", exc_info=True)

@dataclass
class LockInfoInternalSWM: # Renamed to avoid conflict with GLM's potential LockInfo
    resource_id: str
    agent_id: str
    lock_id: str
    acquired_at_unix_ms: int
    lease_duration_ms: int
    lease_expires_at_unix_ms: int

class DistributedLockManager:
    def __init__(self, config: SWMConfig):
        self.config = config
        self.locks: Dict[str, LockInfoInternalSWM] = {}
        self.lock_condition = asyncio.Condition() # Lock for self.locks is implicitly managed by Condition's lock
        self._stop_event = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None
        logger.info("DistributedLockManager initialized.")

    async def start_cleanup_task(self):
        if self._cleanup_task is None or self._cleanup_task.done():
            self._stop_event.clear()
            self._cleanup_task = asyncio.create_task(self._expired_lock_cleanup_loop())
            logger.info("DistributedLockManager: Expired lock cleanup task started.")

    async def stop_cleanup_task(self):
        if self._cleanup_task and not self._cleanup_task.done():
            logger.info("DistributedLockManager: Stopping expired lock cleanup task...")
            self._stop_event.set()
            async with self.lock_condition: # Acquire lock before notifying
                self.lock_condition.notify_all() # Wake up loop if it's sleeping on condition
            try:
                # Use configured shutdown grace period for the cleanup task
                await asyncio.wait_for(self._cleanup_task, timeout=self.config.LOCK_CLEANUP_INTERVAL_S + self.config.LOCK_CLEANUP_SHUTDOWN_GRACE_S)
            except asyncio.TimeoutError:
                logger.warning("DistributedLockManager: Expired lock cleanup task did not finish in time during shutdown.")
                # self._cleanup_task.cancel() # Optionally cancel if it didn't stop
            except asyncio.CancelledError:
                 logger.info("DistributedLockManager: Expired lock cleanup task cancelled during shutdown.")
        self._cleanup_task = None

    async def _expired_lock_cleanup_loop(self):
        # Logic from _expired_lock_cleanup_task_async
        logger.info("DistributedLockManager: Async expired lock cleanup loop started.")
        while not self._stop_event.is_set():
            try:
                # Wait for the shorter of stop_event or cleanup_interval
                # Use lock_condition.wait() with timeout to handle notifications properly
                async with self.lock_condition:
                    try:
                        await asyncio.wait_for(
                            self.lock_condition.wait_for(lambda: self._stop_event.is_set()),
                            timeout=self.config.LOCK_CLEANUP_INTERVAL_S
                        )
                        if self._stop_event.is_set(): break # Exit if stop was signaled by notification
                    except asyncio.TimeoutError:
                        # Timeout means it's time to cleanup (or stop_event was not set within timeout)
                        pass # Proceed to cleanup logic below

                if self._stop_event.is_set(): break # Check again after potential timeout

                logger.debug("DistributedLockManager: Performing periodic cleanup of expired locks...")
                async with self.lock_condition: # Acquire lock for modification
                    now_ms = int(time.time() * 1000)
                    expired_ids = [
                        rid for rid, linfo in self.locks.items()
                        if linfo.lease_duration_ms > 0 and now_ms >= linfo.lease_expires_at_unix_ms
                    ]
                    if expired_ids:
                        cleaned_count = 0
                        for resource_id_expired in expired_ids: # Use different variable name
                            lock_to_check = self.locks.get(resource_id_expired) # Check current state
                            if lock_to_check and lock_to_check.lease_duration_ms > 0 and now_ms >= lock_to_check.lease_expires_at_unix_ms:
                                del self.locks[resource_id_expired]
                                logger.info(f"DistributedLockManager Cleanup: Expired lock for '{lock_to_check.resource_id}' (agent '{lock_to_check.agent_id}') removed.")
                                self.lock_condition.notify_all()
                                cleaned_count +=1
                        if cleaned_count > 0:
                             logger.info(f"DistributedLockManager Cleanup: Removed {cleaned_count} expired locks.")
                    else:
                        logger.debug("DistributedLockManager Cleanup: No expired locks found.")
            except asyncio.CancelledError:
                logger.info("DistributedLockManager: Lock cleanup loop cancelled.")
                break
            except Exception as e:
                logger.error(f"DistributedLockManager: Error in lock cleanup loop: {e}", exc_info=True)
                if not self._stop_event.is_set():
                    try: # Defensive sleep to avoid tight loop on unexpected errors
                        await asyncio.sleep(self.config.LOCK_CLEANUP_INTERVAL_S)
                    except asyncio.CancelledError: break
        logger.info("DistributedLockManager: Async expired lock cleanup loop stopped.")

    async def acquire_lock(self, resource_id: str, agent_id: str, timeout_ms: int, lease_duration_ms: int) -> swm_service_pb2.AcquireLockResponse:
        logger.info(f"LockManager: AcquireLock '{resource_id}' by '{agent_id}', timeout={timeout_ms}ms, lease={lease_duration_ms}ms")
        start_mono_time = time.monotonic()

        async with self.lock_condition:
            while True: # Loop to re-check after being notified or timed out
                now_unix_ms = int(time.time() * 1000)
                current_lock = self.locks.get(resource_id)

                # Check for and remove expired lock
                if current_lock and current_lock.lease_duration_ms > 0 and now_unix_ms >= current_lock.lease_expires_at_unix_ms:
                    logger.info(f"LockManager: Lock for '{resource_id}' held by '{current_lock.agent_id}' expired. Removing.")
                    del self.locks[resource_id]
                    current_lock = None
                    self.lock_condition.notify_all() # Notify others that a lock might be free

                if not current_lock: # Lock is available
                    new_lock_id = str(uuid.uuid4())
                    acquired_at = now_unix_ms
                    expires_at = acquired_at + lease_duration_ms if lease_duration_ms > 0 else 0

                    self.locks[resource_id] = LockInfoInternalSWM(
                        resource_id, agent_id, new_lock_id, acquired_at, lease_duration_ms, expires_at
                    )
                    logger.info(f"LockManager: Lock on '{resource_id}' acquired by '{agent_id}'. ID: {new_lock_id}. Expires: {expires_at if expires_at > 0 else 'never'}")
                    return swm_service_pb2.AcquireLockResponse(
                        resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ACQUIRED,
                        lock_id=new_lock_id, acquired_at_unix_ms=acquired_at, lease_expires_at_unix_ms=expires_at,
                        message="Lock acquired successfully."
                    )

                if current_lock.agent_id == agent_id: # Lock already held by the same agent
                    logger.info(f"LockManager: Lock on '{resource_id}' already held by requesting agent '{agent_id}'. Renewing lease if applicable.")
                    if lease_duration_ms > 0: # Renew or set lease
                        current_lock.lease_expires_at_unix_ms = now_unix_ms + lease_duration_ms
                        current_lock.lease_duration_ms = lease_duration_ms
                        logger.info(f"LockManager: Lease for '{resource_id}' (agent '{agent_id}') updated. New expiry: {current_lock.lease_expires_at_unix_ms}")
                    elif lease_duration_ms == 0 and current_lock.lease_duration_ms > 0 : # Change to indefinite lease
                        current_lock.lease_expires_at_unix_ms = 0
                        current_lock.lease_duration_ms = 0
                        logger.info(f"LockManager: Lease for '{resource_id}' (agent '{agent_id}') changed to indefinite.")
                    return swm_service_pb2.AcquireLockResponse(
                        resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ALREADY_HELD_BY_YOU,
                        lock_id=current_lock.lock_id, acquired_at_unix_ms=current_lock.acquired_at_unix_ms,
                        lease_expires_at_unix_ms=current_lock.lease_expires_at_unix_ms, message="Lock already held by you; lease updated if applicable."
                    )

                # Lock is held by another agent, and we need to wait
                if timeout_ms == 0: # Don't wait
                    logger.info(f"LockManager: Lock on '{resource_id}' held by '{current_lock.agent_id}'. Agent '{agent_id}' chose not to wait.")
                    return swm_service_pb2.AcquireLockResponse(resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.NOT_AVAILABLE, message=f"Lock currently held by agent '{current_lock.agent_id}'.")

                elapsed_ms = (time.monotonic() - start_mono_time) * 1000

                wait_timeout_s: Optional[float] = None
                if timeout_ms > 0: # Finite timeout
                    remaining_wait_ms = timeout_ms - elapsed_ms
                    if remaining_wait_ms <= 0:
                        logger.info(f"LockManager: Timeout ({timeout_ms}ms) expired for agent '{agent_id}' waiting for lock on '{resource_id}'.")
                        return swm_service_pb2.AcquireLockResponse(resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.TIMEOUT, message="Timeout expired while waiting for lock.")
                    wait_timeout_s = remaining_wait_ms / 1000.0

                # If timeout_ms is < 0, wait_timeout_s will be None (wait indefinitely)
                logger.debug(f"LockManager: Agent '{agent_id}' waiting for lock on '{resource_id}'. Wait timeout: {wait_timeout_s}s.")
                try:
                    await asyncio.wait_for(self.lock_condition.wait(), timeout=wait_timeout_s)
                except asyncio.TimeoutError: # This timeout is from wait_for
                    logger.info(f"LockManager: Agent '{agent_id}' timed out waiting for notification on lock '{resource_id}'. Re-checking lock status.")
                    # Loop will re-check conditions, including overall timeout_ms
                logger.debug(f"LockManager: Agent '{agent_id}' awakened for lock on '{resource_id}'. Re-checking status.")
                # Loop continues to re-evaluate lock state

    async def release_lock(self, resource_id: str, agent_id: str, lock_id_to_release: Optional[str]) -> swm_service_pb2.ReleaseLockResponse:
        logger.info(f"LockManager: ReleaseLock '{resource_id}' by '{agent_id}', requested lock_id='{lock_id_to_release}'.")
        async with self.lock_condition:
            current_lock = self.locks.get(resource_id)

            if not current_lock:
                logger.warning(f"LockManager: Attempt to release lock on unheld resource '{resource_id}'.")
                return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.NOT_HELD, message="Resource not locked or lock expired.")

            now_unix_ms = int(time.time() * 1000)
            if current_lock.lease_duration_ms > 0 and now_unix_ms >= current_lock.lease_expires_at_unix_ms:
                logger.info(f"LockManager: Attempt to release an already expired lock for '{resource_id}' by '{current_lock.agent_id}'. Removing it.")
                del self.locks[resource_id]
                self.lock_condition.notify_all()
                return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.NOT_HELD, message="Lock had already expired.")

            if current_lock.agent_id != agent_id:
                logger.warning(f"LockManager: Agent '{agent_id}' attempted to release lock on '{resource_id}' held by another agent '{current_lock.agent_id}'. Denied.")
                return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING, message="Lock held by another agent.")

            if lock_id_to_release and current_lock.lock_id != lock_id_to_release:
                logger.warning(f"LockManager: Invalid lock_id ('{lock_id_to_release}') provided for releasing lock on '{resource_id}'. Expected '{current_lock.lock_id}'. Denied.")
                return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.INVALID_LOCK_ID, message="Invalid lock ID provided.")

            del self.locks[resource_id]
            logger.info(f"LockManager: Lock for '{resource_id}' released by agent '{agent_id}'. Notifying waiters.")
            self.lock_condition.notify_all()
            return swm_service_pb2.ReleaseLockResponse(resource_id=resource_id, status=swm_service_pb2.ReleaseStatusValue.RELEASED, message="Lock successfully released.")

    async def get_lock_info(self, resource_id: str) -> swm_service_pb2.LockInfo:
        logger.debug(f"LockManager: GetLockInfo for '{resource_id}'.")
        async with self.lock_condition: # Ensure consistent read and potential cleanup
            now_unix_ms = int(time.time() * 1000)
            current_lock = self.locks.get(resource_id)

            if current_lock and current_lock.lease_duration_ms > 0 and now_unix_ms >= current_lock.lease_expires_at_unix_ms:
                logger.info(f"LockManager (GetLockInfo): Lock for '{resource_id}' by '{current_lock.agent_id}' found expired. Removing.")
                del self.locks[resource_id]
                current_lock = None
                self.lock_condition.notify_all() # Notify if a lock was just cleared

            if current_lock:
                return swm_service_pb2.LockInfo(
                    resource_id=resource_id, is_locked=True,
                    current_holder_agent_id=current_lock.agent_id, lock_id=current_lock.lock_id,
                    acquired_at_unix_ms=current_lock.acquired_at_unix_ms,
                    lease_expires_at_unix_ms=current_lock.lease_expires_at_unix_ms if current_lock.lease_duration_ms > 0 else 0
                )
            else:
                return swm_service_pb2.LockInfo(resource_id=resource_id, is_locked=False)


class DistributedCounterManager:
    def __init__(self, config: SWMConfig):
        self.config = config
        self.counters: Dict[str, int] = {}
        self.counters_lock = asyncio.Lock()
        logger.info("DistributedCounterManager initialized.")

    async def increment_counter(self, counter_id: str, increment_by: int) -> swm_service_pb2.CounterValueResponse:
        logger.info(f"CounterManager: IncrementCounter '{counter_id}', by {increment_by}")
        if not counter_id:
            # This should ideally raise an error that the gRPC servicer can catch and convert to INVALID_ARGUMENT
            # For now, let's log and return an error-like response, assuming servicer handles it.
            logger.error("CounterManager: increment_counter called with empty counter_id.")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message="Error: counter_id cannot be empty.")

        async with self.counters_lock:
            current_value = self.counters.get(counter_id, 0)
            new_value = current_value + increment_by
            self.counters[counter_id] = new_value
            logger.info(f"CounterManager: Counter '{counter_id}' updated. Old: {current_value}, New: {new_value}")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=new_value, status_message="Counter updated successfully.")

    async def get_counter(self, counter_id: str) -> swm_service_pb2.CounterValueResponse:
        logger.info(f"CounterManager: GetCounter for '{counter_id}'")
        if not counter_id:
            logger.error("CounterManager: get_counter called with empty counter_id.")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message="Error: counter_id cannot be empty.")

        async with self.counters_lock:
            current_value = self.counters.get(counter_id) # Returns None if not found

        if current_value is None:
            logger.warning(f"CounterManager: Counter '{counter_id}' not found.")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message=f"Counter '{counter_id}' not found, returned default 0.")
        else:
            logger.info(f"CounterManager: Counter '{counter_id}' current value: {current_value}")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=current_value, status_message="Counter value retrieved.")

logger.info("SWM Managers module loaded.")
