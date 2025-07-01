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

# Lua script for safe lock release
RELEASE_LOCK_LUA_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

RENEW_LOCK_LUA_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  return 1
else
  return 0
end
"""

class DistributedLockManager:
    def __init__(self, config: SWMConfig, redis_client: any): # 'any' for aioredis.Redis type hint if not globally available
        self.config = config
        self.redis = redis_client # Store the aioredis client
        self._stop_event = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None

        self._release_script_sha: Optional[str] = None
        self._renew_script_sha: Optional[str] = None
        asyncio.create_task(self._load_lua_scripts())

        logger.info("DistributedLockManager initialized to use Redis.")

    async def _load_lua_scripts(self):
        try:
            if self.redis:
                self._release_script_sha = await self.redis.script_load(RELEASE_LOCK_LUA_SCRIPT)
                logger.info(f"DistributedLockManager: Loaded Lua script for lock release (SHA: {self._release_script_sha}).")
                self._renew_script_sha = await self.redis.script_load(RENEW_LOCK_LUA_SCRIPT)
                logger.info(f"DistributedLockManager: Loaded Lua script for lock renewal (SHA: {self._renew_script_sha}).")
        except Exception as e:
            logger.error(f"DistributedLockManager: Failed to load Lua scripts into Redis: {e}", exc_info=True)

    async def start_cleanup_task(self):
        # With Redis TTLs, an explicit cleanup loop for expired locks is less critical.
        # Redis handles expiration automatically. This loop could be for orphaned metadata if any.
        # For now, let's disable it as Redis TTLs are the primary mechanism.
        logger.info("DistributedLockManager: Expired lock cleanup task is NOT started (relying on Redis TTLs).")
        pass

    async def stop_cleanup_task(self):
        logger.info("DistributedLockManager: Stopping expired lock cleanup task (if it were running).")
        pass

    async def _expired_lock_cleanup_loop(self):
        # This loop is largely redundant if Redis TTLs are used for locks.
        # It might be useful for cleaning up auxiliary data structures if those were used,
        # but for simple SET NX PX locks, Redis handles expiry.
        logger.info("DistributedLockManager: In-memory expired lock cleanup loop is currently disabled for Redis implementation.")
        pass


    async def acquire_lock(self, resource_id: str, agent_id: str, timeout_ms: int, lease_duration_ms: int) -> swm_service_pb2.AcquireLockResponse:
        if not self.redis:
            logger.error("DistributedLockManager: Redis client not available for acquire_lock.")
            return swm_service_pb2.AcquireLockResponse(status=swm_service_pb2.LockStatusValue.ERROR_ACQUIRING, message="Redis unavailable.")

        lock_key = f"{self.config.REDIS_LOCK_KEY_PREFIX}{resource_id}"
        lock_value = f"{agent_id}:{uuid.uuid4().hex}" # Include agent_id for info, UUID for uniqueness

        # Ensure lease_duration_ms is positive if specified, otherwise use a default.
        # Redis PX expects positive integer. If lease_duration_ms is 0 (indefinite), we can't use PX directly.
        # For indefinite, one might omit PX and handle it, or use a very long PX.
        # Let's assume lease_duration_ms > 0 for simplicity with PX.
        # If lease_duration_ms is 0 or negative, use a default lease from config.
        effective_lease_ms = lease_duration_ms
        if effective_lease_ms <= 0:
            effective_lease_ms = getattr(self.config, "REDIS_LOCK_DEFAULT_LEASE_MS", 30000) # Default 30s if not positive
            logger.debug(f"AcquireLock: Using default lease {effective_lease_ms}ms for resource '{resource_id}' as requested lease was {lease_duration_ms}ms.")

        start_time = time.monotonic()
        acquired_at_unix_ms = 0

        while True:
            now_unix_ms = int(time.time() * 1000)
            # Try to acquire the lock
            # SET resource_id lock_value NX PX lease_duration_ms
            lock_acquired = await self.redis.set(lock_key, lock_value, nx=True, px=effective_lease_ms)

            if lock_acquired:
                acquired_at_unix_ms = now_unix_ms
                expires_at = acquired_at_unix_ms + effective_lease_ms
                logger.info(f"LockManager: Lock on '{resource_id}' acquired by '{agent_id}'. Value: {lock_value}. Expires: {expires_at}")
                return swm_service_pb2.AcquireLockResponse(
                    resource_id=resource_id, agent_id=agent_id, status=swm_service_pb2.LockStatusValue.ACQUIRED,
                    lock_id=lock_value, # Return the unique lock value as lock_id
                    acquired_at_unix_ms=acquired_at_unix_ms,
                    lease_expires_at_unix_ms=expires_at,
                    message="Lock acquired successfully."
                )
            else: # Lock not acquired, check if it's held by us (for renewal) or someone else
                existing_lock_value_bytes = await self.redis.get(lock_key)
                if existing_lock_value_bytes:
                    existing_lock_value = existing_lock_value_bytes.decode('utf-8', errors='ignore')
                    # Check if the current agent holds the lock by comparing the full lock_value (which is the lock_id)
                    # This requires the agent to pass its current lock_id if it's trying to renew.
                    # The acquire_lock RPC doesn't have a field for "current_lock_id_if_renewing".
                    # So, we can only reliably check if the agent_id part matches.
                    # The Lua script for renewal (if we were to use it here) would need the exact current lock_value.
                    #
                    # Current logic: If `SET NX` fails, it means lock exists.
                    # If an agent calls `acquire_lock` again for a lock it might hold,
                    # the `lock_value` generated in this call will be new.
                    # So, simply trying `SET NX PX` again is the correct path for a "fresh" acquire.
                    # The "renewal" part is if the agent *knew* it held the lock and wanted to extend it.
                    # The current API `acquire_lock` doesn't distinguish well between "acquire new" and "renew existing for me".
                    #
                    # Let's assume the current interpretation: if SET NX PX fails, and the lock *is* held by the current agent_id
                    # (by parsing the agent_id from the stored lock_value), then it's a renewal scenario.
                    # The `lock_value` generated for THIS call is `agent_id:new_uuid`.
                    # The `existing_lock_value` is `agent_id:old_uuid`.
                    #
                    # To use the Lua renewal script, the agent would need to provide its *current* `lock_id` (which is `existing_lock_value`).
                    # Since `acquire_lock` doesn't take `current_lock_id`, we can't use the Lua renewal script directly here
                    # without changing the API or how `lock_value` is constructed/used.
                    #
                    # The previous optimistic renewal `await self.redis.set(lock_key, existing_lock_value, xx=True, px=effective_lease_ms)`
                    # was attempting to renew the *existing_lock_value* if it was found.
                    # This is more like a "refresh lease if I still own it".
                    #
                    # Let's stick to the behavior that `acquire_lock` always tries to get a *new* lock or fail/timeout.
                    # If an agent wants to renew, it should perhaps use a different method or `acquire_lock`
                    # could be made smarter if it gets the *same* agent_id trying to acquire again.
                    #
                    # For now, if `lock_acquired` is false, it means it's held by someone (could be self with old UUID, or other).
                    # The polling loop will just keep trying `SET NX PX` with the *new* `lock_value`.
                    # This means an agent calling `acquire_lock` again for a resource it holds will effectively
                    # try to acquire it "fresh" if its old lease expires. This is simpler and avoids complex renewal logic here.
                    # The `ALREADY_HELD_BY_YOU` status would only be returned if the *exact same* `lock_value` somehow was attempted to be set again,
                    # which is unlikely with UUIDs.
                    #
                    # The Lua script for renewal is better suited for an explicit `renew_lock(resource_id, current_lock_id, new_lease_ms)` API.
                    # Given the current API, the polling loop for `SET NX PX` is the path.
                    # The `ALREADY_HELD_BY_YOU` part of the original in-memory logic is harder to map directly
                    # to `SET NX PX` without an explicit renewal call.
                    #
                    # If `SET NX PX` fails, the lock is held. We just wait and retry.
                    pass # Fall through to polling logic

                # If timeout is 0, don't wait/retry
                if timeout_ms == 0:
                    return swm_service_pb2.AcquireLockResponse(status=swm_service_pb2.LockStatusValue.NOT_AVAILABLE, message="Lock not immediately available.")

                elapsed_ms = (time.monotonic() - start_time) * 1000
                if timeout_ms > 0 and elapsed_ms >= timeout_ms:
                    return swm_service_pb2.AcquireLockResponse(status=swm_service_pb2.LockStatusValue.TIMEOUT, message="Timeout expired waiting for lock.")

                # Wait before retrying
                poll_interval_s = getattr(self.config, "REDIS_LOCK_ACQUIRE_POLL_INTERVAL_S", 0.1) # Default 100ms
                await asyncio.sleep(min(poll_interval_s, (timeout_ms - elapsed_ms) / 1000.0) if timeout_ms > 0 else poll_interval_s)


    async def release_lock(self, resource_id: str, agent_id: str, lock_id_to_release: Optional[str]) -> swm_service_pb2.ReleaseLockResponse:
        if not self.redis:
            logger.error("DistributedLockManager: Redis client not available for release_lock.")
            return swm_service_pb2.ReleaseLockResponse(status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING, message="Redis unavailable.")
        if not lock_id_to_release: # lock_id (which is the unique lock value) is required for safe release
            logger.warning(f"LockManager: Attempt to release lock on '{resource_id}' by '{agent_id}' without a lock_id.")
            return swm_service_pb2.ReleaseLockResponse(status=swm_service_pb2.ReleaseStatusValue.INVALID_LOCK_ID, message="Lock ID is required for release.")

        lock_key = f"{self.config.REDIS_LOCK_KEY_PREFIX}{resource_id}"

        # Expected lock_id_to_release should contain agent_id prefix if generated by this manager
        # This check is mostly for local consistency before calling Redis.
        # The Lua script does the authoritative check against the value in Redis.
        if not lock_id_to_release.startswith(f"{agent_id}:"):
            logger.warning(f"LockManager: Agent '{agent_id}' attempting to release lock '{resource_id}' with lock_id '{lock_id_to_release}' that doesn't match its expected format.")
            # Proceed to Redis to let Lua script make the final decision, but log this anomaly.
            # Or, could return an error here if strict format adherence is desired before hitting Redis.

        try:
            if self._release_script_sha:
                result = await self.redis.evalsha(self._release_script_sha, keys=[lock_key], args=[lock_id_to_release])
            else: # Fallback to EVAL if SHA not loaded (e.g. Redis restarted, script flush)
                logger.warning("DistributedLockManager: Release script SHA not available, using EVAL (less efficient).")
                result = await self.redis.eval(RELEASE_LOCK_LUA_SCRIPT, keys=[lock_key], args=[lock_id_to_release])

            if result == 1:
                logger.info(f"LockManager: Lock for '{resource_id}' released by agent '{agent_id}' (lock_id: {lock_id_to_release}).")
                return swm_service_pb2.ReleaseLockResponse(status=swm_service_pb2.ReleaseStatusValue.RELEASED, message="Lock released.")
            else: # result == 0
                # Check if the key still exists to differentiate between "not my lock" and "lock expired/gone"
                current_value_bytes = await self.redis.get(lock_key)
                if current_value_bytes:
                    current_value = current_value_bytes.decode('utf-8', errors='ignore')
                    logger.warning(f"LockManager: Failed to release lock for '{resource_id}'. Agent '{agent_id}', lock_id '{lock_id_to_release}'. Current lock value: '{current_value}'. Lock may be held by another agent or lock_id is stale.")
                    return swm_service_pb2.ReleaseLockResponse(status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING, message="Failed to release; lock may be held by another or ID is stale.")
                else:
                    logger.info(f"LockManager: Attempted to release lock for '{resource_id}' (lock_id: {lock_id_to_release}), but lock did not exist (possibly expired).")
                    return swm_service_pb2.ReleaseLockResponse(status=swm_service_pb2.ReleaseStatusValue.NOT_HELD, message="Lock not found or already expired.")
        except Exception as e:
            logger.error(f"DistributedLockManager: Error releasing lock for '{resource_id}': {e}", exc_info=True)
            return swm_service_pb2.ReleaseLockResponse(status=swm_service_pb2.ReleaseStatusValue.ERROR_RELEASING, message=f"Error during release: {e}")


    async def get_lock_info(self, resource_id: str) -> swm_service_pb2.LockInfo:
        if not self.redis:
            logger.error("DistributedLockManager: Redis client not available for get_lock_info.")
            # Return is_locked=False and an error message or handle as per API contract for errors
            return swm_service_pb2.LockInfo(resource_id=resource_id, is_locked=False, message="Redis unavailable.")

        lock_key = f"{self.config.REDIS_LOCK_KEY_PREFIX}{resource_id}"
        lock_value_bytes = await self.redis.get(lock_key)

        if lock_value_bytes:
            lock_value_str = lock_value_bytes.decode('utf-8', errors='ignore')
            parts = lock_value_str.split(":", 1)
            holder_agent_id = parts[0] if parts else "unknown"

            # Get TTL for remaining lease duration
            ttl_ms = await self.redis.pttl(lock_key)
            now_unix_ms = int(time.time() * 1000)
            acquired_at_ms = 0 # Cannot easily get original acquisition time from simple Redis lock
            expires_at_ms = now_unix_ms + ttl_ms if ttl_ms and ttl_ms > 0 else 0 # If no TTL or -1/-2, effectively no expiry from Redis perspective

            return swm_service_pb2.LockInfo(
                resource_id=resource_id, is_locked=True,
                current_holder_agent_id=holder_agent_id,
                lock_id=lock_value_str, # The unique value is the lock_id
                acquired_at_unix_ms=acquired_at_ms, # Original acquisition not stored directly
                lease_expires_at_unix_ms=expires_at_ms
            )
        else:
            return swm_service_pb2.LockInfo(resource_id=resource_id, is_locked=False)


class DistributedCounterManager:
    def __init__(self, config: SWMConfig, redis_client: any): # Added redis_client
        self.config = config
        self.redis = redis_client # Store the aioredis client
        # self.counters: Dict[str, int] = {} # Removed
        # self.counters_lock = asyncio.Lock() # Removed
        logger.info("DistributedCounterManager initialized to use Redis.")

    async def increment_counter(self, counter_id: str, increment_by: int) -> swm_service_pb2.CounterValueResponse:
        if not self.redis:
            logger.error("DistributedCounterManager: Redis client not available for increment_counter.")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message="Error: Redis unavailable.")
        if not counter_id:
            logger.error("CounterManager: increment_counter called with empty counter_id.")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message="Error: counter_id cannot be empty.")

        counter_key = f"{self.config.REDIS_COUNTER_KEY_PREFIX}{counter_id}"
        try:
            new_value = await self.redis.incrby(counter_key, increment_by)
            logger.info(f"CounterManager: Counter '{counter_key}' (ID: {counter_id}) incremented by {increment_by}. New value: {new_value}")
            return swm_service_pb2.CounterValueResponse(
                counter_id=counter_id,
                current_value=new_value,
                status_message="Counter updated successfully."
            )
        except Exception as e:
            logger.error(f"DistributedCounterManager: Error incrementing counter '{counter_key}': {e}", exc_info=True)
            return swm_service_pb2.CounterValueResponse(
                counter_id=counter_id,
                current_value=0, # Or attempt to get current value if error is transient
                status_message=f"Error incrementing counter: {e}"
            )

    async def get_counter(self, counter_id: str) -> swm_service_pb2.CounterValueResponse:
        if not self.redis:
            logger.error("DistributedCounterManager: Redis client not available for get_counter.")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message="Error: Redis unavailable.")
        if not counter_id:
            logger.error("CounterManager: get_counter called with empty counter_id.")
            return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message="Error: counter_id cannot be empty.")

        counter_key = f"{self.config.REDIS_COUNTER_KEY_PREFIX}{counter_id}"
        try:
            value_bytes = await self.redis.get(counter_key)
            if value_bytes is None:
                logger.info(f"CounterManager: Counter '{counter_key}' (ID: {counter_id}) not found. Returning 0.")
                return swm_service_pb2.CounterValueResponse(counter_id=counter_id, current_value=0, status_message="Counter not found, returned default 0.")

            current_value = int(value_bytes.decode('utf-8'))
            logger.info(f"CounterManager: Counter '{counter_key}' (ID: {counter_id}) current value: {current_value}")
            return swm_service_pb2.CounterValueResponse(
                counter_id=counter_id,
                current_value=current_value,
                status_message="Counter value retrieved."
            )
        except Exception as e:
            logger.error(f"DistributedCounterManager: Error getting counter '{counter_key}': {e}", exc_info=True)
            return swm_service_pb2.CounterValueResponse(
                counter_id=counter_id,
                current_value=0, # Or a specific error indicator
                status_message=f"Error getting counter: {e}"
            )

logger.info("SWM Managers module loaded.")
