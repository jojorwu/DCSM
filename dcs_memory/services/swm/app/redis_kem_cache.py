import asyncio
import logging
import json
from typing import List, Dict, Optional, Any, Tuple, Set
import uuid # For temporary key generation

try:
    import aioredis # type: ignore
    import msgpack # For serializing embeddings efficiently
except ImportError:
    aioredis = None # type: ignore
    msgpack = None # type: ignore
    logging.getLogger(__name__).warning("aioredis or msgpack library not found. RedisKemCache functionality will be limited or unavailable.")

from .config import SWMConfig
from generated_grpc import kem_pb2, glm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp

logger = logging.getLogger(__name__)

class RedisKemCache:
    """
    A KEM (Key-Event-Metadata) cache implementation using Redis as the backend.

    This cache stores KEMs as Redis Hashes, with individual fields for KEM attributes
    (ID, content_type, content, metadata JSON, created_at, updated_at, embeddings).
    It supports manual secondary indexing for specified metadata fields (stored in Redis Sets)
    and for created_at/updated_at timestamps (stored in Redis Sorted Sets).

    Key features:
    - CRUD operations for KEMs (set, get, delete, contains).
    - Transactional set and delete operations using Redis WATCH/MULTI/EXEC for atomicity
      when updating KEM data and its associated indexes.
    - Filter-based querying (`query_by_filters`) that leverages the secondary indexes
      for efficient retrieval of KEM IDs, followed by fetching the full KEM objects.
      Supports pagination and sorting by 'updated_at_ts'.
    - Serialization of embeddings using MessagePack for efficiency.
    - Timestamp strings are stored in ISO 8601 format, explicitly marked with 'Z' for UTC.

    Raises:
        RuntimeError: If 'aioredis' or 'msgpack' libraries are not installed.
        RedisCacheTransactionError: If a transactional operation (set) fails after retries
                                    due to concurrent modifications (WatchError).
        RedisCacheError: For other generic Redis cache operation failures.
    """
    def __init__(self, redis_client: Any, config: SWMConfig):
        if not aioredis or not msgpack:
            raise RuntimeError("aioredis and msgpack libraries are required for RedisKemCache.")

        self.redis: aioredis.Redis = redis_client # type: ignore
        self.config = config

        self.kem_key_prefix = "swm_kem:"
        self.indexed_keys: Set[str] = set(config.INDEXED_METADATA_KEYS)
        self.index_meta_key_prefix = "swm_idx:meta:"
        self.index_date_created_key = "swm_idx:date:created_at"
        self.index_date_updated_key = "swm_idx:date:updated_at"

        logger.info(f"RedisKemCache initialized for field-based storage. Indexed metadata keys: {self.indexed_keys}")

class RedisCacheError(Exception):
    """Base exception for RedisKemCache related errors."""
    pass

class RedisCacheTransactionError(RedisCacheError):
    """
    Raised when a Redis transaction (e.g., using WATCH/MULTI/EXEC) fails
    after multiple retries, typically due to concurrent modifications to watched keys.
    """
    pass

    def _serialize_embeddings(self, embeddings: List[float]) -> bytes:
        """Serializes a list of floats (embeddings) using MessagePack."""
        if not msgpack: return b''
        return msgpack.packb(embeddings, use_bin_type=True)

    def _deserialize_embeddings(self, data: Optional[bytes]) -> List[float]:
        if not data or not msgpack: return []
        try:
            unpacked = msgpack.unpackb(data, raw=False)
            if isinstance(unpacked, list) and all(isinstance(x, (float, int)) for x in unpacked):
                return [float(x) for x in unpacked]
            logger.warning(f"Failed to deserialize embeddings: unexpected data type {type(unpacked)}")
        except Exception as e:
            logger.error(f"Error deserializing embeddings with msgpack: {e}", exc_info=True)
        return []

    def _deserialize_embeddings(self, data: Optional[bytes]) -> List[float]:
        """Deserializes bytes into a list of floats (embeddings) using MessagePack."""
        if not data or not msgpack: return []
        try:
            unpacked = msgpack.unpackb(data, raw=False)
            if isinstance(unpacked, list) and all(isinstance(x, (float, int)) for x in unpacked):
                return [float(x) for x in unpacked]
            logger.warning(f"Failed to deserialize embeddings: unexpected data type {type(unpacked)}")
        except Exception as e:
            logger.error(f"Error deserializing embeddings with msgpack: {e}", exc_info=True)
        return []

    def _kem_to_hash_payload(self, kem: kem_pb2.KEM) -> Dict[str, Any]:
        """
        Converts a KEM protobuf object into a dictionary suitable for storing as a Redis Hash.
        Ensures timestamp strings are in UTC ISO 8601 format (ending with 'Z').
        """
        # Ensure ISO format strings for timestamps include 'Z' for UTC indication
        # This simplifies parsing later.
        created_at_dt = kem.created_at.ToDatetime()
        updated_at_dt = kem.updated_at.ToDatetime()

        # Python's isoformat() on naive datetime doesn't add 'Z'.
        # If tzinfo is None, assume UTC as per common practice in this system.
        created_at_iso = created_at_dt.isoformat(timespec='seconds')
        if created_at_dt.tzinfo is None and 'T' in created_at_iso: # Naive, make it UTC
            created_at_iso += 'Z'

        updated_at_iso = updated_at_dt.isoformat(timespec='seconds')
        if updated_at_dt.tzinfo is None and 'T' in updated_at_iso: # Naive, make it UTC
            updated_at_iso += 'Z'

        payload = {
            "id": kem.id,
            "content_type": kem.content_type,
            "content": kem.content,
            "metadata": json.dumps(dict(kem.metadata)),
            "created_at": created_at_iso,
            "updated_at": updated_at_iso,
        }
        payload["created_at_ts"] = kem.created_at.seconds
        payload["updated_at_ts"] = kem.updated_at.seconds

        if kem.embeddings:
            payload["embeddings"] = self._serialize_embeddings(list(kem.embeddings))
        else:
            payload["embeddings"] = b''
        return payload

    async def _hash_payload_to_kem(self, kem_id: str, payload_bytes: Dict[bytes, bytes]) -> Optional[kem_pb2.KEM]:
        """
        Converts a dictionary (from Redis HGETALL, with bytes keys/values) into a KEM protobuf object.
        Handles deserialization of metadata (JSON), timestamps (ISO 8601), and embeddings (MessagePack).
        """
        if not payload_bytes: return None
        try:
            kem = kem_pb2.KEM()
            kem.id = payload_bytes.get(b'id', b'').decode('utf-8') or kem_id
            kem.content_type = payload_bytes.get(b'content_type', b'').decode('utf-8')

            content_val = payload_bytes.get(b'content')
            if content_val is not None: kem.content = content_val

            metadata_str = payload_bytes.get(b'metadata', b'{}').decode('utf-8')
            try:
                kem.metadata.clear(); kem.metadata.update(json.loads(metadata_str))
            except json.JSONDecodeError:
                logger.warning(f"RedisKemCache: Invalid metadata JSON for KEM {kem.id}: {metadata_str}")

            for ts_field_name_bytes, ts_proto_field in [(b"created_at", kem.created_at), (b"updated_at", kem.updated_at)]:
                ts_iso_bytes = payload_bytes.get(ts_field_name_bytes)
                if ts_iso_bytes:
                    ts_iso_str = ts_iso_bytes.decode('utf-8')
                    # Assuming timestamps are stored with 'Z' by _kem_to_hash_payload
                    # or are in a format directly parsable by FromJsonString.
                    try:
                        ts_proto_field.FromJsonString(ts_iso_str)
                    except Exception as e_ts:
                        logger.warning(f"Redis: Failed to parse {ts_field_name_bytes.decode()} '{ts_iso_str}' with FromJsonString: {e_ts}. Attempting with appended 'Z'.")
                        try:
                            # Fallback for potentially older data or if 'Z' was missed
                            if 'T' in ts_iso_str and not ts_iso_str.endswith('Z') and '+' not in ts_iso_str and '-' not in ts_iso_str[10:]:
                                ts_proto_field.FromJsonString(ts_iso_str + 'Z')
                            else: # If it already ends with Z or has offset, direct parse should have worked or will fail similarly.
                                raise e_ts # Re-raise original error if Z append logic doesn't apply
                        except Exception as e_ts_fallback:
                             logger.error(f"Redis: Fallback parsing also failed for {ts_field_name_bytes.decode()} '{ts_iso_str}': {e_ts_fallback}")

            embeddings_data = payload_bytes.get(b'embeddings')
            if embeddings_data: kem.embeddings.extend(self._deserialize_embeddings(embeddings_data))

            return kem
        except Exception as e:
            logger.error(f"RedisKemCache: Error deserializing KEM {kem_id} from hash: {e}", exc_info=True)
            return None

    async def get(self, kem_id: str) -> Optional[kem_pb2.KEM]:
        """Retrieves a KEM from the Redis cache by its ID."""
        if not self.redis: logger.error("Redis client not available in get"); return None
        kem_key = f"{self.kem_key_prefix}{kem_id}"
        try:
            kem_hash_data_bytes = await self.redis.hgetall(kem_key)
            if kem_hash_data_bytes:
                return await self._hash_payload_to_kem(kem_id, kem_hash_data_bytes)
            return None
        except Exception as e:
            logger.error(f"RedisKemCache: Error during GET for KEM ID '{kem_id}': {e}", exc_info=True)
            return None

    async def _get_old_indexed_fields(self, kem_key: str) -> Dict[str, Any]:
        """
        Retrieves specific fields (metadata, created_at_ts, updated_at_ts) from an existing KEM
        in Redis. Used internally by 'set' and 'delete' to manage index cleanup.
        Timestamps are converted to seconds (epoch).
        """
        old_data = {"metadata": {}, "created_at_ts": None, "updated_at_ts": None}
        if not self.redis: return old_data
        # Note: This fetches string representations of created_at/updated_at, then parses to seconds.
        # It relies on _kem_to_hash_payload storing 'created_at_ts' and 'updated_at_ts' as direct seconds,
        # but here it seems to re-parse from ISO strings if it were to fetch 'created_at'/'updated_at' fields.
        # For index management, it's the *_ts fields (seconds) that are primarily used.
        # The current implementation fetches 'metadata', 'created_at', 'updated_at' (ISO strings).
        # Let's clarify its purpose and what it actually fetches for index updates.
        # It's used to get old metadata for indexed string values, and old created_at/updated_at
        # for removing from ZSETs. The *_ts values are directly stored in the hash, but this function
        # seems to re-derive them from ISO strings for 'created_at' and 'updated_at' fields.
        # This might be slightly inefficient or redundant if *_ts fields are already in the hash.
        # For now, documenting existing behavior.
        # Corrected: It *should* be fetching the *_ts fields for direct use if they are available
        # and reliably stored. However, current redis.hmget fetches 'created_at', 'updated_at'.
        # Let's assume 'created_at_ts' and 'updated_at_ts' are the primary source for ZSET scores.
        # The _get_old_indexed_fields is mainly for:
        # 1. Getting old metadata values (from 'metadata' JSON field) to remove from string-based indexes.
        # 2. Getting old timestamp *scores* (seconds) to remove from ZSETs.
        # It appears the current `hmget` fetches the ISO string date fields, then converts them to seconds.
        # This is correct for getting the seconds value if only ISO strings were stored.
        raw_fields = await self.redis.hmget(kem_key, "metadata", "created_at", "updated_at")
        if raw_fields:
            if raw_fields[0] is not None:
                try: old_data["metadata"] = json.loads(raw_fields[0].decode('utf-8'))
                except: pass
            if raw_fields[1] is not None:
                try:
                    ts = Timestamp(); ts_iso_str = raw_fields[1].decode('utf-8')
                    ts.FromJsonString(ts_iso_str + ("Z" if not ts_iso_str.endswith("Z") and 'T' in ts_iso_str else ""))
                    old_data["created_at_ts"] = ts.seconds
                except: pass
            if raw_fields[2] is not None:
                try:
                    ts = Timestamp(); ts_iso_str = raw_fields[2].decode('utf-8')
                    ts.FromJsonString(ts_iso_str + ("Z" if not ts_iso_str.endswith("Z") and 'T' in ts_iso_str else ""))
                    old_data["updated_at_ts"] = ts.seconds
                except: pass
        return old_data

    async def set(self, kem_id: str, kem: kem_pb2.KEM) -> None:
        """
        Adds or updates a KEM in the Redis cache and its associated secondary indexes.

        This operation is transactional using Redis WATCH/MULTI/EXEC to ensure atomicity
        when updating both the KEM hash and its index entries. It retries on WatchError
        up to a defined maximum.

        Args:
            kem_id: The ID of the KEM to set.
            kem: The KEM protobuf object to store.

        Raises:
            ConnectionError: If the Redis client is not available.
            RedisCacheTransactionError: If the transaction fails after multiple retries
                                        due to concurrent modifications.
            RedisCacheError: For other errors during the transaction.
        """
        if not self.redis:
            logger.error("Redis client not available in set"); raise ConnectionError("Redis client not available")

        kem_key = f"{self.kem_key_prefix}{kem_id}"
        new_kem_payload_for_hash = self._kem_to_hash_payload(kem)
        new_metadata = dict(kem.metadata)
        new_created_at_ts = kem.created_at.seconds
        new_updated_at_ts = kem.updated_at.seconds

        max_retries = 3
        for attempt in range(max_retries):
            async with self.redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(kem_key)
                    old_indexed_data = await self._get_old_indexed_fields(kem_key)

                    pipe.multi()

                    for meta_idx_key in self.indexed_keys:
                        old_val = old_indexed_data["metadata"].get(meta_idx_key)
                        new_val = new_metadata.get(meta_idx_key)
                        if old_val is not None and str(old_val) != str(new_val):
                            pipe.srem(f"{self.index_meta_key_prefix}{meta_idx_key}:{str(old_val)}", kem_id)
                        if new_val is not None and str(old_val) != str(new_val):
                             pipe.sadd(f"{self.index_meta_key_prefix}{meta_idx_key}:{str(new_val)}", kem_id)

                    if old_indexed_data["created_at_ts"] != new_created_at_ts and old_indexed_data["created_at_ts"] is not None : pipe.zrem(self.index_date_created_key, kem_id)
                    if old_indexed_data["updated_at_ts"] != new_updated_at_ts and old_indexed_data["updated_at_ts"] is not None : pipe.zrem(self.index_date_updated_key, kem_id)

                    pipe.hmset(kem_key, new_kem_payload_for_hash)

                    pipe.zadd(self.index_date_created_key, {kem_id: new_created_at_ts})
                    pipe.zadd(self.index_date_updated_key, {kem_id: new_updated_at_ts})

                    await pipe.execute()
                    logger.info(f"RedisKemCache: KEM ID '{kem_id}' set successfully with index updates.")
                    return
                except aioredis.exceptions.WatchError: # type: ignore
                    logger.warning(f"RedisKemCache: WATCH error for KEM ID '{kem_id}', attempt {attempt + 1}/{max_retries}. Retrying.")
                    if attempt == max_retries - 1:
                        err_msg = f"RedisKemCache: KEM ID '{kem_id}' failed after max WatchError retries."
                        logger.error(err_msg)
                        raise RedisCacheTransactionError(err_msg)
                    await asyncio.sleep(0.01 * (2**attempt))
                    continue
                except Exception as e_exec:
                    logger.error(f"RedisKemCache: Error during SET transaction for KEM ID '{kem_id}': {e_exec}", exc_info=True)
                    raise RedisCacheError(f"Failed during SET transaction for KEM ID '{kem_id}': {e_exec}") from e_exec
        # This line should ideally not be reached if the loop always raises an exception on failure.
        # However, as a safeguard:
        final_err_msg = f"Failed to set KEM ID '{kem_id}' after multiple WatchError retries and loop completion."
        logger.error(final_err_msg)
        raise RedisCacheTransactionError(final_err_msg)


    async def delete(self, kem_id: str) -> bool:
        """
        Deletes a KEM from the Redis cache and cleans up its associated secondary index entries.

        This operation is transactional using Redis MULTI/EXEC. While WATCH is not strictly
        necessary here (as we are just deleting), using a pipeline ensures all delete
        operations (KEM hash and index entries) are sent to Redis as a single atomic unit.

        Args:
            kem_id: The ID of the KEM to delete.

        Returns:
            True if the KEM was found and deletion commands were successfully executed
            (or if KEM was not found initially). False on Redis client unavailability.

        Raises:
            RedisCacheError: If an error occurs during the Redis transaction.
        """
        if not self.redis: logger.error("Redis client not available in delete"); return False
        kem_key = f"{self.kem_key_prefix}{kem_id}"

        old_indexed_data = await self._get_old_indexed_fields(kem_key)
        if not await self.redis.exists(kem_key):
             logger.info(f"RedisKemCache: KEM ID '{kem_id}' not found for deletion.")
             return True

        async with self.redis.pipeline(transaction=True) as pipe:
            for meta_idx_key in self.indexed_keys:
                old_val = old_indexed_data["metadata"].get(meta_idx_key)
                if old_val is not None:
                    pipe.srem(f"{self.index_meta_key_prefix}{meta_idx_key}:{str(old_val)}", kem_id)

            if old_indexed_data["created_at_ts"] is not None: pipe.zrem(self.index_date_created_key, kem_id)
            if old_indexed_data["updated_at_ts"] is not None: pipe.zrem(self.index_date_updated_key, kem_id)
            pipe.delete(kem_key)
            try:
                results = await pipe.execute()
                was_deleted = results[-1] > 0
                logger.info(f"RedisKemCache: KEM ID '{kem_id}' {'deleted' if was_deleted else 'not found during TX'} (with index cleanup).")
                return True
            except Exception as e_del_exec:
                logger.error(f"RedisKemCache: Error during DELETE transaction for KEM ID '{kem_id}': {e_del_exec}", exc_info=True)
                raise
        return False

    async def contains(self, kem_id: str) -> bool:
        """Checks if a KEM with the given ID exists in the Redis cache."""
        if not self.redis: logger.error("Redis client not available in contains"); return False
        kem_key = f"{self.kem_key_prefix}{kem_id}"
        try:
            exists = await self.redis.exists(kem_key)
            return exists == 1
        except Exception as e:
            logger.error(f"RedisKemCache: Error during CONTAINS for KEM ID '{kem_id}': {e}", exc_info=True)
            return False

    async def query_by_filters(
        self,
        query: glm_service_pb2.KEMQuery,
        page_size: int,
        page_token: Optional[str]
    ) -> Tuple[List[kem_pb2.KEM], str]:
        """
        Queries KEMs from the Redis cache based on provided filters.

        The process involves:
        1.  Identifying relevant Redis Sets/Sorted Sets based on indexed fields in the query
            (specific metadata keys, created_at/updated_at ranges, KEM IDs).
            - For date ranges, temporary Redis Sets are created from ZRANGEBYSCORE results.
            - For direct ID lists, a temporary Redis Set is created.
        2.  If multiple indexed filters apply, their corresponding Redis Sets are intersected
            using SINTERSTORE into a temporary result set of KEM IDs.
        3.  The resulting set of KEM IDs is sorted by 'updated_at_ts' (descending) and
            paginated using the Redis SORT command.
        4.  Full KEM objects for the current page are fetched from their Redis Hashes.
        5.  Any metadata filters for non-indexed keys are applied as a post-filtering step
            on the retrieved KEMs.

        Args:
            query: A KEMQuery protobuf object containing filter criteria.
            page_size: The number of KEMs to return per page.
            page_token: An opaque token (currently an integer offset) for pagination.

        Returns:
            A tuple containing:
                - A list of KEM protobuf objects for the current page.
                - A string for the next page token (empty if no more pages).

        Notes:
            - Vector/text search parts of KEMQuery are ignored as this cache doesn't support them.
            - If a query contains only non-indexed filters or no effective indexed filters that
              yield results, it currently returns an empty list as full cache scans (SCAN) are
              not implemented for performance reasons.
            - Temporary Redis keys used during the query are cleaned up in a 'finally' block.
        """
        if not self.redis:
            logger.error("RedisKemCache.query_by_filters: Redis client not available.")
            return [], ""

        temp_key_base = f"swm_query_tmp:{uuid.uuid4()}" # Base for temporary keys for this query
        intersect_keys_to_delete = []
        final_ids_set_key: Optional[str] = None # Key of the Redis SET holding candidate IDs

        try:
            # --- 1. Collect IDs from all relevant indexed filters ---
            temp_set_keys_for_filters: List[str] = []

            # --- 1. Collect IDs from all relevant indexed filters ---
            # Stores keys of Redis sets that will be used in SINTERSTORE.
            # These can be pre-existing index keys or temporary keys created for this query.
            sinter_candidate_keys: List[str] = []


            if query.ids:
                # Create a temporary set for the requested IDs to use in intersection
                current_filter_key = f"{temp_key_base}:ids_direct"
                if query.ids: # Only create and add if there are actual IDs
                    await self.redis.sadd(current_filter_key, *list(query.ids))
                    sinter_candidate_keys.append(current_filter_key)
                    intersect_keys_to_delete.append(current_filter_key)
                else: # If query.ids is empty, it's like no ID filter was applied from this source
                    pass

            for meta_k, meta_v in query.metadata_filters.items():
                if meta_k in self.indexed_keys:
                    # Use the direct pre-existing index key for metadata
                    idx_key = f"{self.index_meta_key_prefix}{meta_k}:{meta_v}"
                    sinter_candidate_keys.append(idx_key)

            async def _fetch_ids_from_zset_range_to_temp_set(
                zset_idx_name: str, ts_start: Timestamp, ts_end: Timestamp, temp_set_name_for_zset: str
            ) -> bool:
                """Fetches IDs from a ZSET range and stores them in a temporary SET. Returns True if IDs were found and stored."""
                min_s = str(ts_start.seconds) if (ts_start.seconds > 0 or ts_start.nanos > 0) else "-inf"
                max_s = str(ts_end.seconds) if (ts_end.seconds > 0 or ts_end.nanos > 0) else "+inf"
                try:
                    ids_b = await self.redis.zrangebyscore(zset_idx_name, min_s, max_s)
                    if ids_b:
                        await self.redis.sadd(temp_set_name_for_zset, *ids_b)
                        return True
                    return False # No IDs found in range
                except Exception as e_zset:
                    logger.error(f"Redis Error processing ZSET {zset_idx_name}: {e_zset}", exc_info=True)
                    return False # Error, treat as no IDs found for this filter

            if query.HasField("created_at_start") or query.HasField("created_at_end"):
                temp_created_key = f"{temp_key_base}:created_at_range"
                if await _fetch_ids_from_zset_range_to_temp_set(
                    self.index_date_created_key, query.created_at_start, query.created_at_end, temp_created_key
                ):
                    sinter_candidate_keys.append(temp_created_key)
                    intersect_keys_to_delete.append(temp_created_key)
                # If no IDs found, this filter effectively means "empty set" for intersection

            if query.HasField("updated_at_start") or query.HasField("updated_at_end"):
                temp_updated_key = f"{temp_key_base}:updated_at_range"
                if await _fetch_ids_from_zset_range_to_temp_set(
                    self.index_date_updated_key, query.updated_at_start, query.updated_at_end, temp_updated_key
                ):
                    sinter_candidate_keys.append(temp_updated_key)
                    intersect_keys_to_delete.append(temp_updated_key)
                # If no IDs found, this filter effectively means "empty set" for intersection

            # --- 2. Intersect ID sets if multiple filters were applied ---
            # If no sinter_candidate_keys, it means either:
            #   a) No indexed filters were specified at all.
            #   b) Indexed filters were specified, but they all resulted in empty sets (e.g., zrangebyscore found nothing).
            if not sinter_candidate_keys:
                # Check if there are non-indexed filters. If so, we'd need to scan all, which is not supported.
                # If no filters at all (indexed or non-indexed), also not supported without scan.
                has_non_indexed_filters = any(k not in self.indexed_keys for k in query.metadata_filters.keys())
                if has_non_indexed_filters or not query.metadata_filters and not query.ids:
                     logger.warning("RedisKemCache: Query has only non-indexed filters or no effective indexed filters. Full scan not implemented. Returning empty.")
                     return [], ""
                # If we are here, it means there were indexed filters, but they all yielded no results.
                # So the intersection is empty.
                return [], ""

            # The key that will hold the result of SINTERSTORE, or be one of the candidates if only one.
            final_ids_set_key_for_sort: str
            if len(sinter_candidate_keys) == 1:
                final_ids_set_key_for_sort = sinter_candidate_keys[0]
            else:
                final_ids_set_key_for_sort = f"{temp_key_base}:final_intersect"
                intersect_keys_to_delete.append(final_ids_set_key_for_sort)
                await self.redis.sinterstore(final_ids_set_key_for_sort, *sinter_candidate_keys)

            total_after_indexed_filter = await self.redis.scard(final_ids_set_key_for_sort)
            if total_after_indexed_filter == 0:
                logger.debug("RedisKemCache: SINTERSTORE resulted in 0 KEMs after indexed filters.")
                return [], ""

            # --- 3. Sorting and Pagination using Redis SORT ---
            current_offset = 0
            if page_token:
                try: current_offset = int(page_token)
                except ValueError: logger.warning(f"Invalid page_token '{page_token}', using 0.")

            # Request one more item than page_size to determine if there's a next page
            items_to_fetch_for_sort = page_size + 1

            sort_args = [
                final_ids_set_key,
                "BY", f"{self.kem_key_prefix}*->updated_at_ts",
                "DESC",
                "LIMIT", str(current_offset), str(items_to_fetch_for_sort),
                "GET", "#"
            ]
            logger.debug(f"RedisKemCache: Executing SORT command: {sort_args}")
            sorted_and_paginated_ids_plus_one_bytes: List[bytes] = await self.redis.sort(*sort_args) # type: ignore

            has_more_items = len(sorted_and_paginated_ids_plus_one_bytes) > page_size
            ids_for_page_bytes = sorted_and_paginated_ids_plus_one_bytes[:page_size]
            ids_for_page = [i.decode('utf-8') for i in ids_for_page_bytes if i]

            # --- 4. Fetch full KEMs for the current page and apply post-filtering ---
            kems_on_page_final: List[kem_pb2.KEM] = []
            if ids_for_page:
                pipe_kems = self.redis.pipeline()
                for kem_id_p in ids_for_page:
                    pipe_kems.hgetall(f"{self.kem_key_prefix}{kem_id_p}")

                kem_hashes_page = await pipe_kems.execute()
                for i, kem_hash_data_p in enumerate(kem_hashes_page):
                    if kem_hash_data_p:
                        kem_obj_p = await self._hash_payload_to_kem(ids_for_page[i], kem_hash_data_p)
                        if kem_obj_p:
                            match_non_indexed = True
                            non_indexed_filters = {k:v for k,v in query.metadata_filters.items() if k not in self.indexed_keys}
                            if non_indexed_filters:
                                for key_ni, val_ni in non_indexed_filters.items():
                                    if kem_obj_p.metadata.get(key_ni) != val_ni:
                                        match_non_indexed = False; break
                            if match_non_indexed:
                                 kems_on_page_final.append(kem_obj_p)

            next_page_token_val = ""
            if has_more_items:
                next_page_token_val = str(current_offset + page_size)

            logger.info(f"RedisKemCache: Query returning {len(kems_on_page_final)} KEMs.")
            return kems_on_page_final, next_page_token_val

        finally: # Cleanup temporary keys
            if intersect_keys_to_delete:
                try: await self.redis.delete(*intersect_keys_to_delete)
                except Exception as e_del_temp: logger.warning(f"RedisKemCache: Failed to delete temporary query keys: {intersect_keys_to_delete}, Error: {e_del_temp}")


logger.info("RedisKemCache module loaded (Iteration 3 - query_by_filters with SORT).")
```
