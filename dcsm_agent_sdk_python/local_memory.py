from cachetools import LRUCache, Cache
import typing
import asyncio # For async version
from dcs_memory.common.utils import RWLock # Import the new RWLock

class IndexedLRUCache(Cache):
    def __init__(self, maxsize: int, indexed_keys: typing.List[str],
                 on_evict_callback: typing.Optional[typing.Callable[[dict], None]] = None):
        # Removed incorrect super().__init__ call.
        # If full cachetools.Cache ABC compliance is needed later, __iter__ must be implemented.
        # For now, we focus on making this specific implementation async with RWLock.

        self.actual_maxsize = maxsize
        self._lru = LRUCache(maxsize=maxsize)
        self._indexed_keys = set(indexed_keys if indexed_keys else [])
        self._metadata_indexes: typing.Dict[str, typing.Dict[str, typing.Set[str]]] = {key: {} for key in self._indexed_keys}
        self.rw_lock = RWLock() # Use the new RWLock
        self._on_evict_callback = on_evict_callback
        # Note: `on_evict_callback` is sync. If it does long operations, it could block the event loop
        # if called directly from an async method holding a write lock. Consider `asyncio.to_thread` for it if needed.

    # Internal helper methods remain synchronous as they are called under lock
    def _add_to_metadata_indexes(self, kem_data: dict):
        if not kem_data or not kem_data.get('id'): return
        kem_id = kem_data['id']
        metadata = kem_data.get('metadata', {})
        if not isinstance(metadata, dict): return

        for meta_key in self._indexed_keys:
            if meta_key in metadata:
                value = metadata[meta_key]
                if isinstance(value, str): # Only index string values for simplicity
                    self._metadata_indexes.setdefault(meta_key, {}).setdefault(value, set()).add(kem_id)

    def _remove_from_metadata_indexes(self, kem_data: dict):
        if not kem_data or not kem_data.get('id'): return
        kem_id = kem_data['id']
        metadata = kem_data.get('metadata', {})
        if not isinstance(metadata, dict): return

        for meta_key in self._indexed_keys:
            if meta_key in metadata:
                original_value = metadata[meta_key]
                if isinstance(original_value, str):
                    if meta_key in self._metadata_indexes and original_value in self._metadata_indexes[meta_key]:
                        self._metadata_indexes[meta_key][original_value].discard(kem_id)
                        if not self._metadata_indexes[meta_key][original_value]: # Value set is empty
                            del self._metadata_indexes[meta_key][original_value]
                        if not self._metadata_indexes[meta_key]: # Meta key dict is empty
                            del self._metadata_indexes[meta_key] # Clean up empty dict for meta_key

    async def __setitem__(self, kem_id: str, kem_data: dict):
        async with self.rw_lock.write_lock():
            # evicted_kem_data = None # Not used outside this scope directly
            if kem_id in self._lru:
                old_kem_data = self._lru[kem_id] # Get before overwriting for removal from index
                self._remove_from_metadata_indexes(old_kem_data)
            elif len(self._lru) >= self._lru.maxsize: # Check for eviction before adding new item
                # LRUCache.popitem() evicts the least recently used item
                _evicted_id, evicted_item_data = self._lru.popitem() # popitem from cachetools.LRUCache
                if evicted_item_data: # Should always be true if popitem succeeded
                    self._remove_from_metadata_indexes(evicted_item_data)
                    if self._on_evict_callback:
                        try:
                            # If _on_evict_callback is potentially blocking, wrap it:
                            # await asyncio.to_thread(self._on_evict_callback, evicted_item_data)
                            self._on_evict_callback(evicted_item_data)
                        except Exception:
                            # Minimal logging for SDK cache, or use proper logger
                            pass

            self._lru[kem_id] = kem_data # This also updates its position as most recently used
            self._add_to_metadata_indexes(kem_data)

    async def __getitem__(self, kem_id: str) -> dict:
        async with self.rw_lock.read_lock():
            # Accessing self._lru[kem_id] also marks it as recently used in LRUCache
            return self._lru[kem_id]

    async def __delitem__(self, kem_id: str):
        async with self.rw_lock.write_lock():
            if kem_id in self._lru: # Check existence before pop
                kem_to_remove = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem_to_remove)
            else:
                raise KeyError(kem_id) # Standard behavior for missing key

    async def get(self, kem_id: str, default=None) -> typing.Optional[dict]:
        async with self.rw_lock.read_lock():
            # .get() on LRUCache also updates its position as most recently used
            return self._lru.get(kem_id, default)

    async def pop(self, kem_id: str, default=object()) -> dict:
        async with self.rw_lock.write_lock():
            if kem_id in self._lru:
                kem_data = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem_data)
                return kem_data
            elif default is not object(): # Check if a default value was provided
                return default # type: ignore
            else:
                raise KeyError(kem_id) # No default and key not found

    async def __len__(self) -> int:
        async with self.rw_lock.read_lock():
            return len(self._lru)

    async def __contains__(self, kem_id: str) -> bool:
        async with self.rw_lock.read_lock():
            return kem_id in self._lru # This updates LRU order for key if present

    async def values(self) -> typing.List[dict]:
        async with self.rw_lock.read_lock():
            return list(self._lru.values()) # LRUCache.values() doesn't guarantee order

    async def items(self) -> typing.List[typing.Tuple[str, dict]]:
        async with self.rw_lock.read_lock():
            return list(self._lru.items()) # LRUCache.items() doesn't guarantee order

    async def clear(self):
        async with self.rw_lock.write_lock():
            self._lru.clear()
            self._metadata_indexes.clear() # Clear existing
            # Re-initialize to ensure structure for indexed_keys is present
            self._metadata_indexes = {key: {} for key in self._indexed_keys}

    @property
    async def maxsize(self): # Made async property, though it only accesses self._lru
        async with self.rw_lock.read_lock(): # Added lock for consistency, though maxsize is usually static
            return self._lru.maxsize

    # This method was missing async conversion and lock in the previous step
    async def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Set[str]:
        if meta_key not in self._indexed_keys: # This check can be outside the lock
            return set()
        async with self.rw_lock.read_lock():
            return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()


class LocalAgentMemory:
    def __init__(self, max_size: int = 100, indexed_keys: typing.Optional[typing.List[str]] = None):
        if indexed_keys is None:
            indexed_keys = []
        # IndexedLRUCache itself is now async internally, but its constructor is sync.
        self.cache = IndexedLRUCache(maxsize=max_size, indexed_keys=indexed_keys)

    async def get(self, kem_id: str) -> typing.Optional[dict]:
        return await self.cache.get(kem_id)

    async def query(self, metadata_filters: typing.Optional[dict] = None, ids: typing.Optional[list[str]] = None) -> list[dict]:
        """
        Queries KEMs from local memory (LAM) with filtering. (Async version)
        Supports filtering by ID and by indexed/non-indexed metadata fields.
        Date filters are not implemented here for simplicity.
        """
        candidate_kems: typing.List[dict] = []

        indexed_metadata_queries: typing.Dict[str, str] = {}
        unindexed_metadata_queries: typing.Dict[str, typing.Any] = {}

        if metadata_filters:
            for key, value in metadata_filters.items():
                if key in self.cache._indexed_keys and isinstance(value, str):
                    indexed_metadata_queries[key] = value
                else:
                    unindexed_metadata_queries[key] = value

        # Stage 1: Filter by indexed metadata keys (if any)
        if indexed_metadata_queries:
            intersected_ids: typing.Optional[typing.Set[str]] = None
            for key, value in indexed_metadata_queries.items():
                ids_from_index = await self.cache.get_ids_by_metadata_filter(key, value) # await
                if intersected_ids is None:
                    intersected_ids = ids_from_index
                else:
                    intersected_ids.intersection_update(ids_from_index)
                if not intersected_ids: # Empty set after intersection
                    return []

            if intersected_ids is not None: # Should not be None if indexed_metadata_queries was not empty
                for kem_id in intersected_ids:
                    kem = await self.cache.get(kem_id) # await
                    if kem: candidate_kems.append(kem)

        # Stage 2: If no indexed filters were applied, or if they were but we need to start from a broader set for other filters
        # If specific IDs are provided, and no indexed filters were effective, start with those.
        elif ids: # No indexed_metadata_queries or they yielded no restrictions to use.
            ids_set = set(ids)
            for kem_id_filter in ids_set:
                kem = await self.cache.get(kem_id_filter) # await
                if kem: candidate_kems.append(kem)
        else:
            # No indexed filters and no ID filters to start with, so get all values for further filtering.
            # This is the potentially inefficient part if the cache is large.
            candidate_kems = await self.cache.values() # await

        # Stage 3: Filter by IDs if not already implicitly handled by indexed search results
        # (e.g., if indexed search was skipped, or if IDs are a further refinement)
        if ids and not (indexed_metadata_queries and final_candidate_ids is not None): # final_candidate_ids is not defined here
                                                                                        # This condition needs rethink if we want to combine ID filter with indexed results
            # If indexed filters were applied, candidate_kems already respects them.
            # If no indexed filters, candidate_kems might be all cache values or filtered by ID (if ids was the entry point).
            # This re-filtering by IDs might be redundant or incorrect depending on previous path.
            # Let's simplify: if IDs are provided, they act as a primary filter if no indexed results are used.
            # If indexed results are used, IDs can be an additional AND filter.

            # Corrected logic:
            # If candidate_kems was populated by indexed search, further filter by IDs.
            # If candidate_kems was populated by initial ID filter, this is redundant.
            # If candidate_kems is all values, filter by IDs.
            ids_set = set(ids)
            candidate_kems = [k for k in candidate_kems if k.get('id') in ids_set]


        # Stage 4: Filter by unindexed metadata
        if unindexed_metadata_queries:
            filtered_kems_after_unindexed = []
            for kem in candidate_kems:
                match = True
                kem_meta = kem.get('metadata', {})
                for key, value in unindexed_metadata_queries.items():
                    if kem_meta.get(key) != value:
                        match = False; break
                if match: filtered_kems_after_unindexed.append(kem)
            candidate_kems = filtered_kems_after_unindexed

        return candidate_kems

    async def put(self, kem_id: str, kem_data: dict): # async
        if not isinstance(kem_data, dict): return
        data_to_store = kem_data.copy(); data_to_store['id'] = kem_id
        await self.cache.__setitem__(kem_id, data_to_store) # await and direct call to async __setitem__

    async def delete(self, kem_id: str) -> bool: # async
        if await self.cache.__contains__(kem_id): # await
            await self.cache.__delitem__(kem_id) # await
            return True
        return False

    async def contains(self, kem_id: str) -> bool: # async
        return await self.cache.__contains__(kem_id) # await

    async def clear(self): # async
        await self.cache.clear() # await

    @property
    async def current_size(self) -> int: # async property
        return await self.cache.__len__() # await

if __name__ == '__main__':
    # Test code needs to be run in an asyncio event loop
    async def main_test():
        print("Running LocalAgentMemory full test (async)...")
        lam_indexed = LocalAgentMemory(max_size=3, indexed_keys=["topic", "status"])
        kem_a = {"id": "kemA", "content": "Content A", "metadata": {"topic": "news", "status": "draft"}}
        kem_b = {"id": "kemB", "content": "Content B", "metadata": {"topic": "tech", "status": "published"}}
        kem_c = {"id": "kemC", "content": "Content C", "metadata": {"topic": "news", "status": "published"}}
        kem_d = {"id": "kemD", "content": "Content D", "metadata": {"topic": "sports", "status": "draft", "extra": "info"}}

        await lam_indexed.put(kem_a["id"], kem_a)
        await lam_indexed.put(kem_b["id"], kem_b)
        await lam_indexed.put(kem_c["id"], kem_c)

        print(f"Indexed LAM size: {await lam_indexed.current_size}")

        news_kems = await lam_indexed.query(metadata_filters={"topic": "news"})
        print(f"KEMs with topic 'news': {len(news_kems)} (IDs: {[k['id'] for k in news_kems]})")
        assert len(news_kems) == 2

        published_kems = await lam_indexed.query(metadata_filters={"status": "published"})
        print(f"KEMs with status 'published': {len(published_kems)} (IDs: {[k['id'] for k in published_kems]})")
        assert len(published_kems) == 2

        news_published_kems = await lam_indexed.query(metadata_filters={"topic": "news", "status": "published"})
        print(f"KEMs with topic 'news' AND status 'published': {len(news_published_kems)} (IDs: {[k['id'] for k in news_published_kems]})")
        assert len(news_published_kems) == 1 and news_published_kems[0]['id'] == "kemC"

        await lam_indexed.get("kemA") # Access to make it recently used
        await lam_indexed.put(kem_d["id"], kem_d) # This should evict kemB if max_size=3

        assert not await lam_indexed.contains("kemB")
        print(f"Contains kemB (False): {await lam_indexed.contains('kemB')}")

        tech_kems_after_evict = await lam_indexed.query(metadata_filters={"topic": "tech"})
        assert len(tech_kems_after_evict) == 0
        print("Indexed LAM tests complete.")

    asyncio.run(main_test())
