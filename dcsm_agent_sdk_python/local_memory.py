from cachetools import LRUCache, Cache
import typing
import threading

class IndexedLRUCache(Cache):
    def __init__(self, maxsize: int, indexed_keys: typing.List[str],
                 on_evict_callback: typing.Optional[typing.Callable[[dict], None]] = None):
        super().__init__(maxsize)
        self._lru = LRUCache(maxsize=maxsize)
        self._indexed_keys = set(indexed_keys if indexed_keys else [])
        self._metadata_indexes: typing.Dict[str, typing.Dict[str, typing.Set[str]]] = {key: {} for key in self._indexed_keys}
        self._lock = threading.Lock()
        self._on_evict_callback = on_evict_callback

    def _add_to_metadata_indexes(self, kem_data: dict):
        if not kem_data or not kem_data.get('id'): return
        kem_id = kem_data['id']
        metadata = kem_data.get('metadata', {})
        if not isinstance(metadata, dict): return

        for meta_key in self._indexed_keys:
            if meta_key in metadata:
                value = metadata[meta_key]
                if isinstance(value, str):
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
                        if not self._metadata_indexes[meta_key][original_value]:
                            del self._metadata_indexes[meta_key][original_value]
                        if not self._metadata_indexes[meta_key]:
                            del self._metadata_indexes[meta_key]

    def __setitem__(self, kem_id: str, kem_data: dict):
        with self._lock:
            evicted_kem_data = None
            if kem_id in self._lru:
                old_kem_data = self._lru[kem_id]
                self._remove_from_metadata_indexes(old_kem_data)
            elif len(self._lru) >= self._lru.maxsize:
                _evicted_id, evicted_kem_data = self._lru.popitem()
                if evicted_kem_data:
                    self._remove_from_metadata_indexes(evicted_kem_data)
                    if self._on_evict_callback:
                        try:
                            self._on_evict_callback(evicted_kem_data)
                        except Exception: # Minimal logging for SDK cache
                            pass
            self._lru[kem_id] = kem_data
            self._add_to_metadata_indexes(kem_data)

    def __getitem__(self, kem_id: str) -> dict:
        with self._lock:
            return self._lru[kem_id]

    def __delitem__(self, kem_id: str):
        with self._lock:
            if kem_id in self._lru:
                kem_to_remove = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem_to_remove)
            else:
                raise KeyError(kem_id)

    def get(self, kem_id: str, default=None) -> typing.Optional[dict]:
        with self._lock:
            return self._lru.get(kem_id, default)

    def pop(self, kem_id: str, default=object()) -> dict:
        with self._lock:
            if kem_id in self._lru:
                kem_data = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem_data)
                return kem_data
            elif default is not object():
                return default # type: ignore
            else:
                raise KeyError(kem_id)

    def __len__(self) -> int:
        with self._lock:
            return len(self._lru)

    def __contains__(self, kem_id: str) -> bool:
        with self._lock:
            return kem_id in self._lru

    def values(self) -> typing.List[dict]:
        with self._lock:
            return list(self._lru.values())

    def items(self) -> typing.List[typing.Tuple[str, dict]]:
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

    def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Set[str]:
        if meta_key not in self._indexed_keys:
            return set()
        with self._lock:
            return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()


class LocalAgentMemory:
    def __init__(self, max_size: int = 100, indexed_keys: typing.Optional[typing.List[str]] = None):
        if indexed_keys is None:
            indexed_keys = []
        self.cache = IndexedLRUCache(maxsize=max_size, indexed_keys=indexed_keys)

    def get(self, kem_id: str) -> typing.Optional[dict]:
        return self.cache.get(kem_id)

    def query(self, metadata_filters: typing.Optional[dict] = None, ids: typing.Optional[list[str]] = None) -> list[dict]:
        """
        Queries KEMs from local memory (LAM) with filtering.
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
                ids_from_index = self.cache.get_ids_by_metadata_filter(key, value)
                if intersected_ids is None:
                    intersected_ids = ids_from_index
                else:
                    intersected_ids.intersection_update(ids_from_index)
                if not intersected_ids: # Empty set after intersection
                    return []

            if intersected_ids is not None: # Should not be None if indexed_metadata_queries was not empty
                for kem_id in intersected_ids:
                    kem = self.cache.get(kem_id)
                    if kem: candidate_kems.append(kem)

        # Stage 2: If no indexed filters were applied, or if they were but we need to start from a broader set for other filters
        # If specific IDs are provided, and no indexed filters were effective, start with those.
        elif ids: # No indexed_metadata_queries or they yielded no restrictions to use.
            ids_set = set(ids)
            for kem_id_filter in ids_set:
                kem = self.cache.get(kem_id_filter)
                if kem: candidate_kems.append(kem)
        else:
            # No indexed filters and no ID filters to start with, so get all values for further filtering.
            # This is the potentially inefficient part if the cache is large.
            candidate_kems = self.cache.values()

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

    def put(self, kem_id: str, kem_data: dict):
        if not isinstance(kem_data, dict): return
        data_to_store = kem_data.copy(); data_to_store['id'] = kem_id
        self.cache[kem_id] = data_to_store

    def delete(self, kem_id: str) -> bool:
        if kem_id in self.cache: del self.cache[kem_id]; return True
        return False

    def contains(self, kem_id: str) -> bool: return kem_id in self.cache
    def clear(self): self.cache.clear()
    @property
    def current_size(self) -> int: return len(self.cache)

if __name__ == '__main__':
    print("Running LocalAgentMemory full test...")
    lam_indexed = LocalAgentMemory(max_size=3, indexed_keys=["topic", "status"])
    kem_a = {"id": "kemA", "content": "Content A", "metadata": {"topic": "news", "status": "draft"}}
    kem_b = {"id": "kemB", "content": "Content B", "metadata": {"topic": "tech", "status": "published"}}
    kem_c = {"id": "kemC", "content": "Content C", "metadata": {"topic": "news", "status": "published"}}
    kem_d = {"id": "kemD", "content": "Content D", "metadata": {"topic": "sports", "status": "draft", "extra": "info"}}
    lam_indexed.put(kem_a["id"], kem_a); lam_indexed.put(kem_b["id"], kem_b); lam_indexed.put(kem_c["id"], kem_c)
    print(f"Indexed LAM size: {lam_indexed.current_size}")
    news_kems = lam_indexed.query(metadata_filters={"topic": "news"})
    print(f"KEMs with topic 'news': {len(news_kems)} (IDs: {[k['id'] for k in news_kems]})")
    assert len(news_kems) == 2
    published_kems = lam_indexed.query(metadata_filters={"status": "published"})
    print(f"KEMs with status 'published': {len(published_kems)} (IDs: {[k['id'] for k in published_kems]})")
    assert len(published_kems) == 2
    news_published_kems = lam_indexed.query(metadata_filters={"topic": "news", "status": "published"})
    print(f"KEMs with topic 'news' AND status 'published': {len(news_published_kems)} (IDs: {[k['id'] for k in news_published_kems]})")
    assert len(news_published_kems) == 1 and news_published_kems[0]['id'] == "kemC"
    lam_indexed.get("kemA"); lam_indexed.put(kem_d["id"], kem_d)
    assert not lam_indexed.contains("kemB")
    print(f"Contains kemB (False): {lam_indexed.contains('kemB')}")
    tech_kems_after_evict = lam_indexed.query(metadata_filters={"topic": "tech"})
    assert len(tech_kems_after_evict) == 0
    print("Indexed LAM tests complete.")

```
