from cachetools import LRUCache, Cache
import typing
import threading

# print("LocalAgentMemory.py loading...") # For debugging import issues

class IndexedLRUCache(Cache): # Inherit from Cache for type hinting
    def __init__(self, maxsize: int, indexed_keys: typing.List[str],
                 on_evict_callback: typing.Optional[typing.Callable[[dict], None]] = None): # kem_data here is a dict
        super().__init__(maxsize)
        self._lru = LRUCache(maxsize=maxsize)
        self._indexed_keys = set(indexed_keys if indexed_keys else [])
        self._metadata_indexes: typing.Dict[str, typing.Dict[str, typing.Set[str]]] = {key: {} for key in self._indexed_keys}
        self._lock = threading.Lock() # Thread-safety for cache operations
        self._on_evict_callback = on_evict_callback # Callback for when items are evicted

    def _add_to_metadata_indexes(self, kem_data: dict):
        if not kem_data or not kem_data.get('id'): return
        kem_id = kem_data['id']
        metadata = kem_data.get('metadata', {})
        if not isinstance(metadata, dict): return # Metadata must be a dictionary

        for meta_key in self._indexed_keys:
            if meta_key in metadata:
                value = metadata[meta_key]
                # For simplicity, only index string values
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
                if isinstance(original_value, str): # Only remove from index if it was a string value
                    if meta_key in self._metadata_indexes and original_value in self._metadata_indexes[meta_key]:
                        self._metadata_indexes[meta_key][original_value].discard(kem_id)
                        if not self._metadata_indexes[meta_key][original_value]: # Clean up empty value set
                            del self._metadata_indexes[meta_key][original_value]
                        if not self._metadata_indexes[meta_key]: # Clean up empty key dict
                            del self._metadata_indexes[meta_key]

    def __setitem__(self, kem_id: str, kem_data: dict):
        with self._lock:
            evicted_kem_data = None
            if kem_id in self._lru: # Updating an existing item
                old_kem_data = self._lru[kem_id] # Get old version to remove from index
                self._remove_from_metadata_indexes(old_kem_data)
            elif len(self._lru) >= self._lru.maxsize: # Adding new, cache is full, eviction will occur
                _evicted_id, evicted_kem_data = self._lru.popitem() # popitem() removes and returns the LRU item
                if evicted_kem_data:
                    self._remove_from_metadata_indexes(evicted_kem_data)
                    if self._on_evict_callback:
                        try:
                            self._on_evict_callback(evicted_kem_data)
                        except Exception as e_cb:
                            # print(f"Error in LAM on_evict_callback for KEM ID '{_evicted_id}': {e_cb}")
                            pass # For now, just pass; logging could be added here

            self._lru[kem_id] = kem_data
            self._add_to_metadata_indexes(kem_data)

    def __getitem__(self, kem_id: str) -> dict:
        with self._lock:
            return self._lru[kem_id] # LRUCache updates access order

    def __delitem__(self, kem_id: str):
        with self._lock:
            if kem_id in self._lru:
                kem_to_remove = self._lru.pop(kem_id) # Use pop to get the value for index removal
                self._remove_from_metadata_indexes(kem_to_remove)
            else: # If key doesn't exist, raise KeyError like standard del
                raise KeyError(kem_id)

    def get(self, kem_id: str, default=None) -> typing.Optional[dict]:
        with self._lock:
            return self._lru.get(kem_id, default) # LRUCache's get updates access order

    def pop(self, kem_id: str, default=object()) -> dict: # object() as a unique marker for absence
        with self._lock:
            if kem_id in self._lru:
                kem_data = self._lru.pop(kem_id)
                self._remove_from_metadata_indexes(kem_data)
                return kem_data
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

    def values(self) -> typing.List[dict]: # Returns a list of dictionaries
        with self._lock:
            return list(self._lru.values())

    def items(self) -> typing.List[typing.Tuple[str, dict]]:
        with self._lock:
            return list(self._lru.items())

    def clear(self):
        with self._lock:
            self._lru.clear()
            self._metadata_indexes.clear()
            self._metadata_indexes = {key: {} for key in self._indexed_keys} # Restore structure

    @property
    def maxsize(self):
        return self._lru.maxsize

    def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Set[str]:
        # Returns a copy of the set to prevent external modification
        if meta_key not in self._indexed_keys:
            # If key is not designated for indexing, return an empty set
            return set()
        with self._lock:
            return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()


class LocalAgentMemory:
    def __init__(self, max_size: int = 100, indexed_keys: typing.Optional[typing.List[str]] = None):
        if indexed_keys is None:
            indexed_keys = []
        self.cache = IndexedLRUCache(maxsize=max_size, indexed_keys=indexed_keys)
        # print(f"LocalAgentMemory initialized with max_size: {max_size}, indexed_keys: {indexed_keys}") # Debug print

    def get(self, kem_id: str) -> typing.Optional[dict]:
        kem_data = self.cache.get(kem_id)
        return kem_data

    def query(self, metadata_filters: typing.Optional[dict] = None, ids: typing.Optional[list[str]] = None) -> list[dict]:
        """
        Queries KEMs from local memory (LAM) with filtering.
        Supports filtering by ID and by indexed/non-indexed metadata fields.
        Date filters are not implemented here for simplicity, as KEMs in LAM are stored as dicts.
        """
        processed_kems: typing.List[dict] = []
        final_candidate_ids: typing.Optional[typing.Set[str]] = None

        indexed_metadata_queries: typing.Dict[str, str] = {}
        unindexed_metadata_queries: typing.Dict[str, typing.Any] = {}

        if metadata_filters:
            for key, value in metadata_filters.items():
                if key in self.cache._indexed_keys:
                    if isinstance(value, str):
                        indexed_metadata_queries[key] = value
                    else:
                        unindexed_metadata_queries[key] = value
                else:
                    unindexed_metadata_queries[key] = value

        if indexed_metadata_queries:
            intersected_ids: typing.Optional[typing.Set[str]] = None
            for key, value in indexed_metadata_queries.items():
                ids_from_index = self.cache.get_ids_by_metadata_filter(key, value)
                if intersected_ids is None:
                    intersected_ids = ids_from_index
                else:
                    intersected_ids.intersection_update(ids_from_index)
                if not intersected_ids: break
            if not intersected_ids :
                return []
            final_candidate_ids = intersected_ids

        if final_candidate_ids is not None:
            for kem_id in final_candidate_ids:
                kem = self.cache.get(kem_id)
                if kem: processed_kems.append(kem)
        else:
            processed_kems = self.cache.values()

        if ids:
            ids_set = set(ids)
            processed_kems = [kem for kem in processed_kems if kem.get('id') in ids_set]

        if unindexed_metadata_queries:
            temp_filtered_kems = []
            for kem in processed_kems:
                match = True
                kem_meta = kem.get('metadata', {})
                for key, value in unindexed_metadata_queries.items():
                    if kem_meta.get(key) != value:
                        match = False; break
                if match: temp_filtered_kems.append(kem)
            processed_kems = temp_filtered_kems
        return processed_kems

    def put(self, kem_id: str, kem_data: dict):
        if not isinstance(kem_data, dict):
            # print(f"Error: kem_data for ID '{kem_id}' must be a dict.") # Debug print
            return
        data_to_store = kem_data.copy()
        data_to_store['id'] = kem_id
        self.cache[kem_id] = data_to_store
        # print(f"KEM ID '{kem_id}' put/updated in LAM. Current LAM size: {len(self.cache)}") # Debug print

    def delete(self, kem_id: str) -> bool:
        if kem_id in self.cache:
            del self.cache[kem_id]
            # print(f"KEM ID '{kem_id}' deleted from LAM.") # Debug print
            return True
        else:
            # print(f"KEM ID '{kem_id}' not found in LAM for deletion.") # Debug print
            return False

    def contains(self, kem_id: str) -> bool:
        return kem_id in self.cache

    def clear(self):
        self.cache.clear()
        # print("LAM completely cleared.") # Debug print

    @property
    def current_size(self) -> int:
        return len(self.cache)

if __name__ == '__main__':
    print("Running LocalAgentMemory full test...")
    lam_indexed = LocalAgentMemory(max_size=3, indexed_keys=["topic", "status"])

    kem_a = {"id": "kemA", "content": "Content A", "metadata": {"topic": "news", "status": "draft"}}
    kem_b = {"id": "kemB", "content": "Content B", "metadata": {"topic": "tech", "status": "published"}}
    kem_c = {"id": "kemC", "content": "Content C", "metadata": {"topic": "news", "status": "published"}}
    kem_d = {"id": "kemD", "content": "Content D", "metadata": {"topic": "sports", "status": "draft", "extra": "info"}}

    print("\n--- Adding KEMs to indexed LAM ---")
    lam_indexed.put(kem_a["id"], kem_a)
    lam_indexed.put(kem_b["id"], kem_b)
    lam_indexed.put(kem_c["id"], kem_c)
    print(f"Indexed LAM size: {lam_indexed.current_size}")

    print("\n--- Querying indexed LAM ---")
    news_kems = lam_indexed.query(metadata_filters={"topic": "news"})
    print(f"KEMs with topic 'news': {len(news_kems)} (IDs: {[k['id'] for k in news_kems]})")
    assert len(news_kems) == 2
    assert "kemA" in [k['id'] for k in news_kems] and "kemC" in [k['id'] for k in news_kems]

    published_kems = lam_indexed.query(metadata_filters={"status": "published"})
    print(f"KEMs with status 'published': {len(published_kems)} (IDs: {[k['id'] for k in published_kems]})")
    assert len(published_kems) == 2
    assert "kemB" in [k['id'] for k in published_kems] and "kemC" in [k['id'] for k in published_kems]

    news_published_kems = lam_indexed.query(metadata_filters={"topic": "news", "status": "published"})
    print(f"KEMs with topic 'news' AND status 'published': {len(news_published_kems)} (IDs: {[k['id'] for k in news_published_kems]})")
    assert len(news_published_kems) == 1
    assert news_published_kems[0]['id'] == "kemC"

    print("\n--- Testing LRU with indexed LAM (kemA should be evicted by kemD) ---")
    lam_indexed.get("kemA")
    lam_indexed.put(kem_d["id"], kem_d)
    print(f"Indexed LAM size after adding kemD: {lam_indexed.current_size}")
    assert not lam_indexed.contains("kemB")
    assert lam_indexed.contains("kemA")
    assert lam_indexed.contains("kemC")
    assert lam_indexed.contains("kemD")
    print(f"Contains kemB (should be False): {lam_indexed.contains('kemB')}")

    tech_kems_after_evict = lam_indexed.query(metadata_filters={"topic": "tech"})
    print(f"KEMs with topic 'tech' after eviction: {len(tech_kems_after_evict)}")
    assert len(tech_kems_after_evict) == 0

    print("\n--- Test original example from file (non-indexed) ---")
    lam_plain = LocalAgentMemory(max_size=2)
    kem1_data = {"id": "kem001", "content": "Content KEM 1", "metadata": {"topic": "alpha"}}
    kem2_data = {"id": "kem002", "content": "Content KEM 2", "metadata": {"topic": "beta"}}
    kem3_data = {"id": "kem003", "content": "Content KEM 3", "metadata": {"topic": "gamma"}}
    lam_plain.put(kem1_data["id"], kem1_data)
    lam_plain.put(kem2_data["id"], kem2_data)
    lam_plain.put(kem3_data["id"], kem3_data)
    assert not lam_plain.contains(kem1_data['id'])
    assert lam_plain.contains(kem2_data['id']) and lam_plain.contains(kem3_data['id'])
    print("Original example test passed.")
    print("Indexed LAM tests complete.")
    print("Full test complete.")

```
