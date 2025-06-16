from cachetools import LRUCache
import typing

print("LocalAgentMemory.py loading...") # Для отладки

class LocalAgentMemory:
    def __init__(self, max_size=100):
        self.cache = LRUCache(maxsize=max_size)
        print("LocalAgentMemory initialized with max_size: {}".format(max_size))

    def get(self, kem_id: str) -> typing.Optional[dict]:
        kem_data = self.cache.get(kem_id)
        if kem_data is not None:
            print("KEM ID '{}' found in LAM.".format(kem_id))
            return kem_data
        else:
            print("KEM ID '{}' not found in LAM.".format(kem_id))
            return None

    def put(self, kem_id: str, kem_data: dict):
        if not isinstance(kem_data, dict):
            print("Error: kem_data for ID '{}' must be a dict.".format(kem_id))
            return

        data_to_store = kem_data.copy()
        data_to_store['id'] = kem_id # Ensure 'id' field matches the key

        self.cache[kem_id] = data_to_store
        print("KEM ID '{}' put/updated in LAM. Current LAM size: {}".format(kem_id, len(self.cache)))

    def delete(self, kem_id: str) -> bool:
        if kem_id in self.cache:
            del self.cache[kem_id]
            print("KEM ID '{}' deleted from LAM.".format(kem_id))
            return True
        else:
            print("KEM ID '{}' not found in LAM for deletion.".format(kem_id))
            return False

    def contains(self, kem_id: str) -> bool:
        return kem_id in self.cache

    def clear(self):
        self.cache.clear()
        print("LAM completely cleared.")

    @property
    def current_size(self) -> int:
        return len(self.cache)

if __name__ == '__main__':
    print("Running LocalAgentMemory full test...")
    lam = LocalAgentMemory(max_size=2)

    kem1_data = {"id": "kem001", "content": "Содержимое КЕП 1", "metadata": {"topic": "alpha"}}
    kem2_data = {"id": "kem002", "content": "Содержимое КЕП 2", "metadata": {"topic": "beta"}}
    kem3_data = {"id": "kem003", "content": "Содержимое КЕП 3", "metadata": {"topic": "gamma"}}

    print("\\n--- Adding KEMs ---")
    lam.put(kem1_data["id"], kem1_data)
    lam.put(kem2_data["id"], kem2_data)
    print("Cache size after adding kem001 & kem002: {}".format(lam.current_size))

    # At this point, cache is {kem001 (LRU), kem002 (MRU)}

    print("\\n--- Testing LRU (kem001 should be evicted by kem003) ---")
    lam.put(kem3_data["id"], kem3_data) # kem001 is evicted. Cache: {kem002 (LRU), kem003 (MRU)}
    print("Cache size after adding kem003: {}".format(lam.current_size))

    print("Contains kem001 (should be False): {}".format(lam.contains(kem1_data['id'])))
    ret_kem1 = lam.get(kem1_data['id'])
    print("Get kem001 (should be None): {}".format(ret_kem1))

    print("Contains kem002 (should be True): {}".format(lam.contains(kem2_data['id'])))
    print("Contains kem003 (should be True): {}".format(lam.contains(kem3_data['id'])))

    # Access kem002 to make it MRU. Cache: {kem003 (LRU), kem002 (MRU)}
    print("\\n--- Accessing kem002 to make it MRU ---")
    retrieved_kem2_early = lam.get(kem2_data["id"])
    print("Retrieved KEM kem002: {}".format(retrieved_kem2_early))

    # Add kem004, kem003 (the LRU) should be evicted. Cache: {kem002 (LRU), kem004 (MRU)}
    print("\\n--- Adding kem004, should evict kem003 ---")
    kem4_data = {"id": "kem004", "content": "Содержимое КЕП 4", "metadata": {"topic": "delta"}}
    lam.put(kem4_data["id"], kem4_data)
    print("Cache size after adding kem004: {}".format(lam.current_size))
    print("Contains kem002 (should be True): {}".format(lam.contains(kem2_data['id'])))
    print("Contains kem003 (should be False): {}".format(lam.contains(kem3_data['id'])))
    print("Contains kem004 (should be True): {}".format(lam.contains(kem4_data['id'])))

    print("\\n--- Retrieving data ---")
    retrieved_kem2 = lam.get(kem2_data["id"]) # Accessing kem002 makes it MRU. Cache: {kem004(LRU), kem002(MRU)}
    print("Retrieved KEM kem002: {}".format(retrieved_kem2))
    retrieved_kem4 = lam.get(kem4_data["id"]) # Accessing kem004 makes it MRU. Cache: {kem002(LRU), kem004(MRU)}
    print("Retrieved KEM kem004: {}".format(retrieved_kem4))

    print("\\n--- Deleting data ---")
    lam.delete(kem4_data["id"]) # Cache: {kem002 (MRU)}
    print("Contains kem004 after deletion: {}".format(lam.contains(kem4_data['id'])))
    print("Cache size after deleting kem004: {}".format(lam.current_size))

    print("\\n--- Clearing cache ---")
    lam.clear()
    print("Cache size after clearing: {}".format(lam.current_size))

    print("\\n--- Test put with id mismatch (should be overridden by key) ---")
    lam.put("kem005", {"id": "kem_wrong_id", "content": "test"})
    retrieved_kem5 = lam.get("kem005")
    if retrieved_kem5:
        print("Retrieved KEM kem005: {}".format(retrieved_kem5))
        assert retrieved_kem5['id'] == 'kem005', "ID in retrieved KEM does not match the key!"
    else:
        print("KEM kem005 not found, which is unexpected here.")
    print("Full test complete.")
