import asyncio
import threading # Import threading for the internal lock
from cachetools import LRUCache
import functools # For functools.wraps if creating decorators, or partial
from typing import TypeVar, Callable, Any, Optional

K = TypeVar('K')
V = TypeVar('V')

class AsyncLRUCache:
    """
    A wrapper around cachetools.LRUCache to make its operations asynchronous
    by running them in a separate thread using asyncio.to_thread.
    This wrapper also ensures thread-safety for the underlying synchronous
    LRUCache instance, as cachetools.LRUCache itself is not thread-safe.
    """
    def __init__(self, maxsize: int, getsizeof: Optional[Callable[[Any], Any]] = None):
        # Initialize the synchronous LRUCache
        self._cache = LRUCache(maxsize=maxsize, getsizeof=getsizeof)
        # Internal lock to ensure thread-safe access to self._cache
        self._sync_lock = threading.Lock()

    async def get(self, key: K, default: Optional[Any] = None) -> Optional[V]:
        """Asynchronously get an item from the cache."""
        def _sync_operation():
            with self._sync_lock:
                return self._cache.get(key, default)
        return await asyncio.to_thread(_sync_operation)

    async def set(self, key: K, value: V) -> None:
        """Asynchronously set an item in the cache."""
        def _sync_operation():
            with self._sync_lock:
                self._cache[key] = value # Use __setitem__
        await asyncio.to_thread(_sync_operation)

    async def pop(self, key: K, default: Optional[Any] = object()) -> Optional[V]:
        """Asynchronously pop an item from the cache.
        If key is not found, default is returned if given, otherwise KeyError is raised.
        Note: object() is a sentinel to distinguish from None as a default.
        """
        def _sync_operation():
            with self._sync_lock:
                if default is object(): # No default provided by user
                    return self._cache.pop(key)
                else:
                    return self._cache.pop(key, default)

        try:
            return await asyncio.to_thread(_sync_operation)
        except KeyError:
             # This should only happen if default was the sentinel and key was not found by _sync_operation
            if default is object():
                raise
            # This path should ideally not be reached if pop with default handles it,
            # but as a safeguard if _sync_operation raises KeyError even with default (which it shouldn't for cachetools.pop)
            return default


    async def __contains__(self, key: K) -> bool:
        """Asynchronously check if a key is in the cache."""
        def _sync_operation():
            with self._sync_lock:
                return key in self._cache
        return await asyncio.to_thread(_sync_operation)

    async def __len__(self) -> int:
        """Asynchronously get the number of items in the cache."""
        def _sync_operation():
            with self._sync_lock:
                return len(self._cache)
        return await asyncio.to_thread(_sync_operation)

    @property
    async def maxsize(self) -> int:
        """Asynchronously get the maximum size of the cache."""
        def _sync_operation():
            with self._sync_lock:
                return self._cache.maxsize
        return await asyncio.to_thread(_sync_operation)

    @property
    async def currsize(self) -> int:
        """Asynchronously get the current size of the cache."""
        def _sync_operation():
            with self._sync_lock:
                return self._cache.currsize
        return await asyncio.to_thread(_sync_operation)

    async def values(self) -> list[V]:
        """Asynchronously get all values in the cache.
        Note: This can be a potentially expensive operation for large caches.
        It also returns a list copy, so modifications to it won't affect the cache.
        """
        def _sync_operation():
            with self._sync_lock:
                return list(self._cache.values())
        return await asyncio.to_thread(_sync_operation)

    # SWM uses `self.swm_cache.get(kem_id)` which is covered by `get`.
    # SWM uses `self.swm_cache[kem.id] = kem` which is covered by `set`.
    # Note: direct __setitem__ like `cache[key] = value` won't be async.
    # Users of AsyncLRUCache must use `await cache.set(key, value)`.
    # SWM uses `kem_id in self.swm_cache` which is covered by `__contains__`.
    # SWM uses `self.swm_cache.pop(kem_id)` which is covered by `pop`.
    # SWM uses `len(self.swm_cache)` which is covered by `__len__`.
    # SWM uses `self.swm_cache.maxsize` which is covered by `maxsize` property.

    # Consider if other LRUCache methods are needed by SWM or for general use.
    # For now, these cover the identified usages.

if __name__ == '__main__':
    # Example Usage (for testing the wrapper)
    async def main_test():
        cache = AsyncLRUCache(maxsize=2)

        await cache.set('key1', 'value1')
        await cache.set('key2', 'value2')

        print(f"Value for key1: {await cache.get('key1')}")
        print(f"Is key2 in cache? {'key2' in await cache}") # Note: await cache for __contains__ doesn't work directly like this.
                                                            # Need to call it as: await cache.__contains__('key2') or use `in` with a wrapper if needed.
                                                            # For direct use with `in`, the class would need an `__await__` which is not what we want here.
                                                            # The `in` operator on an object doesn't implicitly await __contains__.
                                                            # So, direct `key in awaitable_cache_object` won't work as expected.
                                                            # We'd use: `await ('key2' in cache)` if __contains__ was not async,
                                                            # but since __contains__ is async, we'd do: `await cache.__contains__('key2')`

        print(f"Is key2 in cache? {await cache.__contains__('key2')}") # Correct way
        print(f"Length of cache: {await cache.__len__()}")

        await cache.set('key3', 'value3') # key1 should be evicted

        print(f"Value for key1 after eviction: {await cache.get('key1')}") # Expected: None
        print(f"Value for key2: {await cache.get('key2')}")
        print(f"Value for key3: {await cache.get('key3')}")
        print(f"Length of cache: {await cache.__len__()}")
        print(f"Maxsize: {await cache.maxsize}")
        print(f"Values: {await cache.values()}")

        popped_val = await cache.pop('key2')
        print(f"Popped key2: {popped_val}")
        print(f"Is key2 in cache now? {await cache.__contains__('key2')}")
        print(f"Length of cache: {await cache.__len__()}")

        # Test pop with default
        popped_non_existent = await cache.pop('key_non_existent', default="default_val")
        print(f"Popped non_existent with default: {popped_non_existent}")

        # Test pop raising KeyError
        try:
            await cache.pop('key_non_existent_no_default')
        except KeyError as e:
            print(f"Correctly caught KeyError for pop: {e}")

    asyncio.run(main_test())
