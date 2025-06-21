from cachetools import LRUCache, Cache # Добавляем Cache для наследования
import typing
import threading # Нужен для блокировки в IndexedLRUCache

print("LocalAgentMemory.py loading...") # Для отладки

# --- Копия IndexedLRUCache из SWM, адаптированная для словарей ---
class IndexedLRUCache(Cache):
    def __init__(self, maxsize, indexed_keys: typing.List[str]):
        super().__init__(maxsize)
        self._lru = LRUCache(maxsize=maxsize)
        self._indexed_keys = set(indexed_keys if indexed_keys else []) # Убедимся, что это set
        self._metadata_indexes: typing.Dict[str, typing.Dict[str, typing.Set[str]]] = {key: {} for key in self._indexed_keys}
        self._lock = threading.Lock()

    def _add_to_metadata_indexes(self, kem_data: dict):
        if not kem_data or not kem_data.get('id'): return
        kem_id = kem_data['id']
        metadata = kem_data.get('metadata', {})
        if not isinstance(metadata, dict): return # Метаданные должны быть словарем

        for meta_key in self._indexed_keys:
            if meta_key in metadata:
                value = metadata[meta_key]
                # Индексируем только строковые значения для простоты
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
                if isinstance(original_value, str): # Удаляем из индекса только если это была строка
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
                _evicted_id, evicted_kem_data = self._lru.popitem() # LRUCache.popitem() без last=False
                if evicted_kem_data:
                    self._remove_from_metadata_indexes(evicted_kem_data)

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
                return default
            else:
                raise KeyError(kem_id)

    def __len__(self) -> int:
        with self._lock:
            return len(self._lru)

    def __contains__(self, kem_id: str) -> bool:
        with self._lock:
            return kem_id in self._lru

    def values(self) -> typing.List[dict]: # Возвращает список словарей
        with self._lock:
            return list(self._lru.values())

    def items(self) -> typing.List[typing.Tuple[str, dict]]:
        with self._lock:
            return list(self._lru.items())

    def clear(self):
        with self._lock:
            self._lru.clear()
            self._metadata_indexes.clear() # Очищаем правильно
            self._metadata_indexes = {key: {} for key in self._indexed_keys} # Восстанавливаем структуру

    @property
    def maxsize(self):
        return self._lru.maxsize

    def get_ids_by_metadata_filter(self, meta_key: str, meta_value: str) -> typing.Set[str]:
        # Возвращает копию set, чтобы избежать изменения извне
        if meta_key not in self._indexed_keys:
            # Если ключ не предназначен для индексации, сообщаем об этом (или возвращаем None/пустое множество)
            # Для последовательности лучше вернуть пустое множество, как будто по этому фильтру ничего нет
            return set()
        with self._lock:
            return self._metadata_indexes.get(meta_key, {}).get(meta_value, set()).copy()

# --- Конец IndexedLRUCache ---


class LocalAgentMemory:
    def __init__(self, max_size=100, indexed_keys: typing.List[str] = None):
        # Используем IndexedLRUCache если есть indexed_keys, иначе обычный LRUCache для обратной совместимости
        # или если индексация не нужна. Для простоты пока всегда используем IndexedLRUCache.
        if indexed_keys is None:
            indexed_keys = [] # Пустой список, если не указано
        self.cache = IndexedLRUCache(maxsize=max_size, indexed_keys=indexed_keys)
        print("LocalAgentMemory initialized with max_size: {}, indexed_keys: {}".format(max_size, indexed_keys))

    def get(self, kem_id: str) -> typing.Optional[dict]:
        # IndexedLRUCache.get уже потокобезопасен
        kem_data = self.cache.get(kem_id)
        if kem_data is not None:
            # print("KEM ID '{}' found in LAM.".format(kem_id)) # Убрал print для чистоты
            return kem_data
        else:
            # print("KEM ID '{}' not found in LAM.".format(kem_id))
            return None

    def query(self, metadata_filters: typing.Optional[dict] = None, ids: typing.Optional[list[str]] = None) -> list[dict]:
        """
        Запрашивает КЕП из локальной памяти с фильтрацией.
        Поддерживает фильтрацию по ID и по индексированным/неиндексированным полям метаданных.
        Фильтры по датам здесь не реализованы для простоты, т.к. KEM в ЛПА хранятся как dict.
        """
        # print(f"LocalAgentMemory: query вызван с metadata_filters={metadata_filters}, ids={ids}")

        # Типы данных KEM в этом кэше - это словари Python, а не proto-объекты.
        # IndexedLRUCache._add_to_metadata_indexes и _remove_from_metadata_indexes ожидают dict.

        processed_kems: typing.List[dict] = []
        final_candidate_ids: typing.Optional[typing.Set[str]] = None

        indexed_metadata_queries: typing.Dict[str, str] = {}
        unindexed_metadata_queries: typing.Dict[str, str] = {}

        if metadata_filters:
            for key, value in metadata_filters.items():
                if key in self.cache._indexed_keys: # Используем _indexed_keys из экземпляра cache
                    # Убедимся, что значение для фильтра - строка, т.к. индексы строятся по строкам
                    if isinstance(value, str):
                        indexed_metadata_queries[key] = value
                    else:
                        # Если значение фильтра не строка, считаем его неиндексируемым для этого ключа
                        unindexed_metadata_queries[key] = value
                else:
                    unindexed_metadata_queries[key] = value

        if indexed_metadata_queries:
            # print(f"LAM: Применение индексированных фильтров метаданных: {indexed_metadata_queries}")
            intersected_ids: typing.Set[str] = set()
            first_indexed_filter = True
            for key, value in indexed_metadata_queries.items():
                ids_from_index = self.cache.get_ids_by_metadata_filter(key, value)
                if first_indexed_filter:
                    intersected_ids = ids_from_index
                    first_indexed_filter = False
                else:
                    intersected_ids.intersection_update(ids_from_index)
                if not intersected_ids: break

            if not intersected_ids:
                # print("LAM: Результат пуст после применения индексированных фильтров метаданных.")
                return []
            final_candidate_ids = intersected_ids
            # print(f"LAM: Кандидатские ID после индексированных фильтров: {len(final_candidate_ids)}")

        if final_candidate_ids is not None:
            for kem_id in final_candidate_ids:
                kem = self.cache.get(kem_id)
                if kem: processed_kems.append(kem)
            # print(f"LAM: Извлечено {len(processed_kems)} КЕП по ID из индексного поиска.")
        else:
            processed_kems = self.cache.values() # Это list из-за реализации IndexedLRUCache.values()
            # print(f"LAM: Индексные фильтры не применялись. Взято {len(processed_kems)} КЕП из кэша.")

        if ids:
            ids_set = set(ids)
            processed_kems = [kem for kem in processed_kems if kem['id'] in ids_set]
            # print(f"LAM: КЕП после фильтрации по query.ids: {len(processed_kems)}")

        if unindexed_metadata_queries:
            # print(f"LAM: Применение неиндексированных фильтров метаданных: {unindexed_metadata_queries}")
            temp_filtered_kems = []
            for kem in processed_kems:
                match = True
                kem_meta = kem.get('metadata', {})
                for key, value in unindexed_metadata_queries.items():
                    if kem_meta.get(key) != value: # Сравниваем значения как есть
                        match = False; break
                if match: temp_filtered_kems.append(kem)
            processed_kems = temp_filtered_kems
            # print(f"LAM: КЕП после неиндексированных фильтров метаданных: {len(processed_kems)}")

        # print(f"LAM: Query возвращает {len(processed_kems)} КЕП.")
        return processed_kems

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
