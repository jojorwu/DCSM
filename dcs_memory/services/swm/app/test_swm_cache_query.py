import unittest
import time # Для потенциальных замеров времени
import uuid
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict

# Добавляем service_root_dir в sys.path, как это сделано в main.py SWM
import sys
import os
current_script_path = os.path.abspath(__file__)
app_dir_test = os.path.dirname(current_script_path) # /app/dcs_memory/services/swm/app
service_root_dir_test = os.path.dirname(app_dir_test) # /app/dcs_memory/services/swm

if service_root_dir_test not in sys.path:
    sys.path.insert(0, service_root_dir_test) # Добавляем /app/dcs_memory/services/swm

# Теперь импортируем нужные классы из main.py и сгенерированные proto
from app.main import IndexedLRUCache, SharedWorkingMemoryServiceImpl
# Относительный импорт app.main сработает, так как мы добавили service_root_dir_test,
# и Python будет искать app.main внутри dcs_memory.services.swm.

from generated_grpc import kem_pb2
from generated_grpc import swm_service_pb2
# glm_service_pb2 нужен для KEMQuery, который используется в swm_service_pb2.QuerySWMRequest
from generated_grpc import glm_service_pb2


def create_kem(kem_id: str, metadata: dict, created_sec: int, updated_sec: int, content: str = "content") -> kem_pb2.KEM:
    """Хелпер для создания KEM объекта."""
    ts_created = Timestamp()
    ts_created.seconds = created_sec
    ts_updated = Timestamp()
    ts_updated.seconds = updated_sec

    kem_dict = {
        "id": kem_id,
        "content_type": "text/plain",
        "content": content.encode('utf-8'),
        "metadata": metadata,
        "created_at": ts_created,
        "updated_at": ts_updated
    }
    # ParseDict напрямую не работает с Timestamp объектами, их нужно преобразовать в строки или использовать MessageToDict
    # Проще передать строки в ParseDict для временных меток
    kem_dict_for_parse = kem_dict.copy()
    kem_dict_for_parse["created_at"] = ts_created.ToJsonString()
    kem_dict_for_parse["updated_at"] = ts_updated.ToJsonString()

    return ParseDict(kem_dict_for_parse, kem_pb2.KEM(), ignore_unknown_fields=True)

class TestIndexedLRUCache(unittest.TestCase):
    def test_simple_set_get_len_contains(self):
        cache = IndexedLRUCache(maxsize=2, indexed_keys=["type"])
        self.assertEqual(len(cache), 0)

        kem1 = create_kem("id1", {"type": "A"}, 1, 1)
        cache["id1"] = kem1
        self.assertEqual(len(cache), 1)
        self.assertTrue("id1" in cache)
        self.assertEqual(cache["id1"].metadata["type"], "A")
        self.assertEqual(cache.get("id1").id, "id1")

        kem2 = create_kem("id2", {"type": "B"}, 2, 2)
        cache["id2"] = kem2
        self.assertEqual(len(cache), 2)
        self.assertTrue("id2" in cache)

        self.assertIsNotNone(cache.get_ids_by_metadata_filter("type", "A"))
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "A"), {"id1"})
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "B"), {"id2"})

    def test_lru_eviction(self):
        cache = IndexedLRUCache(maxsize=2, indexed_keys=["type"])
        kem1 = create_kem("id1", {"type": "A"}, 1, 1)
        kem2 = create_kem("id2", {"type": "B"}, 2, 2)
        kem3 = create_kem("id3", {"type": "A"}, 3, 3) # Тоже тип А

        cache["id1"] = kem1
        cache["id2"] = kem2
        cache["id3"] = kem3 # id1 должен вытесниться

        self.assertEqual(len(cache), 2)
        self.assertFalse("id1" in cache)
        self.assertTrue("id2" in cache)
        self.assertTrue("id3" in cache)

        # Проверка индексов после вытеснения
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "A"), {"id3"})
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "B"), {"id2"})

    def test_update_indexed_metadata(self):
        cache = IndexedLRUCache(maxsize=2, indexed_keys=["type"])
        kem1 = create_kem("id1", {"type": "A"}, 1, 1)
        cache["id1"] = kem1
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "A"), {"id1"})
        self.assertTrue(not cache.get_ids_by_metadata_filter("type", "C"))


        kem1_updated = create_kem("id1", {"type": "C"}, 1, 5) # Тот же ID, новый тип
        cache["id1"] = kem1_updated # Обновление

        self.assertEqual(len(cache), 1)
        self.assertTrue(not cache.get_ids_by_metadata_filter("type", "A")) # Должен исчезнуть из старого индекса
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "C"), {"id1"}) # Должен появиться в новом

    def test_delete_item(self):
        cache = IndexedLRUCache(maxsize=2, indexed_keys=["type"])
        kem1 = create_kem("id1", {"type": "A"}, 1, 1)
        cache["id1"] = kem1
        self.assertTrue("id1" in cache)
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "A"), {"id1"})

        del cache["id1"]
        self.assertFalse("id1" in cache)
        self.assertEqual(len(cache), 0)
        self.assertTrue(not cache.get_ids_by_metadata_filter("type", "A"))

    def test_pop_item(self):
        cache = IndexedLRUCache(maxsize=2, indexed_keys=["type"])
        kem1 = create_kem("id1", {"type": "A"}, 1, 1)
        cache["id1"] = kem1

        popped_kem = cache.pop("id1")
        self.assertEqual(popped_kem.id, "id1")
        self.assertFalse("id1" in cache)
        self.assertEqual(len(cache), 0)
        self.assertTrue(not cache.get_ids_by_metadata_filter("type", "A"))

    def test_clear_cache(self):
        cache = IndexedLRUCache(maxsize=2, indexed_keys=["type", "source"])
        cache["id1"] = create_kem("id1", {"type": "A", "source":"X"}, 1,1)
        cache["id2"] = create_kem("id2", {"type": "B", "source":"Y"}, 1,1)
        self.assertEqual(len(cache), 2)
        self.assertTrue(len(cache.get_ids_by_metadata_filter("type","A")) > 0)

        cache.clear()
        self.assertEqual(len(cache), 0)
        self.assertFalse(cache.get_ids_by_metadata_filter("type","A"))
        self.assertTrue("type" in cache._metadata_indexes) # Структура индекса должна остаться
        self.assertEqual(len(cache._metadata_indexes["type"]),0)


class TestSWMQuery(unittest.TestCase):
    def setUp(self):
        # Создаем экземпляр сервисера SWM для каждого теста
        # GLM Stub не будет использоваться в этих тестах, так как мы фокусируемся на QuerySWM
        # и его взаимодействии с кэшем.
        # Установим SWM_INDEXED_METADATA_KEYS_CONFIG для тестов
        os.environ["SWM_INDEXED_METADATA_KEYS"] = "type,source" # Индексируем 'type' и 'source'
        # Важно: SharedWorkingMemoryServiceImpl читает env var при создании экземпляра.
        # Чтобы это сработало в тестах, нужно либо мокать os.getenv, либо убедиться, что
        # SWM_INDEXED_METADATA_KEYS_CONFIG обновляется до создания экземпляра SWM.
        # Проще всего пересоздавать SWM_INDEXED_METADATA_KEYS_CONFIG перед каждым тестом, если нужно.
        # Однако, main.py SWM читает это один раз на уровне модуля.
        # Для тестов, мы можем передать indexed_keys прямо в конструктор IndexedLRUCache,
        # если бы конструктор SWM это позволял, или мокнуть SWM_INDEXED_METADATA_KEYS_CONFIG.
        # В данном случае, IndexedLRUCache уже создан в __init__ SWM.
        # Проще будет создать SWM с нужными ключами для тестирования.

        # Чтобы тесты были независимы, мы можем переопределить SWM_INDEXED_METADATA_KEYS_CONFIG
        # перед созданием экземпляра SharedWorkingMemoryServiceImpl
        # Это не очень чисто, лучше бы SWM принимал это как параметр.
        # Пока что положимся на то, что os.environ сработает до импорта main SWM в тестах,
        # или же на то, что SWM_INDEXED_METADATA_KEYS_CONFIG будет прочитан при создании SWM.
        # main.SWM_INDEXED_METADATA_KEYS_CONFIG = ["type", "source"] # Это изменит глобальную переменную в модуле main

        self.swm_service = SharedWorkingMemoryServiceImpl()
        # Очистим кэш перед каждым тестом
        self.swm_service.swm_cache.clear()

        # Заполним кэш тестовыми данными
        self.kems_data = [
            create_kem("kem1", {"type": "doc", "source": "internal", "public": "yes"}, 100, 200),
            create_kem("kem2", {"type": "doc", "source": "external", "public": "yes"}, 110, 210),
            create_kem("kem3", {"type": "msg", "source": "internal", "public": "no"}, 120, 220),
            create_kem("kem4", {"type": "doc", "source": "internal", "public": "no"}, 130, 230),
            create_kem("kem5", {"type": "img", "source": "external", "public": "yes"}, 140, 240),
        ]
        for kem in self.kems_data:
            self.swm_service.swm_cache[kem.id] = kem

        # Убедимся, что SharedWorkingMemoryServiceImpl использует обновленные SWM_INDEXED_METADATA_KEYS_CONFIG
        # Это можно сделать, проверив self.swm_service.swm_cache._indexed_keys
        # Однако, SWM_INDEXED_METADATA_KEYS_CONFIG читается на уровне модуля main.py.
        # Чтобы тесты были надежными, лучше мокать эту переменную или сам IndexedLRUCache.
        # Для простоты пока оставим так, предполагая, что os.environ сработает.
        # Если нет, тесты на индексный поиск могут не пройти как ожидалось.
        # ИЛИ, мы можем явно пересоздать swm_cache с нужными параметрами для теста:
        self.swm_service.swm_cache = IndexedLRUCache(
            maxsize=self.swm_service.swm_cache.maxsize,
            indexed_keys=["type", "source"] # Явно задаем для тестов
        )
        for kem in self.kems_data: # Перезаполняем с новым экземпляром кэша
            self.swm_service.swm_cache[kem.id] = kem


    def tearDown(self):
        # Можно сбросить переменную окружения, если она мешает другим тестам
        # if "SWM_INDEXED_METADATA_KEYS" in os.environ:
        #     del os.environ["SWM_INDEXED_METADATA_KEYS"]
        pass

    def _build_query_request(self, ids=None, metadata_filters=None,
                             created_start_s=None, created_end_s=None,
                             updated_start_s=None, updated_end_s=None,
                             page_size=0, page_token=""):

        q = glm_service_pb2.KEMQuery()
        if ids: q.ids.extend(ids)
        if metadata_filters: q.metadata_filters.update(metadata_filters)

        if created_start_s: q.created_at_start.seconds = created_start_s
        if created_end_s: q.created_at_end.seconds = created_end_s
        if updated_start_s: q.updated_at_start.seconds = updated_start_s
        if updated_end_s: q.updated_at_end.seconds = updated_end_s

        return swm_service_pb2.QuerySWMRequest(query=q, page_size=page_size, page_token=page_token)

    def test_query_no_filters(self):
        req = self._build_query_request()
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 5)

    def test_query_by_indexed_type(self):
        req = self._build_query_request(metadata_filters={"type": "doc"})
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 3)
        for kem in resp.kems: self.assertEqual(kem.metadata["type"], "doc")

    def test_query_by_indexed_source_and_type(self):
        req = self._build_query_request(metadata_filters={"type": "doc", "source": "internal"})
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 2)
        for kem in resp.kems:
            self.assertEqual(kem.metadata["type"], "doc")
            self.assertEqual(kem.metadata["source"], "internal")

    def test_query_by_indexed_and_unindexed_metadata(self):
        # 'type' - индексируется, 'public' - нет
        req = self._build_query_request(metadata_filters={"type": "doc", "public": "yes"})
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 2) # kem1, kem2
        for kem in resp.kems:
            self.assertEqual(kem.metadata["type"], "doc")
            self.assertEqual(kem.metadata["public"], "yes")

    def test_query_by_unindexed_metadata_only(self):
        req = self._build_query_request(metadata_filters={"public": "no"})
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 2) # kem3, kem4
        for kem in resp.kems: self.assertEqual(kem.metadata["public"], "no")

    def test_query_by_ids(self):
        req = self._build_query_request(ids=["kem1", "kem3", "kem5"])
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 3)
        retrieved_ids = {k.id for k in resp.kems}
        self.assertEqual(retrieved_ids, {"kem1", "kem3", "kem5"})

    def test_query_by_date_created(self):
        # kem1: 100, kem2: 110, kem3: 120, kem4: 130, kem5: 140
        req = self._build_query_request(created_start_s=110, created_end_s=130)
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 3) # kem2, kem3, kem4
        retrieved_ids = {k.id for k in resp.kems}
        self.assertEqual(retrieved_ids, {"kem2", "kem3", "kem4"})

    def test_query_by_date_updated(self):
        # kem1: 200, kem2: 210, kem3: 220, kem4: 230, kem5: 240
        req = self._build_query_request(updated_start_s=205, updated_end_s=225)
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 2) # kem2, kem3
        retrieved_ids = {k.id for k in resp.kems}
        self.assertEqual(retrieved_ids, {"kem2", "kem3"})

    def test_query_complex_filter(self):
        # type=doc (indexed), public=yes (unindexed), created_at >= 100, <=110
        # Ожидаем kem1, kem2 от type=doc.
        # Из них public=yes: kem1, kem2.
        # Из них created_at 100-110: kem1, kem2.
        req = self._build_query_request(
            metadata_filters={"type": "doc", "public": "yes"},
            created_start_s=100, created_end_s=110)
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 2)
        retrieved_ids = {k.id for k in resp.kems}
        self.assertEqual(retrieved_ids, {"kem1", "kem2"})

    def test_pagination(self):
        req = self._build_query_request(page_size=2)
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 2)
        self.assertIsNotNone(resp.next_page_token)
        self.assertTrue(int(resp.next_page_token) > 0)

        req2 = self._build_query_request(page_size=2, page_token=resp.next_page_token)
        resp2 = self.swm_service.QuerySWM(req2, None)
        self.assertEqual(len(resp2.kems), 2)
        self.assertIsNotNone(resp2.next_page_token)

        req3 = self._build_query_request(page_size=2, page_token=resp2.next_page_token)
        resp3 = self.swm_service.QuerySWM(req3, None)
        self.assertEqual(len(resp3.kems), 1) # Остался 1
        self.assertEqual(resp3.next_page_token, "") # Больше нет


if __name__ == '__main__':
    unittest.main()
