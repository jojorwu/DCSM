import unittest
import time # Для потенциальных замеров времени
import uuid
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict
from unittest.mock import patch, MagicMock # Added MagicMock

import sys
import os

# --- Remove sys.path manipulation ---
# current_script_path = os.path.abspath(__file__)
# app_dir = os.path.dirname(current_script_path)
# service_root_dir = os.path.dirname(app_dir)
# if service_root_dir not in sys.path:
#     sys.path.insert(0, service_root_dir)
# dcs_memory_root = os.path.abspath(os.path.join(service_root_dir, "../../"))
# if dcs_memory_root not in sys.path:
#     sys.path.insert(0, dcs_memory_root)

# --- Corrected Imports ---
from dcs_memory.services.swm.app.main import IndexedLRUCache, SharedWorkingMemoryServiceImpl
from dcs_memory.services.swm.app.config import SWMConfig
from dcs_memory.services.swm.generated_grpc import kem_pb2, swm_service_pb2, glm_service_pb2


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
    kem_dict_for_parse = kem_dict.copy()
    kem_dict_for_parse["created_at"] = ts_created.ToJsonString()
    kem_dict_for_parse["updated_at"] = ts_updated.ToJsonString()

    return ParseDict(kem_dict_for_parse, kem_pb2.KEM(), ignore_unknown_fields=True)

class TestIndexedLRUCache(unittest.TestCase): # This test class seems to be for a local cache, not directly related to SWM service logic with Redis
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
        kem3 = create_kem("id3", {"type": "A"}, 3, 3)

        cache["id1"] = kem1
        cache["id2"] = kem2
        cache["id3"] = kem3

        self.assertEqual(len(cache), 2)
        self.assertFalse("id1" in cache)
        self.assertTrue("id2" in cache)
        self.assertTrue("id3" in cache)

        self.assertEqual(cache.get_ids_by_metadata_filter("type", "A"), {"id3"})
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "B"), {"id2"})

    def test_update_indexed_metadata(self):
        cache = IndexedLRUCache(maxsize=2, indexed_keys=["type"])
        kem1 = create_kem("id1", {"type": "A"}, 1, 1)
        cache["id1"] = kem1
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "A"), {"id1"})
        self.assertTrue(not cache.get_ids_by_metadata_filter("type", "C"))


        kem1_updated = create_kem("id1", {"type": "C"}, 1, 5)
        cache["id1"] = kem1_updated

        self.assertEqual(len(cache), 1)
        self.assertTrue(not cache.get_ids_by_metadata_filter("type", "A"))
        self.assertEqual(cache.get_ids_by_metadata_filter("type", "C"), {"id1"})

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
        self.assertTrue("type" in cache._metadata_indexes)
        self.assertEqual(len(cache._metadata_indexes["type"]),0)


class TestSWMQuery(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock(spec=SWMConfig)
        self.mock_config.INDEXED_METADATA_KEYS = ["type", "source"]
        self.mock_config.GLM_SERVICE_ADDRESS = ""
        self.mock_config.DEFAULT_PAGE_SIZE = 5
        self.mock_config.REDIS_KEM_KEY_PREFIX = "swm_kem:"
        self.mock_config.REDIS_INDEX_META_KEY_PREFIX = "swm_idx:meta:"
        self.mock_config.REDIS_INDEX_DATE_CREATED_KEY = "swm_idx:date:created_at"
        self.mock_config.REDIS_INDEX_DATE_UPDATED_KEY = "swm_idx:date:updated_at"
        self.mock_config.REDIS_QUERY_TEMP_KEY_PREFIX = "swm_query_tmp:"
        self.mock_config.REDIS_TRANSACTION_MAX_RETRIES = 3
        self.mock_config.REDIS_TRANSACTION_RETRY_INITIAL_DELAY_S = 0.01
        self.mock_config.REDIS_TRANSACTION_RETRY_BACKOFF_FACTOR = 2.0
        self.mock_config.SWM_REDIS_HOST = "mockhost"
        self.mock_config.SWM_REDIS_PORT = 6379
        self.mock_config.SWM_REDIS_DB = 0
        self.mock_config.SWM_REDIS_PASSWORD = None
        self.mock_config.CIRCUIT_BREAKER_ENABLED = False


        self.mock_aioredis_patch = patch('dcs_memory.services.swm.app.main.aioredis')
        self.mock_aioredis = self.mock_aioredis_patch.start()
        self.mock_redis_client = MagicMock()
        self.mock_aioredis.from_url.return_value = self.mock_redis_client

        self.mock_redis_kem_cache_patch = patch('dcs_memory.services.swm.app.main.RedisKemCache')
        self.MockRedisKemCache = self.mock_redis_kem_cache_patch.start()
        self.mock_redis_kem_cache_instance = self.MockRedisKemCache.return_value
        self.mock_redis_kem_cache_instance.query_by_filters.return_value = ([], "")

        # Patch grpc.aio.insecure_channel used in SWMServiceImpl.__init__ for GLM client
        self.mock_grpc_aio_channel_patcher = patch('dcs_memory.services.swm.app.main.grpc_aio.insecure_channel')
        self.mock_grpc_aio_channel = self.mock_grpc_aio_channel_patcher.start()
        self.mock_glm_channel_instance = MagicMock()
        self.mock_grpc_aio_channel.return_value = self.mock_glm_channel_instance

        # Patch GLM Stub
        self.mock_glm_stub_patcher = patch('dcs_memory.services.swm.app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub')
        self.MockGLMStub = self.mock_glm_stub_patcher.start()
        self.mock_glm_stub_instance = self.MockGLMStub.return_value


        self.swm_service = SharedWorkingMemoryServiceImpl(config=self.mock_config)

        self.kems_data = [
            create_kem("kem1", {"type": "doc", "source": "internal", "public": "yes"}, 100, 200),
            create_kem("kem2", {"type": "doc", "source": "external", "public": "yes"}, 110, 210),
            create_kem("kem3", {"type": "msg", "source": "internal", "public": "no"}, 120, 220),
            create_kem("kem4", {"type": "doc", "source": "internal", "public": "no"}, 130, 230),
            create_kem("kem5", {"type": "img", "source": "external", "public": "yes"}, 140, 240),
        ]

    def tearDown(self):
        self.mock_aioredis_patch.stop()
        self.mock_redis_kem_cache_patch.stop()
        self.mock_grpc_aio_channel_patcher.stop()
        self.mock_glm_stub_patcher.stop()
        if "SWM_INDEXED_METADATA_KEYS" in os.environ:
            del os.environ["SWM_INDEXED_METADATA_KEYS"]


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
        self.mock_redis_kem_cache_instance.query_by_filters.return_value = (self.kems_data, "")
        req = self._build_query_request()
        resp = self.swm_service.QuerySWM(req, None) # Pass mock context if needed
        self.assertEqual(len(resp.kems), 5)
        self.mock_redis_kem_cache_instance.query_by_filters.assert_called_once()

    def test_query_by_indexed_type(self):
        doc_kems = [k for k in self.kems_data if k.metadata.get("type") == "doc"]
        self.mock_redis_kem_cache_instance.query_by_filters.return_value = (doc_kems, "")

        req = self._build_query_request(metadata_filters={"type": "doc"})
        resp = self.swm_service.QuerySWM(req, None)

        self.assertEqual(len(resp.kems), 3)
        for kem in resp.kems: self.assertEqual(kem.metadata["type"], "doc")
        called_args, _ = self.mock_redis_kem_cache_instance.query_by_filters.call_args
        self.assertEqual(dict(called_args[0].metadata_filters), {"type":"doc"})


    def test_query_by_indexed_source_and_type(self):
        filtered_kems = [k for k in self.kems_data if k.metadata.get("type") == "doc" and k.metadata.get("source") == "internal"]
        self.mock_redis_kem_cache_instance.query_by_filters.return_value = (filtered_kems, "")

        req = self._build_query_request(metadata_filters={"type": "doc", "source": "internal"})
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 2)
        for kem in resp.kems:
            self.assertEqual(kem.metadata["type"], "doc")
            self.assertEqual(kem.metadata["source"], "internal")

    def test_query_by_ids(self):
        target_ids = ["kem1", "kem3", "kem5"]
        filtered_kems = [k for k in self.kems_data if k.id in target_ids]
        self.mock_redis_kem_cache_instance.query_by_filters.return_value = (filtered_kems, "")

        req = self._build_query_request(ids=target_ids)
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), 3)
        retrieved_ids = {k.id for k in resp.kems}
        self.assertEqual(retrieved_ids, set(target_ids))

    def test_pagination(self):
        page_size = 2
        self.mock_redis_kem_cache_instance.query_by_filters.side_effect = [
            (self.kems_data[:page_size], "token_page_2"),
            (self.kems_data[page_size:page_size*2], "token_page_3"),
            (self.kems_data[page_size*2:], "")
        ]

        req = self._build_query_request(page_size=page_size)
        resp = self.swm_service.QuerySWM(req, None)
        self.assertEqual(len(resp.kems), page_size)
        self.assertEqual(resp.next_page_token, "token_page_2")

        req2 = self._build_query_request(page_size=page_size, page_token=resp.next_page_token)
        resp2 = self.swm_service.QuerySWM(req2, None)
        self.assertEqual(len(resp2.kems), page_size)
        self.assertEqual(resp2.next_page_token, "token_page_3")

        req3 = self._build_query_request(page_size=page_size, page_token=resp2.next_page_token)
        resp3 = self.swm_service.QuerySWM(req3, None)
        self.assertEqual(len(resp3.kems), 1)
        self.assertEqual(resp3.next_page_token, "")


if __name__ == '__main__':
    unittest.main()
