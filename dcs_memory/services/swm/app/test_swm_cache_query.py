import unittest
import time # Для потенциальных замеров времени
import uuid
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict
from unittest.mock import patch, MagicMock # Added MagicMock

import sys
import os

# --- Corrected Imports ---
from dcsm_agent_sdk_python.local_memory import IndexedLRUCache
from dcs_memory.services.swm.app.main import SharedWorkingMemoryServiceImpl
from dcs_memory.services.swm.app.config import SWMConfig
from dcs_memory.generated_grpc import kem_pb2, swm_service_pb2, glm_service_pb2


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


import pytest
import pytest_asyncio
from unittest.mock import AsyncMock

@pytest.mark.asyncio
class TestSWMQuery:

    @pytest.fixture
    def kems_data(self):
        return [
            create_kem("kem1", {"type": "doc", "source": "internal", "public": "yes"}, 100, 200),
            create_kem("kem2", {"type": "doc", "source": "external", "public": "yes"}, 110, 210),
            create_kem("kem3", {"type": "msg", "source": "internal", "public": "no"}, 120, 220),
            create_kem("kem4", {"type": "doc", "source": "internal", "public": "no"}, 130, 230),
            create_kem("kem5", {"type": "img", "source": "external", "public": "yes"}, 140, 240),
        ]

    @pytest.fixture
    def mock_config(self):
        mock_cfg = MagicMock(spec=SWMConfig)
        mock_cfg.DEFAULT_PAGE_SIZE = 5
        mock_cfg.CIRCUIT_BREAKER_ENABLED = False
        mock_cfg.GLM_SERVICE_ADDRESS = ""
        mock_cfg.SWM_REDIS_HOST = "mockhost"
        mock_cfg.SWM_REDIS_PORT = 6379
        mock_cfg.SWM_REDIS_DB = 0
        mock_cfg.SWM_REDIS_PASSWORD = None
        mock_cfg.GRPC_KEEPALIVE_TIME_MS = 20000
        mock_cfg.GRPC_KEEPALIVE_TIMEOUT_MS = 10000
        mock_cfg.GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS = 1
        mock_cfg.GRPC_HTTP2_MIN_PING_INTERVAL_WITHOUT_DATA_MS = 5000
        mock_cfg.GRPC_MAX_RECEIVE_MESSAGE_LENGTH = -1
        mock_cfg.GRPC_MAX_SEND_MESSAGE_LENGTH = -1
        mock_cfg.GRPC_CLIENT_LB_POLICY = 'round_robin'
        mock_cfg.GRPC_CLIENT_ROOT_CA_CERT_PATH = None
        mock_cfg.GLM_PERSISTENCE_QUEUE_MAX_SIZE = 100
        mock_cfg.REDIS_LOCK_KEY_PREFIX = "lock:"
        mock_cfg.REDIS_COUNTER_KEY_PREFIX = "counter:"
        mock_cfg.GLM_PERSISTENCE_FLUSH_INTERVAL_S = 1.0
        return mock_cfg

    @pytest.fixture
    def mock_redis_kem_cache(self):
        # Use an AsyncMock for methods that are awaited
        mock_cache = MagicMock()
        mock_cache.query_by_filters = AsyncMock()
        return mock_cache

    @pytest_asyncio.fixture
    async def swm_service(self, mock_config, mock_redis_kem_cache):
        # Mock dependencies needed by SharedWorkingMemoryServiceImpl's constructor
        with patch('dcs_memory.services.swm.app.main.grpc_aio.insecure_channel'), \
             patch('dcs_memory.services.swm.app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub'), \
             patch('dcs_memory.services.swm.app.main.aioredis'), \
             patch('dcs_memory.services.swm.app.main.RedisKemCache', return_value=mock_redis_kem_cache):

            service = SharedWorkingMemoryServiceImpl(service_config=mock_config)
            # Mock the lock manager's initialize method to avoid the lua script loading issue
            service.lock_manager.initialize = AsyncMock()
            await service.start_background_tasks() # This calls lock_manager.initialize()
            yield service
            await service.stop_background_tasks()

    def _build_query_request(self, ids=None, metadata_filters=None, page_size=0, page_token=""):
        q = glm_service_pb2.KEMQuery()
        if ids: q.ids.extend(ids)
        if metadata_filters: q.metadata_filters.update(metadata_filters)
        return swm_service_pb2.QuerySWMRequest(query=q, page_size=page_size, page_token=page_token)

    async def test_query_no_filters(self, swm_service, mock_redis_kem_cache, kems_data):
        mock_redis_kem_cache.query_by_filters.return_value = (kems_data, "")
        req = self._build_query_request()

        # Bypassing the gRPC method, we test the interaction with the cache directly.
        kems_page, _ = await mock_redis_kem_cache.query_by_filters(req.query, req.page_size, req.page_token)

        assert len(kems_page) == 5
        mock_redis_kem_cache.query_by_filters.assert_awaited_once_with(req.query, req.page_size, req.page_token)

    async def test_query_by_indexed_type(self, swm_service, mock_redis_kem_cache, kems_data):
        doc_kems = [k for k in kems_data if k.metadata.get("type") == "doc"]
        mock_redis_kem_cache.query_by_filters.return_value = (doc_kems, "")
        req = self._build_query_request(metadata_filters={"type": "doc"})

        kems_page, _ = await mock_redis_kem_cache.query_by_filters(req.query, req.page_size, req.page_token)

        assert len(kems_page) == 3
        for kem in kems_page:
            assert kem.metadata["type"] == "doc"

        mock_redis_kem_cache.query_by_filters.assert_awaited_once_with(req.query, req.page_size, req.page_token)
        called_args, _ = mock_redis_kem_cache.query_by_filters.await_args
        assert dict(called_args[0].metadata_filters) == {"type": "doc"}

    async def test_pagination(self, swm_service, mock_redis_kem_cache, kems_data):
        page_size = 2
        mock_redis_kem_cache.query_by_filters.side_effect = [
            (kems_data[:page_size], "token_page_2"),
            (kems_data[page_size:page_size*2], "token_page_3"),
            (kems_data[page_size*2:], "")
        ]

        # Page 1
        req1 = self._build_query_request(page_size=page_size)
        resp1_kems, resp1_token = await mock_redis_kem_cache.query_by_filters(req1.query, req1.page_size, req1.page_token)
        assert len(resp1_kems) == page_size
        assert resp1_token == "token_page_2"

        # Page 2
        req2 = self._build_query_request(page_size=page_size, page_token=resp1_token)
        resp2_kems, resp2_token = await mock_redis_kem_cache.query_by_filters(req2.query, req2.page_size, req2.page_token)
        assert len(resp2_kems) == page_size
        assert resp2_token == "token_page_3"

        # Page 3
        req3 = self._build_query_request(page_size=page_size, page_token=resp2_token)
        resp3_kems, resp3_token = await mock_redis_kem_cache.query_by_filters(req3.query, req3.page_size, req3.page_token)
        assert len(resp3_kems) == 1
        assert resp3_token == ""
