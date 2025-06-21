import unittest
from unittest.mock import MagicMock, patch, call
import typing

# Добавляем путь для импорта модулей SDK
import sys
import os
# current_script_path = os.path.abspath(__file__) # /app/dcsm_agent_sdk_python/test_agent_sdk.py
# sdk_root_dir = os.path.dirname(current_script_path) # /app/dcsm_agent_sdk_python
# project_root_dir = os.path.dirname(sdk_root_dir) # /app
# if sdk_root_dir not in sys.path:
# sys.path.insert(0, sdk_root_dir)
# if project_root_dir not in sys.path: # Для импорта generated_grpc из sdk.py и glm_client.py
# sys.path.insert(0, project_root_dir)

# Предполагаем, что тесты запускаются из корня репозитория, и dcsm_agent_sdk_python в PYTHONPATH
# или используется как пакет
from dcsm_agent_sdk_python.local_memory import LocalAgentMemory, IndexedLRUCache
from dcsm_agent_sdk_python.glm_client import GLMClient
from dcsm_agent_sdk_python.sdk import AgentSDK

# Импорты для proto нужны, если мы будем создавать KEM объекты напрямую,
# но LocalAgentMemory и AgentSDK работают со словарями для KEM.
# GLMClient внутри конвертирует в/из proto.
# from dcsm_agent_sdk_python.generated_grpc_code import kem_pb2
# from dcsm_agent_sdk_python.generated_grpc_code import glm_service_pb2


def create_kem_dict(kem_id: str, metadata: dict, content: str = "content") -> dict:
    """Хелпер для создания KEM в виде словаря."""
    return {
        "id": kem_id,
        "content_type": "text/plain",
        "content": content, # В SDK content обычно строка
        "metadata": metadata,
        # created_at и updated_at обычно добавляются сервером или SDK при необходимости
    }

class TestLocalAgentMemoryWithIndexedCache(unittest.TestCase):
    def test_init_with_indexed_keys(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type", "source"])
        self.assertIsInstance(lam.cache, IndexedLRUCache)
        self.assertEqual(lam.cache.maxsize, 5)
        self.assertEqual(lam.cache._indexed_keys, {"type", "source"})

    def test_init_without_indexed_keys(self):
        lam = LocalAgentMemory(max_size=3) # indexed_keys=None по умолчанию
        self.assertIsInstance(lam.cache, IndexedLRUCache)
        self.assertEqual(lam.cache._indexed_keys, set()) # Должен быть пустым set

    def test_put_get_delete_with_indexing(self):
        lam = LocalAgentMemory(max_size=2, indexed_keys=["type"])
        kem1 = create_kem_dict("kem1", {"type": "A", "tag": "T1"})
        lam.put("kem1", kem1)

        self.assertEqual(lam.get("kem1"), kem1)
        # Проверка внутреннего состояния индекса (если это необходимо и возможно)
        # В IndexedLRUCache есть get_ids_by_metadata_filter
        self.assertEqual(lam.cache.get_ids_by_metadata_filter("type", "A"), {"kem1"})

        lam.delete("kem1")
        self.assertIsNone(lam.get("kem1"))
        self.assertEqual(lam.cache.get_ids_by_metadata_filter("type", "A"), set())

    def test_query_lpa_indexed_field(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type"])
        kem1 = create_kem_dict("kem1", {"type": "doc", "source": "web"})
        kem2 = create_kem_dict("kem2", {"type": "msg", "source": "chat"})
        kem3 = create_kem_dict("kem3", {"type": "doc", "source": "mail"})
        lam.put("kem1", kem1); lam.put("kem2", kem2); lam.put("kem3", kem3)

        results = lam.query(metadata_filters={"type": "doc"})
        self.assertEqual(len(results), 2)
        self.assertIn(kem1, results)
        self.assertIn(kem3, results)

    def test_query_lpa_unindexed_field(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type"]) # source не индексируется
        kem1 = create_kem_dict("kem1", {"type": "doc", "source": "web"})
        kem2 = create_kem_dict("kem2", {"type": "msg", "source": "chat"})
        kem3 = create_kem_dict("kem3", {"type": "doc", "source": "web"})
        lam.put("kem1", kem1); lam.put("kem2", kem2); lam.put("kem3", kem3)

        results = lam.query(metadata_filters={"source": "web"})
        self.assertEqual(len(results), 2)
        self.assertIn(kem1, results)
        self.assertIn(kem3, results)

    def test_query_lpa_mixed_fields(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type"]) # source не индексируется
        kem1 = create_kem_dict("kem1", {"type": "doc", "source": "web"})
        kem2 = create_kem_dict("kem2", {"type": "doc", "source": "chat"})
        kem3 = create_kem_dict("kem3", {"type": "msg", "source": "web"})
        lam.put("kem1", kem1); lam.put("kem2", kem2); lam.put("kem3", kem3)

        results = lam.query(metadata_filters={"type": "doc", "source": "web"})
        self.assertEqual(len(results), 1)
        self.assertIn(kem1, results)

    def test_query_lpa_by_ids(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type"])
        kem1 = create_kem_dict("kem1", {"type": "doc"})
        kem2 = create_kem_dict("kem2", {"type": "msg"})
        kem3 = create_kem_dict("kem3", {"type": "doc"})
        lam.put("kem1", kem1); lam.put("kem2", kem2); lam.put("kem3", kem3)

        results = lam.query(ids=["kem1", "kem3"])
        self.assertEqual(len(results), 2)
        retrieved_ids = {k['id'] for k in results}
        self.assertEqual(retrieved_ids, {"kem1", "kem3"})

    def test_query_lpa_no_filters(self):
        lam = LocalAgentMemory(max_size=5)
        kem1 = create_kem_dict("kem1", {})
        kem2 = create_kem_dict("kem2", {})
        lam.put("kem1", kem1); lam.put("kem2", kem2)

        results = lam.query()
        self.assertEqual(len(results), 2)


class TestGLMClientRetrieve(unittest.TestCase):
    # Этот тест удален/закомментирован, так как функциональность ids_filter была отменена
    # из-за несоответствия proto.
    # @patch('dcsm_agent_sdk_python.glm_client.grpc')
    # def test_retrieve_kems_with_ids_filter(self, mock_grpc):
    #     mock_stub = MagicMock()
    #     mock_channel = MagicMock()
    #     mock_grpc.insecure_channel.return_value = mock_channel
    #     with patch('dcsm_agent_sdk_python.generated_grpc_code.glm_service_pb2_grpc.GlobalLongTermMemoryStub', return_value=mock_stub):
    #         client = GLMClient()
    #         client.connect()
    #         mock_response = MagicMock()
    #         mock_kem_proto1 = MagicMock()
    #         mock_kem_proto1.id = "id1"
    #         mock_response.kems = [mock_kem_proto1]
    #         mock_stub.RetrieveKEMs.return_value = mock_response
    #         with patch.object(client, '_kem_proto_to_dict', side_effect=lambda x: {"id": x.id, "content": "MockContent"} if hasattr(x, 'id') else {}):
    #             client.retrieve_kems(ids_filter=["id1", "id2"], limit=5) # ids_filter здесь вызовет ошибку, если его нет в сигнатуре
    #         self.assertTrue(mock_stub.RetrieveKEMs.called)
    #         args, kwargs = mock_stub.RetrieveKEMs.call_args
    #         called_request = args[0]
    #         # Эти проверки не пройдут, если KEMQuery не имеет поля ids
    #         # self.assertIn("id1", called_request.query.ids)
    #         # self.assertIn("id2", called_request.query.ids)
    #         self.assertEqual(called_request.page_size, 5) # page_size есть в RetrieveKEMsRequest
    pass # Класс оставляем, но тест удаляем


class TestAgentSDK(unittest.TestCase):
    def setUp(self):
        # Мокаем GLMClient и LocalAgentMemory для изоляции тестов AgentSDK
        self.mock_glm_client_patch = patch('dcsm_agent_sdk_python.sdk.GLMClient')
        self.mock_local_memory_patch = patch('dcsm_agent_sdk_python.sdk.LocalAgentMemory')

        self.MockGLMClient = self.mock_glm_client_patch.start()
        self.MockLocalAgentMemory = self.mock_local_memory_patch.start()

        self.mock_glm_instance = self.MockGLMClient.return_value
        self.mock_lpa_instance = self.MockLocalAgentMemory.return_value

    def tearDown(self):
        self.mock_glm_client_patch.stop()
        self.mock_local_memory_patch.stop()

    def test_sdk_init(self):
        sdk = AgentSDK(glm_server_address="test_addr", lpa_max_size=50, lpa_indexed_keys=["key1"])
        self.MockGLMClient.assert_called_once_with(server_address="test_addr")
        self.MockLocalAgentMemory.assert_called_once_with(max_size=50, indexed_keys=["key1"])

    def test_query_local_memory_calls_lpa_query(self):
        sdk = AgentSDK()
        sdk.query_local_memory(metadata_filters={"type": "A"})
        self.mock_lpa_instance.query.assert_called_once_with(metadata_filters={"type": "A"}, ids=None)

    def test_store_kems_no_refresh_lpa(self):
        sdk = AgentSDK()
        kems_to_store = [create_kem_dict("id1", {"type": "A"})]
        self.mock_glm_instance.store_kems.return_value = (["id1"], 1, [])

        sdk.store_kems(kems_to_store) # Убран аргумент refresh_lpa_after_store

        self.mock_glm_instance.store_kems.assert_called_once_with(kems_to_store)
        self.mock_glm_instance.retrieve_kems.assert_not_called() # retrieve_kems не должен вызываться
        self.mock_lpa_instance.put.assert_called_once_with("id1", kems_to_store[0])

    # Тест test_store_kems_with_refresh_lpa удален, так как функциональность refresh_lpa_after_store
    # была отменена из-за отсутствия поля ids в KEMQuery текущей версии proto SDK.
    # def test_store_kems_with_refresh_lpa(self):
    #     sdk = AgentSDK()
    #     kems_to_store = [create_kem_dict("id1", {"type": "A"})]
    #     self.mock_glm_instance.store_kems.return_value = (["id1"], 1, [])
    #     retrieved_kem_from_server = create_kem_dict("id1", {"type": "A", "server_field": True})
    #     self.mock_glm_instance.retrieve_kems.return_value = [retrieved_kem_from_server]
    #     sdk.store_kems(kems_to_store, refresh_lpa_after_store=True)
    #     self.mock_glm_instance.store_kems.assert_called_once_with(kems_to_store)
    #     # Проверка вызова retrieve_kems должна соответствовать актуальной сигнатуре (без ids_filter)
    #     # Например, если бы он пытался получить каждый ID отдельно через metadata_filters:
    #     # self.mock_glm_instance.retrieve_kems.assert_called_once_with(metadata_filters={'id': 'id1'}, limit=1)
    #     # Но так как этого нет, а ids_filter убран, этот тест требует пересмотра или удаления.
    #     self.mock_lpa_instance.put.assert_called_once_with("id1", retrieved_kem_from_server)


    def test_get_kem_from_lpa(self):
        sdk = AgentSDK()
        kem_in_lpa = create_kem_dict("id1", {})
        self.mock_lpa_instance.get.return_value = kem_in_lpa

        result = sdk.get_kem("id1")
        self.assertEqual(result, kem_in_lpa)
        self.mock_lpa_instance.get.assert_called_once_with("id1")
        self.mock_glm_instance.retrieve_kems.assert_not_called()

    def test_get_kem_from_glm_when_not_in_lpa(self):
        sdk = AgentSDK()
        self.mock_lpa_instance.get.return_value = None # Не найдено в ЛПА
        kem_from_glm = create_kem_dict("id1", {"source": "glm"})
        self.mock_glm_instance.retrieve_kems.return_value = [kem_from_glm] # GLM возвращает список

        result = sdk.get_kem("id1")
        self.assertEqual(result, kem_from_glm)
        self.mock_lpa_instance.get.assert_called_once_with("id1")
        self.mock_glm_instance.retrieve_kems.assert_called_once_with(metadata_filters={'id': 'id1'}, limit=1)
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_glm)

    def test_get_kem_force_remote(self):
        sdk = AgentSDK()
        # Даже если есть в ЛПА, force_remote должен проигнорировать
        kem_in_lpa = create_kem_dict("id1", {"source": "lpa"})
        self.mock_lpa_instance.get.return_value = kem_in_lpa

        kem_from_glm = create_kem_dict("id1", {"source": "glm_forced"})
        self.mock_glm_instance.retrieve_kems.return_value = [kem_from_glm]

        result = sdk.get_kem("id1", force_remote=True)
        self.assertEqual(result, kem_from_glm)
        self.mock_lpa_instance.get.assert_not_called() # Не должен был вызываться get из ЛПА
        self.mock_glm_instance.retrieve_kems.assert_called_once_with(metadata_filters={'id': 'id1'}, limit=1)
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_glm)


if __name__ == '__main__':
    unittest.main()
