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
    @patch('dcsm_agent_sdk_python.glm_client.grpc')
    def test_retrieve_kems_with_ids_filter(self, mock_grpc):
        mock_stub = MagicMock()
        mock_channel = MagicMock()
        mock_grpc.insecure_channel.return_value = mock_channel

        # Мокаем GlobalLongTermMemoryStub из правильного сгенерированного модуля
        with patch('dcsm_agent_sdk_python.generated_grpc_code.glm_service_pb2_grpc.GlobalLongTermMemoryStub', return_value=mock_stub) as MockStubConst:
            client = GLMClient()
            client.connect()

            mock_response_from_server = MagicMock()
            # Предположим, сервер вернул один KEM
            mock_kem_proto = MagicMock() # Это должен быть объект типа kem_pb2.KEM
            # Чтобы _kem_proto_to_dict работал, нам нужен объект с полями или мокнуть _kem_proto_to_dict
            # Проще мокнуть _kem_proto_to_dict или создать реальный kem_pb2.KEM, если это просто.
            # Для этого теста важнее проверить, что stub.RetrieveKEMs вызывается с правильным запросом.

            mock_response_from_server.kems = [mock_kem_proto] # Список из одного мок-КЕМа
            mock_response_from_server.next_page_token = "next_token_test"
            mock_stub.RetrieveKEMs.return_value = mock_response_from_server

            # Мы не будем проверять результат _kem_proto_to_dict здесь, а только вызов RetrieveKEMs
            # Поэтому нет нужды мокать _kem_proto_to_dict
            kems, next_token = client.retrieve_kems(ids_filter=["id1", "id2"], page_size=5, page_token="prev_token")

            self.assertTrue(mock_stub.RetrieveKEMs.called)
            args, kwargs = mock_stub.RetrieveKEMs.call_args
            called_request = args[0] # это RetrieveKEMsRequest

            self.assertIn("id1", called_request.query.ids)
            self.assertIn("id2", called_request.query.ids)
            self.assertEqual(called_request.page_size, 5)
            self.assertEqual(called_request.page_token, "prev_token")
            self.assertIsNotNone(kems) # Проверяем, что kems не None
            self.assertEqual(next_token, "next_token_test")


class TestAgentSDK(unittest.TestCase):
    def setUp(self):
        # Мокаем GLMClient и LocalAgentMemory для изоляции тестов AgentSDK
        self.mock_glm_client_patch = patch('dcsm_agent_sdk_python.sdk.GLMClient')
        self.mock_local_memory_patch = patch('dcsm_agent_sdk_python.sdk.LocalAgentMemory')

        self.MockGLMClient = self.mock_glm_client_patch.start()
        self.MockLocalAgentMemory = self.mock_local_memory_patch.start()

        self.mock_glm_instance = self.MockGLMClient.return_value
        self.mock_glm_instance = self.MockGLMClient.return_value
        self.mock_lpa_instance = self.MockLocalAgentMemory.return_value

    # _create_mock_retrieve_kems_side_effect больше не нужен, так как мокируем return_value напрямую
    # def _create_mock_retrieve_kems_side_effect(self, kem_to_return: typing.Optional[dict]):
    #     """Хелпер для создания side_effect функции для retrieve_kems."""
    #     def mock_func(*args, **kwargs):
    #         if kem_to_return and kwargs.get('ids_filter') == [kem_to_return['id']]:
    #             return ([kem_to_return.copy()], None)
    #         return ([], None)
    #     return mock_func

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
        # AgentSDK.store_kems теперь вызывает glm_client.batch_store_kems.
        # Мок должен соответствовать возвращаемому значению batch_store_kems:
        # (list_of_successfully_stored_kems_dicts, list_of_failed_references, error_message_str)
        # Для этого теста (старая логика без явного refresh), предположим, что сервер вернул
        # словари, идентичные отправленным, если ID совпадают.
        self.mock_glm_instance.batch_store_kems.return_value = ([kems_to_store[0]], [], None)

        sdk.store_kems(kems_to_store)

        self.mock_glm_instance.batch_store_kems.assert_called_once_with(kems_to_store)
        # retrieve_kems не должен вызываться, так как refresh_lpa_after_store удален и его логика встроена
        self.mock_glm_instance.retrieve_kems.assert_not_called()
        self.mock_lpa_instance.put.assert_called_once_with("id1", kems_to_store[0])

    # Тест test_store_kems_with_refresh_lpa удален/заменен на test_store_kems_updates_lpa_with_server_data
    def test_store_kems_updates_lpa_with_server_data(self):
        sdk = AgentSDK()
        kems_to_store = [create_kem_dict("id1", {"type": "A", "version": 1})]

        # Мок ответа от glm_client.batch_store_kems
        # (successfully_stored_kems_as_dicts, failed_kem_references, overall_error_message)
        kem_from_server_dict = create_kem_dict("id1", {"type": "A", "version": 1, "server_field": True}) # Сервер мог добавить поля
        self.mock_glm_instance.batch_store_kems.return_value = ([kem_from_server_dict], [], None)

        # Вызываем store_kems (параметр refresh_lpa_after_store удален, теперь это поведение по умолчанию)
        stored_kems, _, _ = sdk.store_kems(kems_to_store)

        self.mock_glm_instance.batch_store_kems.assert_called_once_with(kems_to_store)
        self.assertIsNotNone(stored_kems)
        self.assertEqual(len(stored_kems), 1)
        self.assertEqual(stored_kems[0]['id'], "id1")
        # Проверяем, что ЛПА обновлена данными, возвращенными сервером
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_server_dict)


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
        kem_from_glm_dict = create_kem_dict("id1", {"source": "glm"})
        # GLMClient.retrieve_kems теперь возвращает кортеж (list_of_kems, next_page_token)
        self.mock_glm_instance.retrieve_kems.return_value = ([kem_from_glm_dict], "mock_token_1")

        result = sdk.get_kem("id1")
        self.assertEqual(result, kem_from_glm_dict)
        self.mock_lpa_instance.get.assert_called_once_with("id1")
        # Проверяем, что retrieve_kems вызывается с ids_filter
        self.mock_glm_instance.retrieve_kems.assert_called_once_with(ids_filter=['id1'], page_size=1)
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_glm_dict) # Исправлено здесь

    def test_get_kem_force_remote(self):
        sdk = AgentSDK()
        kem_in_lpa = create_kem_dict("id1", {"source": "lpa"})
        self.mock_lpa_instance.get.return_value = kem_in_lpa

        kem_from_glm = create_kem_dict("id1", {"source": "glm_forced"})
        # GLMClient.retrieve_kems теперь возвращает кортеж (list_of_kems, next_page_token)
        self.mock_glm_instance.retrieve_kems.return_value = ([kem_from_glm], None)


        result = sdk.get_kem("id1", force_remote=True)
        self.assertEqual(result, kem_from_glm)
        self.mock_lpa_instance.get.assert_not_called()
        # Проверяем, что retrieve_kems вызывается с ids_filter
        self.mock_glm_instance.retrieve_kems.assert_called_once_with(ids_filter=['id1'], page_size=1)
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_glm)


if __name__ == '__main__':
    unittest.main()
