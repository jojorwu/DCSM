import unittest
from unittest.mock import MagicMock, patch, call
import typing
import asyncio # For new async tests if needed by SDK internals

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
from dcsm_agent_sdk_python.swm_client import SWMClient # Added for completeness
from dcsm_agent_sdk_python.kps_client import KPSClient # Added for new client
from dcsm_agent_sdk_python.sdk import AgentSDK
from dcsm_agent_sdk_python.config import DCSMClientSDKConfig # Import the new config class


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
            # Pass all required new args for GLMClient, even if defaults
            client = GLMClient(
                retry_jitter_fraction=0.1, # example default
                tls_enabled=False
            )
            client.connect()

            mock_response_from_server = MagicMock()
            mock_kem_proto = MagicMock()
            mock_response_from_server.kems = [mock_kem_proto]
            mock_response_from_server.next_page_token = "next_token_test"
            mock_stub.RetrieveKEMs.return_value = mock_response_from_server

            kems, next_token = client.retrieve_kems(ids_filter=["id1", "id2"], page_size=5, page_token="prev_token")

            self.assertTrue(mock_stub.RetrieveKEMs.called)
            args, kwargs = mock_stub.RetrieveKEMs.call_args
            called_request = args[0]

            self.assertIn("id1", called_request.query.ids)
            self.assertIn("id2", called_request.query.ids)
            self.assertEqual(called_request.page_size, 5)
            self.assertEqual(called_request.page_token, "prev_token")
            self.assertIsNotNone(kems)
            self.assertEqual(next_token, "next_token_test")


class TestAgentSDKExistingFunctionality(unittest.TestCase): # Renamed for clarity
    def setUp(self):
        # We will now mock where AgentSDK imports these clients from
        self.mock_glm_client_patch = patch('dcsm_agent_sdk_python.sdk.GLMClient')
        self.mock_swm_client_patch = patch('dcsm_agent_sdk_python.sdk.SWMClient')
        self.mock_kps_client_patch = patch('dcsm_agent_sdk_python.sdk.KPSClient') # Also mock KPSClient
        self.mock_local_memory_patch = patch('dcsm_agent_sdk_python.sdk.LocalAgentMemory')

        self.MockGLMClient = self.mock_glm_client_patch.start()
        self.MockSWMClient = self.mock_swm_client_patch.start()
        self.MockKPSClient = self.mock_kps_client_patch.start() # Start KPS mock
        self.MockLocalAgentMemory = self.mock_local_memory_patch.start()

        self.mock_glm_instance = self.MockGLMClient.return_value
        self.mock_swm_instance = self.MockSWMClient.return_value
        self.mock_kps_instance = self.MockKPSClient.return_value # Get KPS instance
        self.mock_lpa_instance = self.MockLocalAgentMemory.return_value

        # Default config for AgentSDK tests
        self.sdk_config = DCSMClientSDKConfig(connect_on_init=False) # Avoid auto-connect in setup

    def tearDown(self):
        self.mock_glm_client_patch.stop()
        self.mock_swm_client_patch.stop()
        self.mock_kps_client_patch.stop() # Stop KPS mock
        self.mock_local_memory_patch.stop()

    def test_sdk_init_uses_config(self):
        custom_config = DCSMClientSDKConfig(
            glm_host="myglm", glm_port=1111,
            swm_host="myswm", swm_port=2222,
            kps_host="mykps", kps_port=3333,
            lpa_max_size=50, lpa_indexed_keys=["key1"],
            connect_on_init=False # Important for this test not to call connect
        )
        sdk = AgentSDK(config=custom_config)

        self.MockGLMClient.assert_called_once_with(
            server_address="myglm:1111",
            retry_max_attempts=custom_config.retry_max_attempts,
            retry_initial_delay_s=custom_config.retry_initial_delay_s,
            retry_backoff_factor=custom_config.retry_backoff_factor,
            retry_jitter_fraction=custom_config.retry_jitter_fraction,
            tls_enabled=custom_config.tls_enabled,
            tls_ca_cert_path=custom_config.tls_ca_cert_path,
            tls_client_cert_path=custom_config.tls_client_cert_path,
            tls_client_key_path=custom_config.tls_client_key_path,
            tls_server_override_authority=custom_config.tls_server_override_authority
        )
        self.MockSWMClient.assert_called_once_with(
            server_address="myswm:2222",
            # ... similar retry and TLS args
            retry_max_attempts=custom_config.retry_max_attempts,
            retry_initial_delay_s=custom_config.retry_initial_delay_s,
            retry_backoff_factor=custom_config.retry_backoff_factor,
            retry_jitter_fraction=custom_config.retry_jitter_fraction,
            tls_enabled=custom_config.tls_enabled,
            tls_ca_cert_path=custom_config.tls_ca_cert_path,
            tls_client_cert_path=custom_config.tls_client_cert_path,
            tls_client_key_path=custom_config.tls_client_key_path,
            tls_server_override_authority=custom_config.tls_server_override_authority
        )
        self.MockKPSClient.assert_called_once_with(
            server_address="mykps:3333",
            # ... similar retry and TLS args
            retry_max_attempts=custom_config.retry_max_attempts,
            retry_initial_delay_s=custom_config.retry_initial_delay_s,
            retry_backoff_factor=custom_config.retry_backoff_factor,
            retry_jitter_fraction=custom_config.retry_jitter_fraction,
            tls_enabled=custom_config.tls_enabled,
            tls_ca_cert_path=custom_config.tls_ca_cert_path,
            tls_client_cert_path=custom_config.tls_client_cert_path,
            tls_client_key_path=custom_config.tls_client_key_path,
            tls_server_override_authority=custom_config.tls_server_override_authority
        )
        self.MockLocalAgentMemory.assert_called_once_with(max_size=50, indexed_keys=["key1"])
        self.mock_glm_instance.connect.assert_not_called() # Because connect_on_init=False
        self.mock_swm_instance.connect.assert_not_called()
        self.mock_kps_instance.connect.assert_not_called()


    def test_query_local_memory_calls_lpa_query(self):
        sdk = AgentSDK(config=self.sdk_config)
        sdk.query_local_memory(metadata_filters={"type": "A"})
        self.mock_lpa_instance.query.assert_called_once_with(metadata_filters={"type": "A"}, ids=None)

    def test_store_kems_updates_lpa_with_server_data(self):
        sdk = AgentSDK(config=self.sdk_config)
        kems_to_store = [create_kem_dict("id1", {"type": "A", "version": 1})]
        kem_from_server_dict = create_kem_dict("id1", {"type": "A", "version": 1, "server_field": True})
        self.mock_glm_instance.batch_store_kems.return_value = ([kem_from_server_dict], [], None)

        stored_kems, _, _ = sdk.store_kems(kems_to_store)

        self.mock_glm_instance.batch_store_kems.assert_called_once_with(kems_to_store)
        self.assertIsNotNone(stored_kems)
        self.assertEqual(len(stored_kems), 1)
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_server_dict)


    def test_get_kem_from_lpa(self):
        sdk = AgentSDK(config=self.sdk_config)
        kem_in_lpa = create_kem_dict("id1", {})
        self.mock_lpa_instance.get.return_value = kem_in_lpa

        result = sdk.get_kem("id1")
        self.assertEqual(result, kem_in_lpa)
        self.mock_lpa_instance.get.assert_called_once_with("id1")
        self.mock_glm_instance.retrieve_kems.assert_not_called()

    def test_get_kem_from_glm_when_not_in_lpa(self):
        sdk = AgentSDK(config=self.sdk_config)
        self.mock_lpa_instance.get.return_value = None
        kem_from_glm_dict = create_kem_dict("id1", {"source": "glm"})
        self.mock_glm_instance.retrieve_kems.return_value = ([kem_from_glm_dict], "mock_token_1")

        result = sdk.get_kem("id1")
        self.assertEqual(result, kem_from_glm_dict)
        self.mock_lpa_instance.get.assert_called_once_with("id1")
        self.mock_glm_instance.retrieve_kems.assert_called_once_with(ids_filter=['id1'], page_size=1)
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_glm_dict)

    def test_get_kem_force_remote(self):
        sdk = AgentSDK(config=self.sdk_config)
        kem_in_lpa = create_kem_dict("id1", {"source": "lpa"})
        self.mock_lpa_instance.get.return_value = kem_in_lpa # This should be ignored

        kem_from_glm = create_kem_dict("id1", {"source": "glm_forced"})
        self.mock_glm_instance.retrieve_kems.return_value = ([kem_from_glm], None)

        result = sdk.get_kem("id1", force_remote=True)
        self.assertEqual(result, kem_from_glm)
        self.mock_lpa_instance.get.assert_not_called() # LPA get should not be called
        self.mock_glm_instance.retrieve_kems.assert_called_once_with(ids_filter=['id1'], page_size=1)
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_glm)


# New test class for AgentSDK initialization and client properties
@patch('dcsm_agent_sdk_python.sdk.KPSClient')
@patch('dcsm_agent_sdk_python.sdk.SWMClient')
@patch('dcsm_agent_sdk_python.sdk.GLMClient')
class TestAgentSDKInitializationAndProperties(unittest.TestCase):

    def test_init_with_default_config_and_connect_on_init_true(self, MockGLMClient, MockSWMClient, MockKPSClient):
        mock_glm_instance = MockGLMClient.return_value
        mock_swm_instance = MockSWMClient.return_value
        mock_kps_instance = MockKPSClient.return_value

        sdk_config = DCSMClientSDKConfig(connect_on_init=True)
        sdk = AgentSDK(config=sdk_config)

        MockGLMClient.assert_called_once_with(
            server_address=sdk_config.glm_address,
            retry_max_attempts=sdk_config.retry_max_attempts,
            retry_initial_delay_s=sdk_config.retry_initial_delay_s,
            retry_backoff_factor=sdk_config.retry_backoff_factor,
            retry_jitter_fraction=sdk_config.retry_jitter_fraction,
            tls_enabled=sdk_config.tls_enabled,
            tls_ca_cert_path=sdk_config.tls_ca_cert_path,
            tls_client_cert_path=sdk_config.tls_client_cert_path,
            tls_client_key_path=sdk_config.tls_client_key_path,
            tls_server_override_authority=sdk_config.tls_server_override_authority
        )
        # ... similar asserts for MockSWMClient and MockKPSClient ...
        MockSWMClient.assert_called_with(server_address=sdk_config.swm_address, retry_max_attempts=sdk_config.retry_max_attempts, retry_initial_delay_s=sdk_config.retry_initial_delay_s, retry_backoff_factor=sdk_config.retry_backoff_factor, retry_jitter_fraction=sdk_config.retry_jitter_fraction, tls_enabled=sdk_config.tls_enabled, tls_ca_cert_path=sdk_config.tls_ca_cert_path, tls_client_cert_path=sdk_config.tls_client_cert_path, tls_client_key_path=sdk_config.tls_client_key_path, tls_server_override_authority=sdk_config.tls_server_override_authority)
        MockKPSClient.assert_called_with(server_address=sdk_config.kps_address, retry_max_attempts=sdk_config.retry_max_attempts, retry_initial_delay_s=sdk_config.retry_initial_delay_s, retry_backoff_factor=sdk_config.retry_backoff_factor, retry_jitter_fraction=sdk_config.retry_jitter_fraction, tls_enabled=sdk_config.tls_enabled, tls_ca_cert_path=sdk_config.tls_ca_cert_path, tls_client_cert_path=sdk_config.tls_client_cert_path, tls_client_key_path=sdk_config.tls_client_key_path, tls_server_override_authority=sdk_config.tls_server_override_authority)

        mock_glm_instance.connect.assert_called_once()
        mock_swm_instance.connect.assert_called_once()
        mock_kps_instance.connect.assert_called_once()

    def test_init_with_connect_on_init_false(self, MockGLMClient, MockSWMClient, MockKPSClient):
        mock_glm_instance = MockGLMClient.return_value
        mock_swm_instance = MockSWMClient.return_value
        mock_kps_instance = MockKPSClient.return_value

        sdk_config = DCSMClientSDKConfig(connect_on_init=False)
        sdk = AgentSDK(config=sdk_config)

        mock_glm_instance.connect.assert_not_called()
        mock_swm_instance.connect.assert_not_called()
        mock_kps_instance.connect.assert_not_called()

    def test_client_properties_ensure_connection(self, MockGLMClient, MockSWMClient, MockKPSClient):
        mock_glm_instance = MockGLMClient.return_value
        mock_swm_instance = MockSWMClient.return_value
        mock_kps_instance = MockKPSClient.return_value

        sdk_config = DCSMClientSDKConfig(connect_on_init=False)
        sdk = AgentSDK(config=sdk_config)

        self.assertEqual(sdk.glm, mock_glm_instance)
        mock_glm_instance._ensure_connected.assert_called_once()

        self.assertEqual(sdk.swm, mock_swm_instance)
        mock_swm_instance._ensure_connected.assert_called_once()

        self.assertEqual(sdk.kps, mock_kps_instance)
        mock_kps_instance._ensure_connected.assert_called_once()

    def test_client_properties_raise_if_not_configured(self, MockGLMClient, MockSWMClient, MockKPSClient):
        sdk_config = DCSMClientSDKConfig(glm_host=None, glm_port=0) # Effectively disable GLM
        sdk = AgentSDK(config=sdk_config)

        with self.assertRaisesRegex(RuntimeError, "GLM service not configured"):
            _ = sdk.glm

        self.assertIsNotNone(sdk.swm) # SWM and KPS use defaults from DCSMClientSDKConfig
        self.assertIsNotNone(sdk.kps)


    def test_close_method_closes_all_clients(self, MockGLMClient, MockSWMClient, MockKPSClient):
        mock_glm_instance = MockGLMClient.return_value
        mock_swm_instance = MockSWMClient.return_value
        mock_kps_instance = MockKPSClient.return_value

        sdk_config = DCSMClientSDKConfig()
        sdk = AgentSDK(config=sdk_config)
        sdk.close()

        if sdk_config.glm_address: mock_glm_instance.close.assert_called_once()
        else: mock_glm_instance.close.assert_not_called()

        if sdk_config.swm_address: mock_swm_instance.close.assert_called_once()
        else: mock_swm_instance.close.assert_not_called()

        if sdk_config.kps_address: mock_kps_instance.close.assert_called_once()
        else: mock_kps_instance.close.assert_not_called()

    def test_context_manager_closes_clients(self, MockGLMClient, MockSWMClient, MockKPSClient):
        mock_glm_instance = MockGLMClient.return_value
        mock_swm_instance = MockSWMClient.return_value
        mock_kps_instance = MockKPSClient.return_value

        sdk_config = DCSMClientSDKConfig()
        with AgentSDK(config=sdk_config) as sdk:
            self.assertIsNotNone(sdk.glm)

        if sdk_config.glm_address: mock_glm_instance.close.assert_called_once()
        if sdk_config.swm_address: mock_swm_instance.close.assert_called_once()
        if sdk_config.kps_address: mock_kps_instance.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
