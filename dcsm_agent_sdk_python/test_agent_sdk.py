import unittest
from unittest.mock import MagicMock, patch, call, ANY
import typing
import asyncio
import threading # For SWM event handler tests
import time # For SWM event handler tests

from dcsm_agent_sdk_python.local_memory import LocalAgentMemory, IndexedLRUCache
from dcsm_agent_sdk_python.glm_client import GLMClient
from dcsm_agent_sdk_python.swm_client import SWMClient
from dcsm_agent_sdk_python.kps_client import KPSClient
from dcsm_agent_sdk_python.sdk import AgentSDK
from dcsm_agent_sdk_python.config import DCSMClientSDKConfig
from dcsm_agent_sdk_python.generated_grpc_code import swm_service_pb2 # For event types

def create_kem_dict(kem_id: str, metadata: dict, content: str = "content") -> dict:
    return {"id": kem_id, "content_type": "text/plain", "content": content, "metadata": metadata}

class TestLocalAgentMemoryWithIndexedCache(unittest.TestCase):
    # ... (tests for LocalAgentMemory remain unchanged, they are good) ...
    def test_init_with_indexed_keys(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type", "source"])
        self.assertIsInstance(lam.cache, IndexedLRUCache)
        self.assertEqual(lam.cache.maxsize, 5)
        self.assertEqual(lam.cache._indexed_keys, {"type", "source"})

    def test_init_without_indexed_keys(self):
        lam = LocalAgentMemory(max_size=3)
        self.assertIsInstance(lam.cache, IndexedLRUCache)
        self.assertEqual(lam.cache._indexed_keys, set())

    def test_put_get_delete_with_indexing(self):
        lam = LocalAgentMemory(max_size=2, indexed_keys=["type"])
        kem1 = create_kem_dict("kem1", {"type": "A", "tag": "T1"})
        lam.put("kem1", kem1)
        self.assertEqual(lam.get("kem1"), kem1)
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
        self.assertEqual(len(results), 2); self.assertIn(kem1, results); self.assertIn(kem3, results)

    def test_query_lpa_unindexed_field(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type"])
        kem1 = create_kem_dict("kem1", {"type": "doc", "source": "web"})
        kem2 = create_kem_dict("kem2", {"type": "msg", "source": "chat"})
        kem3 = create_kem_dict("kem3", {"type": "doc", "source": "web"})
        lam.put("kem1", kem1); lam.put("kem2", kem2); lam.put("kem3", kem3)
        results = lam.query(metadata_filters={"source": "web"})
        self.assertEqual(len(results), 2); self.assertIn(kem1, results); self.assertIn(kem3, results)

    def test_query_lpa_mixed_fields(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type"])
        kem1 = create_kem_dict("kem1", {"type": "doc", "source": "web"})
        kem2 = create_kem_dict("kem2", {"type": "doc", "source": "chat"})
        kem3 = create_kem_dict("kem3", {"type": "msg", "source": "web"})
        lam.put("kem1", kem1); lam.put("kem2", kem2); lam.put("kem3", kem3)
        results = lam.query(metadata_filters={"type": "doc", "source": "web"})
        self.assertEqual(len(results), 1); self.assertIn(kem1, results)

    def test_query_lpa_by_ids(self):
        lam = LocalAgentMemory(max_size=5, indexed_keys=["type"])
        kem1 = create_kem_dict("kem1", {"type": "doc"})
        kem2 = create_kem_dict("kem2", {"type": "msg"})
        kem3 = create_kem_dict("kem3", {"type": "doc"})
        lam.put("kem1", kem1); lam.put("kem2", kem2); lam.put("kem3", kem3)
        results = lam.query(ids=["kem1", "kem3"])
        self.assertEqual(len(results), 2); self.assertEqual({k['id'] for k in results}, {"kem1", "kem3"})

    def test_query_lpa_no_filters(self):
        lam = LocalAgentMemory(max_size=5)
        kem1 = create_kem_dict("kem1", {}); kem2 = create_kem_dict("kem2", {})
        lam.put("kem1", kem1); lam.put("kem2", kem2)
        self.assertEqual(len(lam.query()), 2)

# TestGLMClientRetrieve class removed as its purpose is better served in test_glm_client.py

class TestAgentSDK(unittest.TestCase):
    def setUp(self):
        self.mock_glm_client_patch = patch('dcsm_agent_sdk_python.sdk.GLMClient')
        self.mock_swm_client_patch = patch('dcsm_agent_sdk_python.sdk.SWMClient')
        self.mock_kps_client_patch = patch('dcsm_agent_sdk_python.sdk.KPSClient')
        self.mock_local_memory_patch = patch('dcsm_agent_sdk_python.sdk.LocalAgentMemory')

        self.MockGLMClient = self.mock_glm_client_patch.start()
        self.MockSWMClient = self.mock_swm_client_patch.start()
        self.MockKPSClient = self.mock_kps_client_patch.start()
        self.MockLocalAgentMemory = self.mock_local_memory_patch.start()

        self.mock_glm_instance = self.MockGLMClient.return_value
        self.mock_swm_instance = self.MockSWMClient.return_value
        self.mock_kps_instance = self.MockKPSClient.return_value
        self.mock_lpa_instance = self.MockLocalAgentMemory.return_value

        # Default config for AgentSDK tests, connect_on_init=False to better control mock calls
        self.sdk_config = DCSMClientSDKConfig(connect_on_init=False)

    def tearDown(self):
        self.mock_glm_client_patch.stop()
        self.mock_swm_client_patch.stop()
        self.mock_kps_client_patch.stop()
        self.mock_local_memory_patch.stop()

    def test_sdk_init_passes_all_configs_to_clients(self):
        """Verify AgentSDK.__init__ passes all relevant config values to client constructors."""
        sdk_config = DCSMClientSDKConfig(
            glm_host="myglm", glm_port=1111,
            swm_host="myswm", swm_port=2222,
            kps_host="mykps", kps_port=3333,
            # Test a few specific timeout and feature flags
            glm_retrieve_kems_timeout_s=12.0,
            swm_publish_kem_timeout_s=8.5,
            kps_process_raw_data_timeout_s=25.0,
            swm_event_max_retries=7,
            connect_on_init=False
        )
        sdk = AgentSDK(config=sdk_config)

        self.MockGLMClient.assert_called_once_with(
            server_address=sdk_config.glm_address,
            retry_max_attempts=sdk_config.retry_max_attempts,
            retry_initial_delay_s=sdk_config.retry_initial_delay_s,
            retry_backoff_factor=sdk_config.retry_backoff_factor,
            retry_jitter_fraction=sdk_config.retry_jitter_fraction,
            tls_enabled=sdk_config.tls_enabled,
            tls_ca_cert_path=sdk_config.tls_ca_cert_path,
            tls_client_cert_path=sdk_config.tls_client_cert_path,
            tls_client_key_path=sdk_config.tls_client_key_path,
            tls_server_override_authority=sdk_config.tls_server_override_authority,
            # Assert GLM-specific timeouts
            batch_store_kems_timeout_s=sdk_config.glm_batch_store_kems_timeout_s,
            retrieve_kems_timeout_s=12.0, # The one we overrode
            update_kem_timeout_s=sdk_config.glm_update_kem_timeout_s,
            delete_kem_timeout_s=sdk_config.glm_delete_kem_timeout_s
        )
        self.MockSWMClient.assert_called_once_with(
            server_address=sdk_config.swm_address,
            retry_max_attempts=sdk_config.retry_max_attempts,
            # ... (other common args) ...
            tls_server_override_authority=sdk_config.tls_server_override_authority,
            # Assert SWM-specific timeouts
            publish_kem_timeout_s=8.5, # The one we overrode
            query_swm_timeout_s=sdk_config.swm_query_swm_timeout_s,
            load_kems_timeout_s=sdk_config.swm_load_kems_timeout_s,
            lock_rpc_timeout_s=sdk_config.swm_lock_rpc_timeout_s,
            counter_rpc_timeout_s=sdk_config.swm_counter_rpc_timeout_s
        )
        self.MockKPSClient.assert_called_once_with(
            server_address=sdk_config.kps_address,
            retry_max_attempts=sdk_config.retry_max_attempts,
            # ... (other common args) ...
            tls_server_override_authority=sdk_config.tls_server_override_authority,
            # Assert KPS-specific timeouts
            process_raw_data_timeout_s=25.0 # The one we overrode
        )
        self.MockLocalAgentMemory.assert_called_once_with(
            max_size=sdk_config.lpa_max_size,
            indexed_keys=sdk_config.lpa_indexed_keys
        )
        self.mock_glm_instance.connect.assert_not_called()
        self.mock_swm_instance.connect.assert_not_called()
        self.mock_kps_instance.connect.assert_not_called()

    def test_get_kem_timeout_propagation(self):
        sdk = AgentSDK(config=self.sdk_config)
        self.mock_lpa_instance.get.return_value = None # Force remote call
        kem_from_glm = create_kem_dict("id1", {})
        self.mock_glm_instance.retrieve_kems.return_value = ([kem_from_glm], None)

        sdk.get_kem("id1", force_remote=True, timeout=0.5)
        self.mock_glm_instance.retrieve_kems.assert_called_once_with(
            ids_filter=['id1'], page_size=1, timeout=0.5 # Check timeout is passed
        )

    def test_close_stops_swm_event_handlers(self):
        sdk = AgentSDK(config=self.sdk_config)
        # Simulate an active SWM event handler
        mock_thread = MagicMock(spec=threading.Thread)
        mock_thread.is_alive.return_value = True
        mock_stop_event = MagicMock(spec=threading.Event)
        sdk._swm_event_handlers["test_agent"] = (mock_thread, mock_stop_event)

        sdk.close()

        mock_stop_event.set.assert_called_once()
        mock_thread.join.assert_called_once_with(timeout=5.0) # Default join timeout in close()
        self.assertEqual(sdk._swm_event_handlers, {}) # Should be cleared

    # ... (other AgentSDK tests like store_kems, update_kem, delete_kem, LPA interactions remain largely the same)
    # ... but they could also be enhanced to check if per-call timeouts are passed if those SDK methods get timeout params.
    def test_store_kems_updates_lpa_with_server_data(self):
        sdk = AgentSDK(config=self.sdk_config)
        kems_to_store = [create_kem_dict("id1", {"type": "A", "version": 1})]
        kem_from_server_dict = create_kem_dict("id1", {"type": "A", "version": 1, "server_field": True})
        self.mock_glm_instance.batch_store_kems.return_value = ([kem_from_server_dict], [], None)
        stored_kems, _, _ = sdk.store_kems(kems_to_store) # Timeout not passed here, uses GLMClient default
        self.mock_glm_instance.batch_store_kems.assert_called_once_with(kems_to_store)
        self.assertIsNotNone(stored_kems); self.assertEqual(len(stored_kems), 1)
        self.mock_lpa_instance.put.assert_called_once_with("id1", kem_from_server_dict)

    def test_process_data_for_kps(self):
        sdk = AgentSDK(config=self.sdk_config)
        mock_kps_response = kps_service_pb2.ProcessRawDataResponse(kem_id="kps_kem1", success=True)
        self.mock_kps_instance.process_raw_data.return_value = mock_kps_response

        response = sdk.process_data_for_kps("data1", "text/plain", b"content", {"meta":"val"}, timeout=15.0)

        self.assertEqual(response, mock_kps_response)
        self.mock_kps_instance.process_raw_data.assert_called_once_with(
            "data1", "text/plain", b"content", {"meta":"val"}, timeout=15.0
        )

    # Placeholder for more SWM method tests
    def test_publish_to_swm_batch_propagates_timeout(self):
        sdk = AgentSDK(config=self.sdk_config)
        kems_data = [create_kem_dict("swm_kem1", {})]
        # Mock the SWMClient's method directly for this specific test
        self.mock_swm_instance.publish_kem_to_swm.return_value = {"published_to_swm": True} # Assume success

        sdk.publish_kems_to_swm_batch(kems_data, persist_to_glm=True, rpc_timeout=7.0)

        self.mock_swm_instance.publish_kem_to_swm.assert_called_once_with(
            kems_data[0], True, timeout=7.0
        )

if __name__ == '__main__':
    unittest.main()
