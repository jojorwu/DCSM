import unittest
import os
from pydantic import ValidationError

from .config import DCSMClientSDKConfig

class TestDCSMClientSDKConfig(unittest.TestCase):

    def test_default_initialization(self):
        config = DCSMClientSDKConfig()
        # Existing checks
        self.assertEqual(config.glm_host, "localhost")
        self.assertEqual(config.glm_port, 50051)
        self.assertEqual(config.swm_host, "localhost")
        self.assertEqual(config.swm_port, 50052)
        self.assertEqual(config.kps_host, "localhost")
        self.assertEqual(config.kps_port, 50053)
        self.assertEqual(config.lpa_max_size, 100)
        self.assertEqual(config.lpa_indexed_keys, [])
        self.assertTrue(config.connect_on_init)
        self.assertEqual(config.retry_max_attempts, 3)
        self.assertEqual(config.retry_initial_delay_s, 1.0)
        self.assertEqual(config.retry_backoff_factor, 2.0)
        self.assertEqual(config.retry_jitter_fraction, 0.1)
        self.assertFalse(config.tls_enabled)
        self.assertIsNone(config.tls_ca_cert_path)
        self.assertIsNone(config.tls_client_cert_path)
        self.assertIsNone(config.tls_client_key_path)
        self.assertIsNone(config.tls_server_override_authority)

        # New timeout field checks (add all that were added to DCSMClientSDKConfig)
        self.assertEqual(config.glm_batch_store_kems_timeout_s, 20.0)
        self.assertEqual(config.glm_retrieve_kems_timeout_s, 10.0)
        self.assertEqual(config.glm_update_kem_timeout_s, 10.0)
        self.assertEqual(config.glm_delete_kem_timeout_s, 10.0)
        self.assertEqual(config.swm_publish_kem_timeout_s, 10.0)
        self.assertEqual(config.swm_query_swm_timeout_s, 10.0)
        self.assertEqual(config.swm_load_kems_timeout_s, 20.0)
        self.assertEqual(config.swm_lock_rpc_timeout_s, 5.0)
        self.assertEqual(config.swm_counter_rpc_timeout_s, 5.0)
        self.assertEqual(config.swm_query_swm_page_size_default, 50)
        self.assertEqual(config.kps_process_raw_data_timeout_s, 30.0)

        # SWM Event Handler Resilience Configs
        self.assertTrue(config.swm_event_handler_enabled)
        self.assertEqual(config.swm_event_max_retries, 5)
        self.assertEqual(config.swm_event_initial_retry_delay_s, 2.0)
        self.assertEqual(config.swm_event_max_retry_delay_s, 60.0)
        self.assertEqual(config.swm_event_backoff_factor, 2.0)
        self.assertEqual(config.swm_event_jitter_fraction, 0.2)
        self.assertEqual(config.swm_event_stream_normal_termination_reconnect_delay_s, 1.0)


    def test_initialization_with_custom_values(self):
        custom_data = {
            "glm_host": "remote-glm", "glm_port": 60001,
            "lpa_max_size": 250,
            "tls_enabled": True, "tls_ca_cert_path": "/path/to/ca.pem",
            "tls_client_cert_path": "/path/to/client.pem",
            "tls_client_key_path": "/path/to/client.key",
            "tls_server_override_authority": "override.server.com",
            "glm_retrieve_kems_timeout_s": 15.5,
            "swm_event_max_retries": 10,
            "swm_query_swm_page_size_default": 25
        }
        config = DCSMClientSDKConfig(**custom_data)

        self.assertEqual(config.glm_host, "remote-glm")
        self.assertEqual(config.glm_port, 60001)
        self.assertTrue(config.tls_enabled)
        self.assertEqual(config.tls_ca_cert_path, "/path/to/ca.pem")
        self.assertEqual(config.tls_client_cert_path, "/path/to/client.pem")
        self.assertEqual(config.tls_client_key_path, "/path/to/client.key")
        self.assertEqual(config.tls_server_override_authority, "override.server.com")
        self.assertEqual(config.glm_retrieve_kems_timeout_s, 15.5)
        self.assertEqual(config.swm_event_max_retries, 10)
        self.assertEqual(config.swm_query_swm_page_size_default, 25)


    def test_env_variable_loading(self):
        original_env = os.environ.copy()
        test_env_vars = {
            "DCSM_SDK_GLM_HOST": "env_glm_host",
            "DCSM_SDK_GLM_PORT": "50099",
            "DCSM_SDK_GLM_RETRIEVE_KEMS_TIMEOUT_S": "12.5",
            "DCSM_SDK_SWM_EVENT_MAX_RETRIES": "7",
            "DCSM_SDK_TLS_CLIENT_CERT_PATH": "/env/client.pem",
            "DCSM_SDK_SWM_QUERY_SWM_PAGE_SIZE_DEFAULT": "30"
        }
        os.environ.update(test_env_vars)

        try:
            config = DCSMClientSDKConfig()

            self.assertEqual(config.glm_host, "env_glm_host")
            self.assertEqual(config.glm_port, 50099)
            self.assertEqual(config.glm_retrieve_kems_timeout_s, 12.5)
            self.assertEqual(config.swm_event_max_retries, 7)
            self.assertEqual(config.tls_client_cert_path, "/env/client.pem")
            self.assertEqual(config.swm_query_swm_page_size_default, 30)
        finally:
            # Restore original environment
            for key in test_env_vars:
                if key in original_env:
                    os.environ[key] = original_env[key]
                else:
                    del os.environ[key]
            # Ensure any keys set that were not in original are removed
            current_env_keys = set(os.environ.keys())
            for key_to_remove in current_env_keys - set(original_env.keys()):
                del os.environ[key_to_remove]


    def test_address_properties_optional(self):
        config = DCSMClientSDKConfig()
        self.assertIsNotNone(config.glm_address)
        self.assertIsNotNone(config.swm_address)
        self.assertIsNotNone(config.kps_address)

    def test_invalid_port_type_in_env(self):
        original_glm_port = os.environ.get("DCSM_SDK_GLM_PORT")
        os.environ["DCSM_SDK_GLM_PORT"] = "not_an_int"
        with self.assertRaises(ValidationError):
            DCSMClientSDKConfig()
        if original_glm_port is not None:
            os.environ["DCSM_SDK_GLM_PORT"] = original_glm_port
        else:
            del os.environ["DCSM_SDK_GLM_PORT"]

if __name__ == '__main__':
    unittest.main()
