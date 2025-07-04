import unittest
import os
from pydantic import ValidationError

from .config import DCSMClientSDKConfig

class TestDCSMClientSDKConfig(unittest.TestCase):

    def test_default_initialization(self):
        config = DCSMClientSDKConfig()
        self.assertEqual(config.glm_host, "localhost")
        self.assertEqual(config.glm_port, 50051)
        self.assertEqual(config.glm_address, "localhost:50051")

        self.assertEqual(config.swm_host, "localhost")
        self.assertEqual(config.swm_port, 50052)
        self.assertEqual(config.swm_address, "localhost:50052")

        self.assertEqual(config.kps_host, "localhost")
        self.assertEqual(config.kps_port, 50053)
        self.assertEqual(config.kps_address, "localhost:50053")

        self.assertEqual(config.lpa_max_size, 100)
        self.assertEqual(config.lpa_indexed_keys, [])
        self.assertTrue(config.connect_on_init)
        self.assertEqual(config.retry_max_attempts, 3)
        self.assertFalse(config.tls_enabled)
        self.assertIsNone(config.tls_ca_cert_path)

    def test_initialization_with_custom_values(self):
        custom_data = {
            "glm_host": "remote-glm", "glm_port": 60001,
            "swm_host": "remote-swm", "swm_port": 60002,
            "kps_host": "remote-kps", "kps_port": 60003,
            "lpa_max_size": 250,
            "lpa_indexed_keys": ["type", "status"],
            "connect_on_init": False,
            "retry_max_attempts": 5,
            "retry_initial_delay_s": 0.5,
            "tls_enabled": True,
            "tls_ca_cert_path": "/path/to/ca.pem"
        }
        config = DCSMClientSDKConfig(**custom_data)

        self.assertEqual(config.glm_host, "remote-glm")
        self.assertEqual(config.glm_port, 60001)
        self.assertEqual(config.glm_address, "remote-glm:60001")

        self.assertEqual(config.lpa_max_size, 250)
        self.assertEqual(config.lpa_indexed_keys, ["type", "status"])
        self.assertFalse(config.connect_on_init)
        self.assertEqual(config.retry_max_attempts, 5)
        self.assertEqual(config.retry_initial_delay_s, 0.5)
        self.assertTrue(config.tls_enabled)
        self.assertEqual(config.tls_ca_cert_path, "/path/to/ca.pem")

    def test_env_variable_loading(self):
        # Set environment variables
        os.environ["DCSM_SDK_GLM_HOST"] = "env_glm_host"
        os.environ["DCSM_SDK_GLM_PORT"] = "50099"
        os.environ["DCSM_SDK_RETRY_MAX_ATTEMPTS"] = "10"
        os.environ["DCSM_SDK_TLS_ENABLED"] = "true" # Pydantic handles string "true" to bool

        config = DCSMClientSDKConfig() # Reloads from env

        self.assertEqual(config.glm_host, "env_glm_host")
        self.assertEqual(config.glm_port, 50099) # Pydantic converts to int
        self.assertEqual(config.retry_max_attempts, 10)
        self.assertTrue(config.tls_enabled)

        # Clean up environment variables
        del os.environ["DCSM_SDK_GLM_HOST"]
        del os.environ["DCSM_SDK_GLM_PORT"]
        del os.environ["DCSM_SDK_RETRY_MAX_ATTEMPTS"]
        del os.environ["DCSM_SDK_TLS_ENABLED"]

    def test_address_properties_optional(self):
        # Test when host or port might be None (though current model makes them required with defaults)
        # If we made host/port Optional in DCSMClientSDKConfig:
        # config_no_glm = DCSMClientSDKConfig(glm_host=None)
        # self.assertIsNone(config_no_glm.glm_address)
        # For now, they always have defaults.
        config = DCSMClientSDKConfig()
        self.assertIsNotNone(config.glm_address)
        self.assertIsNotNone(config.swm_address)
        self.assertIsNotNone(config.kps_address)

    def test_invalid_port_type_in_env(self):
        os.environ["DCSM_SDK_GLM_PORT"] = "not_an_int"
        with self.assertRaises(ValidationError): # Pydantic v2 raises ValidationError
            DCSMClientSDKConfig()
        del os.environ["DCSM_SDK_GLM_PORT"]


if __name__ == '__main__':
    unittest.main()
```
