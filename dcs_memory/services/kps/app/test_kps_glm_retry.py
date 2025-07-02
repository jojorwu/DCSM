import unittest
from unittest.mock import MagicMock, patch
import grpc
import time

# import sys # No longer needed
# import os # No longer needed

# --- Corrected Imports ---
from dcs_memory.services.kps.app.main import KnowledgeProcessorServiceImpl
# from dcs_memory.common.grpc_utils import RETRYABLE_STATUS_CODES # Not directly used
from dcs_memory.services.kps.generated_grpc import glm_service_pb2, kem_pb2, kps_service_pb2 # Adjusted path

def create_rpc_error(code, details="Test RpcError from KPS test"):
    error = grpc.RpcError(details)
    error.code = lambda: code
    error.details = lambda: details
    return error

class TestKPSRetryLogic(unittest.TestCase):

    def setUp(self):
        # Мокаем GlobalLongTermMemoryStub
        self.mock_glm_stub_patcher = patch('dcs_memory.services.kps.app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub') # Adjusted patch path
        self.MockGLMStub = self.mock_glm_stub_patcher.start()
        self.mock_glm_stub_instance = self.MockGLMStub.return_value

        # Мокируем grpc.insecure_channel
        self.mock_grpc_channel_patcher = patch('dcs_memory.services.kps.app.main.grpc.insecure_channel') # Adjusted patch path
        self.mock_grpc_channel = self.mock_grpc_channel_patcher.start()

        # Мокируем time.sleep
        self.mock_time_sleep_patcher_common = patch('dcs_memory.common.grpc_utils.time.sleep')
        self.mock_time_sleep_common = self.mock_time_sleep_patcher_common.start()

        # Мок для SentenceTransformer
        self.mock_sentence_transformer_patcher = patch('dcs_memory.services.kps.app.main.SentenceTransformer') # Adjusted patch path
        self.MockSentenceTransformer = self.mock_sentence_transformer_patcher.start()
        self.mock_transformer_instance = self.MockSentenceTransformer.return_value

        self.test_vector_size = 5 # Example, ensure this matches config if not patched
        # Patch the config that KPS_DEFAULT_VECTOR_SIZE is read from if it's module level in kps.app.main
        # or ensure the KPSConfig instance used by kps_service has this value.
        # For now, assuming KPS_DEFAULT_VECTOR_SIZE in kps.app.main can be patched.
        with patch('dcs_memory.services.kps.app.main.config') as mock_kps_config:
            mock_kps_config.DEFAULT_VECTOR_SIZE = self.test_vector_size
            self.mock_transformer_instance.encode.return_value = [[0.1] * self.test_vector_size]

            # It's important that KPSConfig is properly mocked *before* KPLServiceImpl reads it.
            # If KPS_DEFAULT_VECTOR_SIZE is a global in main, this patch is fine.
            # If it's from a config object, that object needs mocking.
            self.kps_service = KnowledgeProcessorServiceImpl(config=mock_kps_config) # Pass mocked config

            # Override retry parameters on the instance for test predictability
            # These are now typically part of a config object that the retry decorator reads.
            # For testing the decorator via the service method, we'd mock the config used by the decorator
            # or ensure the service's config has these test values.
            # The decorator now gets config from the service instance (self.config).
            self.kps_service.config.RETRY_MAX_ATTEMPTS = 3
            self.kps_service.config.RETRY_INITIAL_DELAY_S = 0.01
            self.kps_service.config.RETRY_BACKOFF_FACTOR = 1.5
            self.kps_service.config.CIRCUIT_BREAKER_ENABLED = False # Disable CB for focused retry test


    def tearDown(self):
        self.mock_glm_stub_patcher.stop()
        self.mock_grpc_channel_patcher.stop()
        self.mock_time_sleep_patcher_common.stop()
        self.mock_sentence_transformer_patcher.stop()

    def test_process_raw_data_glm_store_retry_success(self):
        """Тест: ProcessRawData успешно сохраняет в GLM после retry."""
        kem_to_return_from_glm = kem_pb2.KEM(id="glm_kem_id_123")
        success_response_glm = glm_service_pb2.StoreKEMResponse(kem=kem_to_return_from_glm)

        self.mock_glm_stub_instance.StoreKEM.side_effect = [
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            success_response_glm
        ]

        request = kps_service_pb2.ProcessRawDataRequest(
            content_type="text/plain",
            raw_content="test data".encode('utf-8')
        )
        response = self.kps_service.ProcessRawData(request, MagicMock())

        self.assertTrue(response.success)
        self.assertEqual(response.kem_id, "glm_kem_id_123")
        self.assertEqual(self.mock_glm_stub_instance.StoreKEM.call_count, 2)
        self.assertEqual(self.mock_time_sleep_common.call_count, 1)


    def test_process_raw_data_glm_store_retry_fail_all_attempts(self):
        """Тест: ProcessRawData неуспешно после всех retry к GLM.StoreKEM."""
        self.mock_glm_stub_instance.StoreKEM.side_effect = create_rpc_error(grpc.StatusCode.UNAVAILABLE)

        request = kps_service_pb2.ProcessRawDataRequest(
            content_type="text/plain",
            raw_content="test data".encode('utf-8')
        )
        mock_context = MagicMock()

        # The method ProcessRawData catches the RpcError from the retry decorator
        # and should return a response with success=False.
        response = self.kps_service.ProcessRawData(request, mock_context)

        self.assertFalse(response.success)
        self.assertIn("Ошибка взаимодействия с GLM", response.status_message)
        self.assertEqual(self.mock_glm_stub_instance.StoreKEM.call_count, 3) # 3 attempts by default from setup
        self.assertEqual(self.mock_time_sleep_common.call_count, 2) # 2 delays for 3 attempts
        mock_context.abort.assert_not_called() # Should not abort, but return success=False


if __name__ == '__main__':
    unittest.main()
