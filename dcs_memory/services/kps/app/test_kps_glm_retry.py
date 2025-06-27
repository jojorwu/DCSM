import unittest
from unittest.mock import MagicMock, patch
import grpc
import time

import sys
import os

from dcs_memory.services.kps.app.main import KnowledgeProcessorServiceImpl
from dcs_memory.common.grpc_utils import RETRYABLE_STATUS_CODES
from generated_grpc import glm_service_pb2, kem_pb2, kps_service_pb2

def create_rpc_error(code, details="Test RpcError from KPS test"):
    error = grpc.RpcError(details)
    error.code = lambda: code
    error.details = lambda: details
    return error

class TestKPSRetryLogic(unittest.TestCase):

    def setUp(self):
        self.mock_glm_stub_patcher = patch('dcs_memory.services.kps.app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub')
        self.MockGLMStub = self.mock_glm_stub_patcher.start()
        self.mock_glm_stub_instance = self.MockGLMStub.return_value

        self.mock_grpc_channel_patcher = patch('dcs_memory.services.kps.app.main.grpc.insecure_channel')
        self.mock_grpc_channel = self.mock_grpc_channel_patcher.start()

        self.mock_time_sleep_patcher_common = patch('dcs_memory.common.grpc_utils.time.sleep')
        self.mock_time_sleep_common = self.mock_time_sleep_patcher_common.start()

        # Мок для SentenceTransformer, чтобы он не пытался загружать модель
        self.mock_sentence_transformer_patcher = patch('dcs_memory.services.kps.app.main.SentenceTransformer')
        self.MockSentenceTransformer = self.mock_sentence_transformer_patcher.start()
        self.mock_transformer_instance = self.MockSentenceTransformer.return_value
        # Имитируем, что encode возвращает что-то с правильной размерностью
        # KPS_DEFAULT_VECTOR_SIZE по умолчанию 384 в main.py KPS
        # Мы можем либо мокнуть KPS_DEFAULT_VECTOR_SIZE, либо вернуть эмбеддинг нужной длины
        # Предположим, KPS_DEFAULT_VECTOR_SIZE = 3 для простоты теста, если он не мокается.
        # Лучше мокнуть KPS_DEFAULT_VECTOR_SIZE в kps.app.main
        self.test_vector_size = 5
        with patch('dcs_memory.services.kps.app.main.KPS_DEFAULT_VECTOR_SIZE', self.test_vector_size):
            self.mock_transformer_instance.encode.return_value = [[0.1] * self.test_vector_size]

            self.kps_service = KnowledgeProcessorServiceImpl()
            # Переопределяем параметры retry для предсказуемости тестов
            self.kps_service.retry_max_attempts = 3
            self.kps_service.retry_initial_delay_s = 0.01
            self.kps_service.retry_backoff_factor = 1.5


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
        response = self.kps_service.ProcessRawData(request, mock_context)

        self.assertFalse(response.success)
        # В текущей реализации KPS.ProcessRawData он возвращает ProcessRawDataResponse с success=False,
        # а не пробрасывает ошибку через context.abort() при ошибке вызова GLM.
        # Декоратор retry_grpc_call пробросит RpcError, если все попытки исчерпаны.
        # Это означает, что try-except в ProcessRawData поймает эту ошибку.
        self.assertIn("Ошибка взаимодействия с GLM", response.status_message)
        self.assertEqual(self.mock_glm_stub_instance.StoreKEM.call_count, 3) # 3 попытки
        self.assertEqual(self.mock_time_sleep_common.call_count, 2)


if __name__ == '__main__':
    unittest.main()
