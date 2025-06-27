import unittest
from unittest.mock import MagicMock, patch
import grpc
import time

import sys
import os

# Добавляем корень проекта в sys.path для корректных импортов dcs_memory.common и generated_grpc
# Тест запускается из корня как: python -m unittest dcs_memory.services.swm.app.test_swm_glm_retry
# current_script_path = os.path.abspath(__file__)
# app_dir_test = os.path.dirname(current_script_path) # /app/dcs_memory/services/swm/app
# service_root_dir_test = os.path.dirname(app_dir_test) # /app/dcs_memory/services/swm
# dcs_memory_dir = os.path.dirname(service_root_dir_test) # /app/dcs_memory
# project_root_dir = os.path.dirname(dcs_memory_dir) # /app
# if project_root_dir not in sys.path:
#    sys.path.insert(0, project_root_dir)

from dcs_memory.services.swm.app.main import SharedWorkingMemoryServiceImpl
from dcs_memory.common.grpc_utils import RETRYABLE_STATUS_CODES # Используем для создания ошибок
from generated_grpc import glm_service_pb2, kem_pb2, swm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp # Нужен для create_kem_proto_for_test


def create_kem_proto_for_test(id_str: str, content_str: str = "content") -> kem_pb2.KEM: # Скопировано из test_swm_pubsub.py
    kem = kem_pb2.KEM(id=id_str, content_type="text/plain", content=content_str.encode('utf-8'))
    ts = Timestamp()
    ts.GetCurrentTime()
    kem.created_at.CopyFrom(ts)
    kem.updated_at.CopyFrom(ts)
    return kem

def create_rpc_error(code, details="Test RpcError from SWM test"):
    error = grpc.RpcError(details)
    error.code = lambda: code
    error.details = lambda: details
    return error

class TestSWMRetryLogic(unittest.TestCase):

    def setUp(self):
        # Мокируем GlobalLongTermMemoryStub, который используется внутри SWM
        self.mock_glm_stub_patcher = patch('dcs_memory.services.swm.app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub')
        self.MockGLMStub = self.mock_glm_stub_patcher.start()
        self.mock_glm_stub_instance = self.MockGLMStub.return_value

        # Мокируем grpc.insecure_channel, чтобы __init__ SWM не пытался реально соединиться
        self.mock_grpc_channel_patcher = patch('dcs_memory.services.swm.app.main.grpc.insecure_channel')
        self.mock_grpc_channel = self.mock_grpc_channel_patcher.start()

        # Мокируем time.sleep
        self.mock_time_sleep_patcher = patch('time.sleep') # Важно мокать time в том модуле, где он используется декоратором
                                                          # Декоратор в dcs_memory.common.grpc_utils
        self.mock_time_sleep_patcher_common = patch('dcs_memory.common.grpc_utils.time.sleep')
        self.mock_time_sleep = self.mock_time_sleep_patcher.start()
        self.mock_time_sleep_common = self.mock_time_sleep_patcher_common.start()


        # Создаем экземпляр SWM с маленькими задержками для тестов retry
        # SWM теперь имеет атрибуты retry, которые использует декоратор
        with patch.dict(os.environ, {
            "SWM_GLM_RETRY_MAX_ATTEMPTS": "3",
            "SWM_GLM_RETRY_INITIAL_DELAY_S": "0.01",
            "SWM_GLM_RETRY_BACKOFF_FACTOR": "1.5"
        }):
            # Нужно заново импортировать или пересоздать конфигурацию, если она читается при импорте модуля
            # Проще всего переопределить атрибуты после создания экземпляра для теста
            self.swm_service = SharedWorkingMemoryServiceImpl()
            self.swm_service.retry_max_attempts = 3
            self.swm_service.retry_initial_delay_s = 0.01
            self.swm_service.retry_backoff_factor = 1.5


    def tearDown(self):
        self.mock_glm_stub_patcher.stop()
        self.mock_grpc_channel_patcher.stop()
        self.mock_time_sleep_patcher.stop()
        self.mock_time_sleep_patcher_common.stop()

    def test_load_kems_from_glm_retry_success(self):
        """Тест: LoadKEMsFromGLM успешно после нескольких ошибок GLM.RetrieveKEMs."""
        kem_id = "kem1"
        mock_kem_proto = kem_pb2.KEM(id=kem_id)
        success_response_glm = glm_service_pb2.RetrieveKEMsResponse(kems=[mock_kem_proto])

        self.mock_glm_stub_instance.RetrieveKEMs.side_effect = [
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            success_response_glm
        ]

        request = swm_service_pb2.LoadKEMsFromGLMRequest(
            query_for_glm=glm_service_pb2.KEMQuery(ids=[kem_id])
        )
        response = self.swm_service.LoadKEMsFromGLM(request, MagicMock())

        self.assertEqual(response.kems_loaded_to_swm_count, 1)
        self.assertIn(kem_id, response.loaded_kem_ids)
        self.assertEqual(self.mock_glm_stub_instance.RetrieveKEMs.call_count, 3)
        self.assertEqual(self.mock_time_sleep_common.call_count, 2) # 2 задержки

    def test_load_kems_from_glm_retry_fail_all_attempts(self):
        """Тест: LoadKEMsFromGLM неуспешно после всех попыток."""
        self.mock_glm_stub_instance.RetrieveKEMs.side_effect = create_rpc_error(grpc.StatusCode.UNAVAILABLE)

        request = swm_service_pb2.LoadKEMsFromGLMRequest(
            query_for_glm=glm_service_pb2.KEMQuery(ids=["kem1"])
        )

        mock_context = MagicMock()
        self.swm_service.LoadKEMsFromGLM(request, mock_context)

        # Проверяем, что была вызвана context.abort с правильным кодом
        # Декоратор пробрасывает последнюю ошибку RpcError
        mock_context.abort.assert_called_once()
        args_abort, _ = mock_context.abort.call_args
        self.assertEqual(args_abort[0], grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(self.mock_glm_stub_instance.RetrieveKEMs.call_count, 3)
        self.assertEqual(self.mock_time_sleep_common.call_count, 2)

    def test_publish_kem_to_glm_retry_success(self):
        """Тест: PublishKEMToSWM (с persist_to_glm) успешно после ошибок GLM.StoreKEM."""
        kem_to_publish = create_kem_proto_for_test("kem_publish_retry")
        # Ответ от GLM.StoreKEM
        mock_glm_store_response = glm_service_pb2.StoreKEMResponse(kem=kem_to_publish)

        self.mock_glm_stub_instance.StoreKEM.side_effect = [
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            mock_glm_store_response
        ]

        request = swm_service_pb2.PublishKEMToSWMRequest(
            kem_to_publish=kem_to_publish,
            persist_to_glm_if_new_or_updated=True
        )
        response = self.swm_service.PublishKEMToSWM(request, MagicMock())

        self.assertTrue(response.published_to_swm)
        self.assertTrue(response.persistence_triggered_to_glm)
        self.assertIn("Успешно сохранено/обновлено в GLM", response.status_message)
        self.assertEqual(self.mock_glm_stub_instance.StoreKEM.call_count, 2)
        self.assertEqual(self.mock_time_sleep_common.call_count, 1)

if __name__ == '__main__':
    unittest.main()
