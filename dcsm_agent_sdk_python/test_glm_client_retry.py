import unittest
from unittest.mock import MagicMock, patch, call
import grpc # Нужен для grpc.RpcError и StatusCode
import time # Для проверки, что sleep вызывался (косвенно)

# Добавляем путь для импорта модулей SDK
import sys
import os
# current_script_path = os.path.abspath(__file__)
# sdk_root_dir = os.path.dirname(current_script_path)
# if sdk_root_dir not in sys.path:
# sys.path.insert(0, sdk_root_dir)

from dcsm_agent_sdk_python.glm_client import GLMClient
# RETRYABLE_STATUS_CODES и _retry_grpc_call (теперь retry_grpc_call) больше не импортируются отсюда,
# так как они инкапсулированы в dcs_memory.common.grpc_utils и используются GLMClient внутренне.
# Импортируем также glm_service_pb2 для создания фейковых запросов/ответов, если потребуется
from dcsm_agent_sdk_python.generated_grpc_code import glm_service_pb2


# Вспомогательная функция для создания фейковой ошибки RpcError
def create_rpc_error(code, details="Test RpcError"):
    error = grpc.RpcError(details)
    error.code = lambda: code # Мокаем метод code()
    error.details = lambda: details
    return error

class TestGLMClientRetry(unittest.TestCase):

    def setUp(self):
        # Мокируем grpc.insecure_channel и stub, чтобы GLMClient мог инициализироваться
        self.mock_grpc_channel_patcher = patch('dcsm_agent_sdk_python.glm_client.grpc.insecure_channel')
        self.mock_grpc_channel = self.mock_grpc_channel_patcher.start()
        self.mock_channel_instance = self.mock_grpc_channel.return_value

        self.mock_glm_stub_patcher = patch('dcsm_agent_sdk_python.generated_grpc_code.glm_service_pb2_grpc.GlobalLongTermMemoryStub')
        self.MockGLMStub = self.mock_glm_stub_patcher.start()
        self.mock_stub_instance = self.MockGLMStub.return_value

        # Мокируем time.sleep, чтобы тесты не ждали реально
        self.mock_time_sleep_patcher = patch('time.sleep')
        self.mock_time_sleep = self.mock_time_sleep_patcher.start()

    def tearDown(self):
        self.mock_grpc_channel_patcher.stop()
        self.mock_glm_stub_patcher.stop()
        self.mock_time_sleep_patcher.stop()

    def test_retry_success_after_unavailable_errors(self):
        """Тест: успех после нескольких ошибок UNAVAILABLE."""
        client = GLMClient(retry_max_attempts=3, retry_initial_delay_s=0.01) # Маленькая задержка для теста

        # Мок для delete_kem (простой метод для теста)
        # Первые два вызова - ошибка, третий - успех
        mock_response_success = MagicMock() # google.protobuf.empty_pb2.Empty()
        self.mock_stub_instance.DeleteKEM.side_effect = [
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            mock_response_success
        ]

        result = client.delete_kem("test_id_retry")
        self.assertTrue(result) # Ожидаем True после успешного retry
        self.assertEqual(self.mock_stub_instance.DeleteKEM.call_count, 3)
        self.assertEqual(self.mock_time_sleep.call_count, 2) # 2 задержки перед 2-й и 3-й попыткой

    def test_retry_fail_after_all_attempts_unavailable(self):
        """Тест: неудача после всех попыток с ошибкой UNAVAILABLE."""
        client = GLMClient(retry_max_attempts=3, retry_initial_delay_s=0.01)

        self.mock_stub_instance.DeleteKEM.side_effect = create_rpc_error(grpc.StatusCode.UNAVAILABLE)

        with self.assertRaises(grpc.RpcError) as cm:
            client.delete_kem("test_id_fail")

        self.assertEqual(cm.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(self.mock_stub_instance.DeleteKEM.call_count, 3) # Все 3 попытки
        self.assertEqual(self.mock_time_sleep.call_count, 2)

    def test_no_retry_for_non_retryable_error(self):
        """Тест: нет повторов для ошибки, не подлежащей retry (например, INVALID_ARGUMENT)."""
        client = GLMClient(retry_max_attempts=3, retry_initial_delay_s=0.01)

        self.mock_stub_instance.DeleteKEM.side_effect = create_rpc_error(grpc.StatusCode.INVALID_ARGUMENT)

        with self.assertRaises(grpc.RpcError) as cm:
            client.delete_kem("test_id_non_retry")

        self.assertEqual(cm.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(self.mock_stub_instance.DeleteKEM.call_count, 1) # Только 1 попытка
        self.mock_time_sleep.assert_not_called() # Не должно быть задержек

    def test_no_retry_for_python_exception(self):
        """Тест: нет повторов для обычного Python исключения."""
        client = GLMClient(retry_max_attempts=3, retry_initial_delay_s=0.01)

        self.mock_stub_instance.DeleteKEM.side_effect = ValueError("Simulated Python error")

        with self.assertRaises(ValueError):
            client.delete_kem("test_id_python_error")

        self.assertEqual(self.mock_stub_instance.DeleteKEM.call_count, 1)
        self.mock_time_sleep.assert_not_called()

    def test_retrieve_kems_with_retry_success(self):
        """Тест retry для retrieve_kems."""
        client = GLMClient(retry_max_attempts=2, retry_initial_delay_s=0.01)

        mock_kem_dict = {"id": "kem1", "content": "data"}
        # retrieve_kems возвращает (list_of_kems, next_page_token)
        # glm_service_pb2.RetrieveKEMsResponse(kems=[kem_pb2.KEM(id="kem1")], next_page_token="t1")
        # Но мы мокаем метод клиента, который уже конвертирует в dict
        success_response_tuple = ([mock_kem_dict], "next_token")

        # Мок для _kem_proto_to_dict не нужен, так как мы мокаем сам метод stub.RetrieveKEMs
        # и декоратор вызывает обернутый метод, который уже содержит вызов stub.RetrieveKEMs

        # Нам нужно мокнуть stub.RetrieveKEMs
        mock_grpc_response = glm_service_pb2.RetrieveKEMsResponse()
        # Попробуем импортировать kem_pb2 здесь снова, на всякий случай
        from dcsm_agent_sdk_python.generated_grpc_code import kem_pb2 as local_kem_pb2
        kem_to_add = local_kem_pb2.KEM(id="kem1")
        kem_to_add.content = "data".encode()
        mock_grpc_response.kems.append(kem_to_add)
        mock_grpc_response.next_page_token = "next_token"

        self.mock_stub_instance.RetrieveKEMs.side_effect = [
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            mock_grpc_response # Успешный ответ
        ]

        # Мокаем _kem_proto_to_dict, так как он вызывается после успешного RetrieveKEMs
        with patch.object(client, '_kem_proto_to_dict', return_value=mock_kem_dict):
            kems, next_token = client.retrieve_kems(ids_filter=["kem1"])

        self.assertIsNotNone(kems)
        self.assertEqual(len(kems), 1)
        self.assertEqual(kems[0]["id"], "kem1")
        self.assertEqual(next_token, "next_token")
        self.assertEqual(self.mock_stub_instance.RetrieveKEMs.call_count, 2)
        self.assertEqual(self.mock_time_sleep.call_count, 1)


if __name__ == '__main__':
    unittest.main()
