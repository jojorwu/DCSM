import unittest
from unittest.mock import MagicMock, patch, call
import grpc
import time

import sys
import os

from dcsm_agent_sdk_python.glm_client import GLMClient
from dcsm_agent_sdk_python.generated_grpc_code import glm_service_pb2
from dcsm_agent_sdk_python.generated_grpc_code import kem_pb2 as common_kem_pb2 # For creating KEM protos for responses


def create_rpc_error(code, details="Test RpcError"):
    error = grpc.RpcError(details) # type: ignore # grpc.RpcError is not well-typed by stubs for dynamic attrs
    error.code = lambda: code
    error.details = lambda: details
    return error

class TestGLMClientRetry(unittest.TestCase):

    def setUp(self):
        self.mock_grpc_channel_patcher = patch('dcsm_agent_sdk_python.glm_client.grpc.insecure_channel')
        self.mock_grpc_channel = self.mock_grpc_channel_patcher.start()
        self.mock_channel_instance = self.mock_grpc_channel.return_value

        self.mock_glm_stub_patcher = patch('dcsm_agent_sdk_python.generated_grpc_code.glm_service_pb2_grpc.GlobalLongTermMemoryStub')
        self.MockGLMStub = self.mock_glm_stub_patcher.start()
        self.mock_stub_instance = self.MockGLMStub.return_value

        self.mock_time_sleep_patcher = patch('time.sleep')
        self.mock_time_sleep = self.mock_time_sleep_patcher.start()

    def tearDown(self):
        self.mock_grpc_channel_patcher.stop()
        self.mock_glm_stub_patcher.stop()
        self.mock_time_sleep_patcher.stop()

    def test_retry_success_after_unavailable_errors(self):
        client = GLMClient(retry_max_attempts=3, retry_initial_delay_s=0.01, delete_kem_timeout_s=0.5)
        mock_response_success = MagicMock()
        self.mock_stub_instance.DeleteKEM.side_effect = [
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            mock_response_success
        ]
        result = client.delete_kem("test_id_retry")
        self.assertTrue(result)
        self.assertEqual(self.mock_stub_instance.DeleteKEM.call_count, 3)
        self.mock_stub_instance.DeleteKEM.assert_has_calls([
            call(unittest.mock.ANY, timeout=0.5),
            call(unittest.mock.ANY, timeout=0.5),
            call(unittest.mock.ANY, timeout=0.5)
        ])
        self.assertEqual(self.mock_time_sleep.call_count, 2)

    def test_retry_fail_after_all_attempts_unavailable(self):
        client = GLMClient(retry_max_attempts=3, retry_initial_delay_s=0.01, delete_kem_timeout_s=0.5)
        self.mock_stub_instance.DeleteKEM.side_effect = create_rpc_error(grpc.StatusCode.UNAVAILABLE)
        with self.assertRaises(grpc.RpcError) as cm:
            client.delete_kem("test_id_fail")
        self.assertEqual(cm.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(self.mock_stub_instance.DeleteKEM.call_count, 3)
        self.mock_stub_instance.DeleteKEM.assert_called_with(unittest.mock.ANY, timeout=0.5)
        self.assertEqual(self.mock_time_sleep.call_count, 2)

    def test_no_retry_for_non_retryable_error(self):
        client = GLMClient(retry_max_attempts=3, retry_initial_delay_s=0.01, delete_kem_timeout_s=0.5)
        self.mock_stub_instance.DeleteKEM.side_effect = create_rpc_error(grpc.StatusCode.INVALID_ARGUMENT)
        with self.assertRaises(grpc.RpcError) as cm:
            client.delete_kem("test_id_non_retry")
        self.assertEqual(cm.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(self.mock_stub_instance.DeleteKEM.call_count, 1)
        self.mock_stub_instance.DeleteKEM.assert_called_with(unittest.mock.ANY, timeout=0.5)
        self.mock_time_sleep.assert_not_called()

    def test_no_retry_for_python_exception(self):
        """Test: no retries for a generic Python exception (decorator was changed)."""
        client = GLMClient(retry_max_attempts=3, retry_initial_delay_s=0.01, delete_kem_timeout_s=0.5)
        self.mock_stub_instance.DeleteKEM.side_effect = ValueError("Simulated Python error")
        with self.assertRaises(ValueError):
            client.delete_kem("test_id_python_error")
        self.assertEqual(self.mock_stub_instance.DeleteKEM.call_count, 1)
        self.mock_stub_instance.DeleteKEM.assert_called_with(unittest.mock.ANY, timeout=0.5)
        self.mock_time_sleep.assert_not_called()

    def test_retrieve_kems_with_retry_success_and_timeout(self):
        client = GLMClient(retry_max_attempts=2, retry_initial_delay_s=0.01, retrieve_kems_timeout_s=0.7)
        mock_kem_dict = {"id": "kem1", "content": "data"}

        mock_grpc_response = glm_service_pb2.RetrieveKEMsResponse()
        kem_to_add = common_kem_pb2.KEM(id="kem1", content="data".encode()) # Use common_kem_pb2
        mock_grpc_response.kems.append(kem_to_add)
        mock_grpc_response.next_page_token = "next_token"

        self.mock_stub_instance.RetrieveKEMs.side_effect = [
            create_rpc_error(grpc.StatusCode.UNAVAILABLE),
            mock_grpc_response
        ]

        # Patch proto_utils.kem_proto_to_dict used by the client method
        with patch('dcsm_agent_sdk_python.glm_client.kem_proto_to_dict', return_value=mock_kem_dict) as mock_converter:
            kems, next_token = client.retrieve_kems(ids_filter=["kem1"])

        self.assertIsNotNone(kems)
        self.assertEqual(len(kems), 1)
        self.assertEqual(kems[0]["id"], "kem1")
        self.assertEqual(next_token, "next_token")
        self.assertEqual(self.mock_stub_instance.RetrieveKEMs.call_count, 2)
        self.mock_stub_instance.RetrieveKEMs.assert_has_calls([
            call(unittest.mock.ANY, timeout=0.7),
            call(unittest.mock.ANY, timeout=0.7)
        ])
        self.assertEqual(self.mock_time_sleep.call_count, 1)
        mock_converter.assert_called_once_with(kem_to_add)

    def test_retrieve_kems_per_call_timeout_override(self):
        client = GLMClient(retrieve_kems_timeout_s=10.0) # Default high timeout

        mock_grpc_response = glm_service_pb2.RetrieveKEMsResponse()
        kem_to_add = common_kem_pb2.KEM(id="kem1", content="data".encode())
        mock_grpc_response.kems.append(kem_to_add)
        self.mock_stub_instance.RetrieveKEMs.return_value = mock_grpc_response

        with patch('dcsm_agent_sdk_python.glm_client.kem_proto_to_dict', return_value={"id":"kem1"}):
            client.retrieve_kems(ids_filter=["kem1"], timeout=0.1) # Override with short timeout

        self.mock_stub_instance.RetrieveKEMs.assert_called_once_with(unittest.mock.ANY, timeout=0.1)

if __name__ == '__main__':
    unittest.main()
