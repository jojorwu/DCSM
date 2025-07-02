import unittest
from unittest.mock import MagicMock, patch
import grpc
import time
import asyncio # For async tests

import sys
import os

# --- Remove sys.path manipulation ---
# current_script_path = os.path.abspath(__file__)
# app_dir = os.path.dirname(current_script_path)
# service_root_dir = os.path.dirname(app_dir)
# if service_root_dir not in sys.path:
#     sys.path.insert(0, service_root_dir)
# dcs_memory_root = os.path.abspath(os.path.join(service_root_dir, "../../"))
# if dcs_memory_root not in sys.path:
#     sys.path.insert(0, dcs_memory_root)

# --- Corrected Imports ---
from dcs_memory.services.swm.app.main import SharedWorkingMemoryServiceImpl
from dcs_memory.services.swm.app.config import SWMConfig # Import SWMConfig
# from dcs_memory.common.grpc_utils import RETRYABLE_STATUS_CODES # Not directly used
from dcs_memory.services.swm.generated_grpc import glm_service_pb2, kem_pb2, swm_service_pb2 # Adjusted path
from google.protobuf.timestamp_pb2 import Timestamp


def create_kem_proto_for_test(id_str: str, content_str: str = "content") -> kem_pb2.KEM:
    kem = kem_pb2.KEM(id=id_str, content_type="text/plain", content=content_str.encode('utf-8'))
    ts = Timestamp()
    ts.GetCurrentTime()
    kem.created_at.CopyFrom(ts)
    kem.updated_at.CopyFrom(ts)
    return kem

def create_rpc_error(code, details="Test RpcError from SWM test"):
    error = grpc.RpcError(details) # This is for sync gRPC
    # For grpc.aio, it would be grpc.aio.AioRpcError
    # The decorator handles both, but for manual creation, be specific if testing async paths.
    # Let's assume this mock error is for the sync parts or will be adapted by mocks.
    error.code = lambda: code
    error.details = lambda: details
    return error

class TestSWMRetryLogic(unittest.IsolatedAsyncioTestCase): # Changed for async tests

    async def asyncSetUp(self): # Renamed and made async
        # Мокируем GlobalLongTermMemoryStub
        self.mock_glm_stub_patcher = patch('dcs_memory.services.swm.app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub')
        self.MockGLMStub = self.mock_glm_stub_patcher.start()
        self.mock_glm_stub_instance = self.MockGLMStub.return_value

        # Мокируем grpc_aio.insecure_channel (SWM uses async client)
        self.mock_grpc_aio_channel_patcher = patch('dcs_memory.services.swm.app.main.grpc_aio.insecure_channel')
        self.mock_grpc_aio_channel = self.mock_grpc_aio_channel_patcher.start()
        self.mock_glm_aio_channel_instance = MagicMock(spec=grpc.aio.Channel) # Mock for async channel
        self.mock_grpc_aio_channel.return_value = self.mock_glm_aio_channel_instance


        self.mock_time_sleep_patcher_common = patch('dcs_memory.common.grpc_utils.asyncio.sleep') # Patch asyncio.sleep for async decorator
        self.mock_asyncio_sleep_common = self.mock_time_sleep_patcher_common.start()

        # Mock aioredis and RedisKemCache as SWMServiceImpl depends on them
        self.mock_aioredis_patch = patch('dcs_memory.services.swm.app.main.aioredis')
        self.mock_aioredis = self.mock_aioredis_patch.start()
        self.mock_redis_client = MagicMock()
        self.mock_aioredis.from_url.return_value = self.mock_redis_client

        self.mock_redis_kem_cache_patch = patch('dcs_memory.services.swm.app.main.RedisKemCache')
        self.MockRedisKemCache = self.mock_redis_kem_cache_patch.start()
        self.mock_redis_kem_cache_instance = self.MockRedisKemCache.return_value


        test_swm_config = SWMConfig(
            RETRY_MAX_ATTEMPTS = 3,
            RETRY_INITIAL_DELAY_S = 0.01,
            RETRY_BACKOFF_FACTOR = 1.5,
            CIRCUIT_BREAKER_ENABLED = False,
            GLM_SERVICE_ADDRESS = "mock_glm_address",
            SWM_REDIS_HOST = "mock_redis",
            # Add other minimal required fields for SWMConfig if any
            GLM_PERSISTENCE_QUEUE_MAX_SIZE=10, GLM_PERSISTENCE_BATCH_SIZE=1,
            GLM_PERSISTENCE_FLUSH_INTERVAL_S=0.1, GLM_PERSISTENCE_BATCH_MAX_RETRIES=1,
            REDIS_PUBSUB_GET_MESSAGE_TIMEOUT_S=0.1, REDIS_PUBSUB_ERROR_SLEEP_S=0.1,
            REDIS_MAX_PUBSUB_RETRIES=1, REDIS_RECONNECT_DELAY_S=0.1,
            SUBSCRIBER_MIN_QUEUE_SIZE=1, SUBSCRIBER_DEFAULT_QUEUE_SIZE=5, SUBSCRIBER_MAX_QUEUE_SIZE=10,
            SUBSCRIBER_IDLE_CHECK_INTERVAL_S=0.1, SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD=2,
            LOCK_CLEANUP_INTERVAL_S=1, LOCK_CLEANUP_SHUTDOWN_GRACE_S=0.1,
            REDIS_DLQ_KEY="test_dlq", DLQ_MAX_SIZE=10
        )
        self.swm_service = SharedWorkingMemoryServiceImpl(config=test_swm_config)
        # Explicitly set retry params on the service's config object for clarity in tests
        self.swm_service.config.RETRY_MAX_ATTEMPTS = 3
        self.swm_service.config.RETRY_INITIAL_DELAY_S = 0.01
        self.swm_service.config.RETRY_BACKOFF_FACTOR = 1.5
        self.swm_service.config.CIRCUIT_BREAKER_ENABLED = False


    async def asyncTearDown(self): # Renamed and made async
        self.mock_glm_stub_patcher.stop()
        self.mock_grpc_aio_channel_patcher.stop() # Stop async channel mock
        self.mock_time_sleep_patcher_common.stop()
        self.mock_aioredis_patch.stop()
        self.mock_redis_kem_cache_patch.stop()

    async def test_load_kems_from_glm_retry_success(self): # Made async
        """Тест: LoadKEMsFromGLM успешно после нескольких ошибок GLM.RetrieveKEMs."""
        kem_id = "kem1"
        mock_kem_proto = kem_pb2.KEM(id=kem_id)
        success_response_glm = glm_service_pb2.RetrieveKEMsResponse(kems=[mock_kem_proto])

        # Simulate AioRpcError for async client calls
        self.mock_glm_stub_instance.RetrieveKEMs.side_effect = [
            grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, initial_metadata=None, trailing_metadata=None, details="Test Unavailable"),
            grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, initial_metadata=None, trailing_metadata=None, details="Test Unavailable"),
            success_response_glm
        ]

        request = swm_service_pb2.LoadKEMsFromGLMRequest(
            query_for_glm=glm_service_pb2.KEMQuery(ids=[kem_id])
        )

        # Mock the _put_kem_to_cache_and_notify_async method
        with patch.object(self.swm_service, '_put_kem_to_cache_and_notify_async', new_callable=MagicMock) as mock_put_kem:
            # Make the mock an async function
            async def async_mock_put_kem(*args, **kwargs):
                return None
            mock_put_kem.side_effect = async_mock_put_kem

            response = await self.swm_service.LoadKEMsFromGLM(request, MagicMock(spec=grpc.aio.ServicerContext))

            self.assertEqual(response.kems_loaded_to_swm_count, 1)
            self.assertIn(kem_id, response.loaded_kem_ids)
            self.assertEqual(self.mock_glm_stub_instance.RetrieveKEMs.call_count, 3)
            self.assertEqual(self.mock_asyncio_sleep_common.call_count, 2)
            mock_put_kem.assert_called_once()


    async def test_load_kems_from_glm_retry_fail_all_attempts(self): # Made async
        """Тест: LoadKEMsFromGLM неуспешно после всех попыток."""
        self.mock_glm_stub_instance.RetrieveKEMs.side_effect = grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, initial_metadata=None, trailing_metadata=None, details="Test Unavailable")


        request = swm_service_pb2.LoadKEMsFromGLMRequest(
            query_for_glm=glm_service_pb2.KEMQuery(ids=["kem1"])
        )

        mock_context = MagicMock(spec=grpc.aio.ServicerContext)

        with self.assertRaises(grpc.aio.AioRpcError) as cm:
            await self.swm_service.LoadKEMsFromGLM(request, mock_context)

        self.assertEqual(cm.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(self.mock_glm_stub_instance.RetrieveKEMs.call_count, 3)
        self.assertEqual(self.mock_asyncio_sleep_common.call_count, 2)
        mock_context.abort.assert_not_called() # Decorator re-raises, SWM service might catch and abort or return error response.
                                              # Current LoadKEMsFromGLM catches and aborts.

    # test_publish_kem_to_glm_retry_success needs significant rework
    # as PublishKEMToSWM queues to a worker, and that worker calls _glm_batch_store_kems_async_with_retry.
    # Testing the retry on _glm_batch_store_kems_async_with_retry would be more direct.
    # For now, commenting out this specific test as it's not testing the decorator as intended.

    # async def test_publish_kem_to_glm_retry_success(self):
    #     """Тест: PublishKEMToSWM (с persist_to_glm) успешно после ошибок GLM.StoreKEM."""
    #     kem_to_publish = create_kem_proto_for_test("kem_publish_retry")
    #     mock_glm_store_response = glm_service_pb2.StoreKEMResponse(kem=kem_to_publish)

    #     self.mock_glm_stub_instance.StoreKEM.side_effect = [ # This mock is for StoreKEM, but worker uses BatchStoreKEMs
    #         create_rpc_error(grpc.StatusCode.UNAVAILABLE),
    #         mock_glm_store_response
    #     ]

    #     with patch.object(self.swm_service.redis_kem_cache, 'contains', return_value=False) as mock_contains, \
    #          patch.object(self.swm_service.redis_kem_cache, 'set') as mock_set, \
    #          patch.object(self.swm_service.subscription_manager, 'notify_kem_event') as mock_notify:

    #         request = swm_service_pb2.PublishKEMToSWMRequest(
    #             kem_to_publish=kem_to_publish,
    #             persist_to_glm_if_new_or_updated=True
    #         )
    #         response = await self.swm_service.PublishKEMToSWM(request, MagicMock(spec=grpc.aio.ServicerContext))

    #         self.assertTrue(response.published_to_swm)
    #         self.assertTrue(response.queued_for_glm_persistence)
    #         # The worker would make the call, so StoreKEM mock on stub won't be hit directly here.
    #         # Need to check queue or mock worker's call.
    #         self.assertFalse(self.swm_service.glm_persistence_queue.empty())


if __name__ == '__main__':
    unittest.main()
