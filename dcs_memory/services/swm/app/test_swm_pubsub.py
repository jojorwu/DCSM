import unittest
from unittest.mock import MagicMock, patch
import time
import queue
import uuid
# import threading # No longer needed for async tests
import asyncio # Added for async tests
import grpc # Added for grpc.aio context
import grpc.aio # Added for grpc.aio context


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
from dcs_memory.services.swm.app.config import SWMConfig
from dcs_memory.services.swm.generated_grpc import kem_pb2, swm_service_pb2 # Adjusted path
from google.protobuf.timestamp_pb2 import Timestamp

def create_kem_proto_for_test(id_str: str, content_str: str = "content") -> kem_pb2.KEM:
    kem = kem_pb2.KEM(id=id_str, content_type="text/plain", content=content_str.encode('utf-8'))
    ts = Timestamp()
    ts.GetCurrentTime()
    kem.created_at.CopyFrom(ts)
    kem.updated_at.CopyFrom(ts)
    return kem

class TestSWMPubSub(unittest.IsolatedAsyncioTestCase): # Changed to IsolatedAsyncioTestCase

    async def asyncSetUp(self): # Renamed to asyncSetUp
        self.mock_glm_stub_patch = patch('dcs_memory.services.swm.app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub')
        self.MockGLMStub = self.mock_glm_stub_patch.start()
        self.mock_glm_instance = self.MockGLMStub.return_value

        self.mock_grpc_aio_channel_patcher = patch('dcs_memory.services.swm.app.main.grpc_aio.insecure_channel')
        self.mock_grpc_aio_channel = self.mock_grpc_aio_channel_patcher.start()
        self.mock_glm_aio_channel_instance = MagicMock(spec=grpc.aio.Channel)
        self.mock_grpc_aio_channel.return_value = self.mock_glm_aio_channel_instance

        self.mock_aioredis_patch = patch('dcs_memory.services.swm.app.main.aioredis')
        self.mock_aioredis = self.mock_aioredis_patch.start()
        self.mock_redis_client = MagicMock()
        self.mock_aioredis.from_url.return_value = self.mock_redis_client

        self.mock_redis_kem_cache_patch = patch('dcs_memory.services.swm.app.main.RedisKemCache')
        self.MockRedisKemCache = self.mock_redis_kem_cache_patch.start()
        self.mock_redis_kem_cache_instance = self.MockRedisKemCache.return_value

        test_swm_config = SWMConfig(
            SWM_INDEXED_METADATA_KEYS=["type", "source"],
            GLM_SERVICE_ADDRESS = "",
            SWM_REDIS_HOST = "mockhost",
            SUBSCRIBER_IDLE_CHECK_INTERVAL_S=0.01, # Make intervals short for tests
            SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD=2,
            # Add other minimal required fields for SWMConfig if any
            GLM_PERSISTENCE_QUEUE_MAX_SIZE=10, GLM_PERSISTENCE_BATCH_SIZE=1,
            GLM_PERSISTENCE_FLUSH_INTERVAL_S=0.1, GLM_PERSISTENCE_BATCH_MAX_RETRIES=1,
            REDIS_PUBSUB_GET_MESSAGE_TIMEOUT_S=0.1, REDIS_PUBSUB_ERROR_SLEEP_S=0.1,
            REDIS_MAX_PUBSUB_RETRIES=1, REDIS_RECONNECT_DELAY_S=0.1,
            SUBSCRIBER_MIN_QUEUE_SIZE=1, SUBSCRIBER_DEFAULT_QUEUE_SIZE=5, SUBSCRIBER_MAX_QUEUE_SIZE=10,
            LOCK_CLEANUP_INTERVAL_S=1, LOCK_CLEANUP_SHUTDOWN_GRACE_S=0.1,
            REDIS_DLQ_KEY="test_dlq", DLQ_MAX_SIZE=10
        )
        self.swm_service = SharedWorkingMemoryServiceImpl(config=test_swm_config)
        await self.swm_service.start_background_tasks()


    async def asyncTearDown(self): # Renamed to asyncTearDown
        await self.swm_service.stop_background_tasks()
        self.mock_glm_stub_patch.stop()
        self.mock_grpc_aio_channel_patcher.stop()
        self.mock_aioredis_patch.stop()
        self.mock_redis_kem_cache_patch.stop()

    async def _put_kem_and_notify_for_test(self, kem: kem_pb2.KEM):
        # This simulates the internal logic of SWM when a KEM is put/updated.
        # It bypasses direct Redis calls for focused pub/sub testing.
        if self.swm_service.redis_kem_cache:
             # Simulate RedisKemCache behavior or mock its methods if they are complex
            self.mock_redis_kem_cache_instance.contains.return_value = False # Assume new for KEM_PUBLISHED
            self.mock_redis_kem_cache_instance.set.return_value = None # Mocked set

        event_type = swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED
        await self.swm_service.subscription_manager.notify_kem_event(kem, event_type, "SWM_TEST_HELPER")


    async def _test_subscribe_and_unsubscribe_async(self):
        mock_context = MagicMock(spec=grpc.aio.ServicerContext)
        mock_context.is_active.return_value = True
        request = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id="agent1")
        events_received = []

        async def consume_events():
            try:
                async for event in self.swm_service.SubscribeToSWMEvents(request, mock_context):
                    events_received.append(event)
                    if len(events_received) >= 1:
                        mock_context.is_active.return_value = False
            except Exception: pass # Allow graceful exit on context inactive

        consumer_task = asyncio.create_task(consume_events())
        await asyncio.sleep(0.05) # Give time for subscriber to register
        self.assertIn("agent1", self.swm_service.subscription_manager.subscribers)

        kem_test = create_kem_proto_for_test("kem_ev_1")
        await self.swm_service.subscription_manager.notify_kem_event(kem_test, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

        await asyncio.wait_for(consumer_task, timeout=0.5) # Wait for consumer to get event & exit

        self.assertGreaterEqual(len(events_received), 1)
        if events_received:
            self.assertEqual(events_received[0].kem_payload.id, "kem_ev_1")

        await asyncio.sleep(0.1) # Allow time for SubscriptionManager cleanup
        self.assertNotIn("agent1", self.swm_service.subscription_manager.subscribers)

    async def test_subscribe_and_unsubscribe(self):
        await self._test_subscribe_and_unsubscribe_async()

    async def _test_event_on_publish_async(self):
        mock_context = MagicMock(spec=grpc.aio.ServicerContext); mock_context.is_active.return_value = True
        request_sub = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id="agent_publish_test")
        event_list = []
        async def event_collector():
            async for event in self.swm_service.SubscribeToSWMEvents(request_sub, mock_context):
                event_list.append(event); mock_context.is_active.return_value = False

        collector_task = asyncio.create_task(event_collector())
        await asyncio.sleep(0.05)

        kem1 = create_kem_proto_for_test("kem_pub1")
        # Call the actual PublishKEMToSWM method
        publish_req = swm_service_pb2.PublishKEMToSWMRequest(kem_to_publish=kem1, persist_to_glm_if_new_or_updated=False)
        await self.swm_service.PublishKEMToSWM(publish_req, MagicMock(spec=grpc.aio.ServicerContext))

        await asyncio.wait_for(collector_task, timeout=1.0)
        self.assertEqual(len(event_list), 1)
        self.assertEqual(event_list[0].event_type, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        self.assertEqual(event_list[0].kem_payload.id, "kem_pub1")

    async def test_event_on_publish(self):
        await self._test_event_on_publish_async()

    async def _subscribe_and_collect_events_async(self, agent_id: str, topics: list = None, event_limit: int = 1):
        mock_context = MagicMock(spec=grpc.aio.ServicerContext); mock_context.is_active.return_value = True
        sub_topics = []
        if topics:
            for crit in topics: sub_topics.append(swm_service_pb2.SubscriptionTopic(filter_criteria=crit))
        request = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id=agent_id, topics=sub_topics, requested_queue_size=event_limit + 5) # Ensure q is big enough
        events_received = []
        async def event_collector_task_func():
            try:
                async for event in self.swm_service.SubscribeToSWMEvents(request, mock_context):
                    events_received.append(event)
                    if len(events_received) >= event_limit: mock_context.is_active.return_value = False
            except Exception: pass

        collector_task = asyncio.create_task(event_collector_task_func())
        await asyncio.sleep(0.05) # Time for subscription
        return events_received, collector_task, mock_context


    async def _test_filter_by_kem_id_async(self):
        topics = ["kem_id=target_kem"]
        events, task, context = await self._subscribe_and_collect_events_async("agent_filter_id", topics)
        kem1 = create_kem_proto_for_test("target_kem"); kem2 = create_kem_proto_for_test("other_kem")
        await self.swm_service.subscription_manager.notify_kem_event(kem1, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await self.swm_service.subscription_manager.notify_kem_event(kem2, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await asyncio.wait_for(task, timeout=0.5)
        self.assertEqual(len(events), 1); self.assertEqual(events[0].kem_payload.id, "target_kem")

    async def test_filter_by_kem_id(self):
        await self._test_filter_by_kem_id_async()

    async def _test_filter_by_metadata_async(self):
        topics = ["metadata.type=doc"]
        events, task, context = await self._subscribe_and_collect_events_async("agent_filter_meta", topics, event_limit=2)
        kem1 = create_kem_proto_for_test("kem_doc_1"); kem1.metadata["type"] = "doc"
        kem2 = create_kem_proto_for_test("kem_msg_1"); kem2.metadata["type"] = "msg"
        kem3 = create_kem_proto_for_test("kem_doc_2"); kem3.metadata["type"] = "doc"
        await self.swm_service.subscription_manager.notify_kem_event(kem1, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await self.swm_service.subscription_manager.notify_kem_event(kem2, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await self.swm_service.subscription_manager.notify_kem_event(kem3, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await asyncio.wait_for(task, timeout=0.5)
        self.assertEqual(len(events), 2); self.assertEqual({e.kem_payload.id for e in events}, {"kem_doc_1", "kem_doc_2"})

    async def test_filter_by_metadata(self):
        await self._test_filter_by_metadata_async()

    async def _test_no_filters_receives_all_async(self):
        events, task, context = await self._subscribe_and_collect_events_async("agent_no_filter", topics=[], event_limit=2)
        kem1 = create_kem_proto_for_test("kem_all_1"); kem2 = create_kem_proto_for_test("kem_all_2")
        await self.swm_service.subscription_manager.notify_kem_event(kem1, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await self.swm_service.subscription_manager.notify_kem_event(kem2, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await asyncio.wait_for(task, timeout=0.5)
        self.assertEqual(len(events), 2)

    async def test_no_filters_receives_all(self):
        await self._test_no_filters_receives_all_async()

    async def _test_multiple_filters_or_logic_async(self):
        topics = ["kem_id=id1", "metadata.status=important"] # OR logic for multiple topic criteria
        events, task, context = await self._subscribe_and_collect_events_async("agent_multi_filter", topics, event_limit=2)
        kem_id1 = create_kem_proto_for_test("id1")
        kem_important = create_kem_proto_for_test("id_other"); kem_important.metadata["status"] = "important"
        kem_irrelevant = create_kem_proto_for_test("id_irrelevant")
        await self.swm_service.subscription_manager.notify_kem_event(kem_id1, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await self.swm_service.subscription_manager.notify_kem_event(kem_important, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await self.swm_service.subscription_manager.notify_kem_event(kem_irrelevant, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await asyncio.wait_for(task, timeout=0.5)
        self.assertEqual(len(events), 2); self.assertEqual({e.kem_payload.id for e in events}, {"id1", "id_other"})

    async def test_multiple_filters_or_logic(self):
        await self._test_multiple_filters_or_logic_async()

if __name__ == '__main__':
    unittest.main()
