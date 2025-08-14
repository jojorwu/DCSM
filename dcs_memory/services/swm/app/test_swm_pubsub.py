import pytest
import pytest_asyncio
import asyncio
import grpc
from unittest.mock import patch, MagicMock, AsyncMock

from dcs_memory.services.swm.app.main import SharedWorkingMemoryServiceImpl
from dcs_memory.services.swm.app.config import SWMConfig
from dcs_memory.generated_grpc import kem_pb2, swm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp

def create_kem_proto_for_test(id_str: str, metadata: dict = None, content_str: str = "content") -> kem_pb2.KEM:
    kem = kem_pb2.KEM(id=id_str, content_type="text/plain", content=content_str.encode('utf-8'))
    if metadata:
        for k, v in metadata.items():
            kem.metadata[k] = str(v)
    ts = Timestamp()
    ts.GetCurrentTime()
    kem.created_at.CopyFrom(ts)
    kem.updated_at.CopyFrom(ts)
    return kem

@pytest.mark.asyncio
class TestSWMPubSub:

    @pytest_asyncio.fixture
    async def swm_service(self):
        """Fixture to set up the SWM service with mocked dependencies."""
        mock_config = SWMConfig(
            SWM_INDEXED_METADATA_KEYS=["type", "source"],
            GLM_SERVICE_ADDRESS="",
            SWM_REDIS_HOST="mockhost",
            SUBSCRIBER_IDLE_CHECK_INTERVAL_S=0.01,
            SUBSCRIBER_IDLE_TIMEOUT_THRESHOLD=2,
            GLM_PERSISTENCE_QUEUE_MAX_SIZE=10
        )

        with patch('dcs_memory.services.swm.app.main.aioredis') as mock_aioredis, \
             patch('dcs_memory.services.swm.app.main.RedisKemCache') as MockRedisKemCache, \
             patch('dcs_memory.services.swm.app.main.grpc_aio.insecure_channel'), \
             patch('dcs_memory.services.swm.app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub'):

            mock_redis_client = MagicMock()
            mock_redis_client.pubsub = MagicMock()
            mock_aioredis.from_url.return_value = mock_redis_client

            mock_redis_kem_cache_instance = MockRedisKemCache.return_value
            mock_redis_kem_cache_instance.contains = AsyncMock(return_value=False)
            mock_redis_kem_cache_instance.set = AsyncMock()

            service = SharedWorkingMemoryServiceImpl(service_config=mock_config)
            service.lock_manager.initialize = AsyncMock()

            # Temporarily disable persistence worker to focus on pub/sub
            service._persistence_worker = AsyncMock()

            await service.start_background_tasks()
            yield service
            await service.stop_background_tasks()

    async def test_subscribe_and_unsubscribe(self, swm_service):
        """Tests that a subscriber can be added, receive an event, and be removed."""
        agent_id = "test_agent_1"
        topics = [swm_service_pb2.SubscriptionTopic(
            type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
            filter_criteria=""
        )]
        event_q = await swm_service.subscription_manager.add_subscriber(agent_id, topics, requested_q_size=10)

        assert agent_id in swm_service.subscription_manager.subscribers

        kem_to_publish = create_kem_proto_for_test("kem1")
        await swm_service.subscription_manager.notify_kem_event(
            kem_to_publish,
            swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED
        )

        try:
            event = await asyncio.wait_for(event_q.get(), timeout=0.1)
            assert event.kem_payload.id == "kem1"
        except asyncio.TimeoutError:
            pytest.fail("Event was not received by subscriber within timeout.")

        await swm_service.subscription_manager.remove_subscriber(agent_id)
        assert agent_id not in swm_service.subscription_manager.subscribers

    async def test_event_on_publish(self, swm_service):
        """Tests that publishing a KEM via the service method generates a pub/sub event."""
        agent_id = "agent_publish_test"
        topics = [swm_service_pb2.SubscriptionTopic(
            type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
            filter_criteria=""
        )]
        event_q = await swm_service.subscription_manager.add_subscriber(agent_id, topics, requested_q_size=10)

        kem1 = create_kem_proto_for_test("kem_pub1")

        # Mock the internal cache method to avoid Redis dependency in this logic test
        swm_service.redis_kem_cache.set = AsyncMock()

        # Directly call the internal method that PublishKEMToSWM would call
        await swm_service._put_kem_to_cache_and_notify_async(kem1)

        try:
            event = await asyncio.wait_for(event_q.get(), timeout=0.1)
            assert event.event_type == swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED
            assert event.kem_payload.id == "kem_pub1"
        except asyncio.TimeoutError:
            pytest.fail("Publish event not received by subscriber.")

        await swm_service.subscription_manager.remove_subscriber(agent_id)

    async def test_filter_by_kem_id(self, swm_service):
        """Tests that a subscriber with a KEM ID filter only receives matching KEMs."""
        agent_id = "agent_filter_id"
        topics = [swm_service_pb2.SubscriptionTopic(
            type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
            filter_criteria="kem_id=target_kem"
        )]
        event_q = await swm_service.subscription_manager.add_subscriber(agent_id, topics, requested_q_size=10)

        await swm_service.subscription_manager.notify_kem_event(create_kem_proto_for_test("target_kem"), swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await swm_service.subscription_manager.notify_kem_event(create_kem_proto_for_test("other_kem"), swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

        try:
            event = await asyncio.wait_for(event_q.get(), timeout=0.1)
            assert event.kem_payload.id == "target_kem"
        except asyncio.TimeoutError:
            pytest.fail("Filtered event not received.")

        # Ensure the other event was not sent
        assert event_q.empty()
        await swm_service.subscription_manager.remove_subscriber(agent_id)

    async def test_filter_by_metadata(self, swm_service):
        """Tests filtering by metadata key-value pairs."""
        agent_id = "agent_filter_meta"
        topics = [swm_service_pb2.SubscriptionTopic(
            type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
            filter_criteria="metadata.type=doc"
        )]
        event_q = await swm_service.subscription_manager.add_subscriber(agent_id, topics, requested_q_size=10)

        kem1 = create_kem_proto_for_test("kem_doc_1", metadata={"type": "doc"})
        kem2 = create_kem_proto_for_test("kem_msg_1", metadata={"type": "msg"})
        kem3 = create_kem_proto_for_test("kem_doc_2", metadata={"type": "doc"})

        await swm_service.subscription_manager.notify_kem_event(kem1, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await swm_service.subscription_manager.notify_kem_event(kem2, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await swm_service.subscription_manager.notify_kem_event(kem3, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

        received_ids = set()
        for _ in range(2):
            try:
                event = await asyncio.wait_for(event_q.get(), timeout=0.1)
                received_ids.add(event.kem_payload.id)
            except asyncio.TimeoutError:
                pytest.fail("Expected 2 events, but only received {}.".format(len(received_ids)))

        assert received_ids == {"kem_doc_1", "kem_doc_2"}
        assert event_q.empty()
        await swm_service.subscription_manager.remove_subscriber(agent_id)

    async def test_no_filters_receives_all(self, swm_service):
        """Tests a subscriber with no filters receives all events."""
        agent_id = "agent_no_filter"
        topics = [swm_service_pb2.SubscriptionTopic(
            type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
            filter_criteria=""
        )]
        event_q = await swm_service.subscription_manager.add_subscriber(agent_id, topics, requested_q_size=10)

        await swm_service.subscription_manager.notify_kem_event(create_kem_proto_for_test("kem_all_1"), swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await swm_service.subscription_manager.notify_kem_event(create_kem_proto_for_test("kem_all_2"), swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

        received_count = 0
        for _ in range(2):
            try:
                await asyncio.wait_for(event_q.get(), timeout=0.1)
                received_count += 1
            except asyncio.TimeoutError:
                pytest.fail("Expected 2 events, but only received {}.".format(received_count))

        assert received_count == 2
        await swm_service.subscription_manager.remove_subscriber(agent_id)

    async def test_multiple_filters_or_logic(self, swm_service):
        """Tests that multiple topics for one subscriber act as an OR filter."""
        agent_id = "agent_multi_filter"
        topics = [
            swm_service_pb2.SubscriptionTopic(
                type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
                filter_criteria="kem_id=id1"
            ),
            swm_service_pb2.SubscriptionTopic(
                type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
                filter_criteria="metadata.status=important"
            )
        ]
        event_q = await swm_service.subscription_manager.add_subscriber(agent_id, topics, requested_q_size=10)

        kem_id1 = create_kem_proto_for_test("id1")
        kem_important = create_kem_proto_for_test("id_other", metadata={"status": "important"})
        kem_irrelevant = create_kem_proto_for_test("id_irrelevant")

        await swm_service.subscription_manager.notify_kem_event(kem_id1, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await swm_service.subscription_manager.notify_kem_event(kem_important, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        await swm_service.subscription_manager.notify_kem_event(kem_irrelevant, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

        received_ids = set()
        for _ in range(2):
            try:
                event = await asyncio.wait_for(event_q.get(), timeout=0.1)
                received_ids.add(event.kem_payload.id)
            except asyncio.TimeoutError:
                pytest.fail("Expected 2 events, but only received {}.".format(len(received_ids)))

        assert received_ids == {"id1", "id_other"}
        assert event_q.empty()
        await swm_service.subscription_manager.remove_subscriber(agent_id)
