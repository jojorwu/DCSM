import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, call
import json
import lz4.frame

from dcs_memory.services.swm.app.redis_kem_cache import RedisKemCache
from dcs_memory.common.config import SWMConfig
from dcs_memory.generated_grpc import kem_pb2
from google.protobuf.timestamp_pb2 import Timestamp

def create_kem_for_test(kem_id: str, content: bytes, metadata: dict = None) -> kem_pb2.KEM:
    """Helper to create a KEM for testing."""
    now = Timestamp()
    now.GetCurrentTime()
    return kem_pb2.KEM(
        id=kem_id,
        content=content,
        content_type="text/plain",
        metadata=metadata or {},
        created_at=now,
        updated_at=now,
    )

@pytest.mark.asyncio
class TestRedisCacheCompression:

    @pytest.fixture
    def mock_redis_client(self):
        """Provides a mock aioredis client."""
        redis_mock = MagicMock()
        redis_mock.hmset = AsyncMock()
        redis_mock.hgetall = AsyncMock()
        # The pipeline is used for transactions, we need to mock its context manager
        # and the chained calls.
        pipeline_mock = MagicMock()
        pipeline_mock.hmset = AsyncMock()
        pipeline_mock.execute = AsyncMock()

        # Mock the __aenter__ and __aexit__ methods for async with
        async_pipeline_context = AsyncMock()
        async_pipeline_context.__aenter__.return_value = pipeline_mock

        redis_mock.pipeline.return_value = async_pipeline_context
        redis_mock.hmget = AsyncMock(return_value=[None, None, None]) # For _get_old_indexed_fields

        return redis_mock, pipeline_mock

    def get_mock_config(self, enabled: bool, min_size: int, algo: str = 'lz4') -> SWMConfig:
        """Helper to create a mock SWMConfig."""
        mock_cfg = MagicMock(spec=SWMConfig)
        mock_cfg.SWM_COMPRESSION_ENABLED = enabled
        mock_cfg.SWM_COMPRESSION_TYPE = algo
        mock_cfg.SWM_COMPRESSION_MIN_SIZE_BYTES = min_size
        # Add other necessary default values for RedisKemCache constructor
        mock_cfg.REDIS_KEM_KEY_PREFIX = "test_kem:"
        mock_cfg.SWM_INDEXED_METADATA_KEYS = []
        mock_cfg.REDIS_INDEX_META_KEY_PREFIX = "test_idx:meta:"
        mock_cfg.REDIS_INDEX_DATE_CREATED_KEY = "test_idx:created"
        mock_cfg.REDIS_INDEX_DATE_UPDATED_KEY = "test_idx:updated"
        mock_cfg.REDIS_TRANSACTION_MAX_RETRIES = 1
        mock_cfg.REDIS_TRANSACTION_RETRY_INITIAL_DELAY_S = 0.01
        mock_cfg.REDIS_TRANSACTION_RETRY_BACKOFF_FACTOR = 2
        return mock_cfg

    async def test_compression_logic_on_set(self, mock_redis_client):
        """
        Tests that content is compressed only when it's enabled and the content
        is larger than the minimum size threshold.
        """
        redis_mock, pipeline_mock = mock_redis_client
        config = self.get_mock_config(enabled=True, min_size=1024)
        cache = RedisKemCache(redis_client=redis_mock, config=config)

        small_content = b'a' * 512
        large_content = b'b' * 2048

        kem_small = create_kem_for_test("kem_small", small_content)
        kem_large = create_kem_for_test("kem_large", large_content)

        # Set both KEMs
        await cache.set(kem_small.id, kem_small)
        await cache.set(kem_large.id, kem_large)

        # Verify hmset was called on the pipeline object
        assert pipeline_mock.hmset.call_count == 2

        # Check the payload for the small KEM (should not be compressed)
        call_small_args, _ = pipeline_mock.hmset.call_args_list[0]
        payload_small = call_small_args[1]
        assert payload_small['content'] == small_content
        metadata_small = json.loads(payload_small['metadata'])
        assert 'compression' not in metadata_small

        # Check the payload for the large KEM (should be compressed)
        call_large_args, _ = pipeline_mock.hmset.call_args_list[1]
        payload_large = call_large_args[1]
        assert payload_large['content'] != large_content
        assert len(payload_large['content']) < len(large_content)

        # Verify it's valid lz4
        assert lz4.frame.decompress(payload_large['content']) == large_content

        metadata_large = json.loads(payload_large['metadata'])
        assert metadata_large.get('compression') == 'lz4'

    async def test_decompression_on_get(self, mock_redis_client):
        """
        Tests that content is correctly decompressed when retrieved if the
        compression flag is present in metadata.
        """
        redis_mock, _ = mock_redis_client
        config = self.get_mock_config(enabled=True, min_size=1024)
        cache = RedisKemCache(redis_client=redis_mock, config=config)

        original_content = b'c' * 2048
        compressed_content = lz4.frame.compress(original_content)

        # Create the payload as it would be stored in Redis
        redis_payload = {
            b'id': b'kem_compressed',
            b'content': compressed_content,
            b'content_type': b'text/plain',
            b'metadata': json.dumps({'compression': 'lz4', 'source': 'test'}).encode('utf-8')
        }
        redis_mock.hgetall.return_value = redis_payload

        # Get the KEM
        retrieved_kem = await cache.get("kem_compressed")

        redis_mock.hgetall.assert_awaited_once_with("test_kem:kem_compressed")
        assert retrieved_kem is not None
        assert retrieved_kem.content == original_content
        assert retrieved_kem.metadata['source'] == 'test' # Ensure other metadata is preserved

    async def test_compression_disabled(self, mock_redis_client):
        """
        Tests that content is not compressed if the feature is disabled in config.
        """
        redis_mock, pipeline_mock = mock_redis_client
        config = self.get_mock_config(enabled=False, min_size=1024)
        cache = RedisKemCache(redis_client=redis_mock, config=config)

        large_content = b'd' * 2048
        kem_large = create_kem_for_test("kem_large_disabled", large_content)

        await cache.set(kem_large.id, kem_large)

        assert pipeline_mock.hmset.call_count == 1
        call_args, _ = pipeline_mock.hmset.call_args_list[0]
        payload = call_args[1]

        assert payload['content'] == large_content
        metadata = json.loads(payload['metadata'])
        assert 'compression' not in metadata
