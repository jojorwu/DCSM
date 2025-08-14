import unittest
from unittest.mock import MagicMock, patch, call
import uuid
import json
import sqlite3
from typing import Optional

import sys
import os

# --- Remove sys.path manipulation ---
# current_script_path = os.path.abspath(__file__)
# app_dir_test = os.path.dirname(current_script_path)
# service_root_dir_test = os.path.dirname(app_dir_test)
# if service_root_dir_test not in sys.path:
#     sys.path.insert(0, service_root_dir_test)
# dcs_memory_root = os.path.abspath(os.path.join(service_root_dir_test, "../../"))
# if dcs_memory_root not in sys.path:
#    sys.path.insert(0, dcs_memory_root)

# --- Corrected Imports ---
from dcs_memory.services.glm.app.main import GlobalLongTermMemoryServicerImpl
from dcs_memory.generated_grpc import kem_pb2, glm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp


TEST_DEFAULT_VECTOR_SIZE = 3

def create_proto_kem(id: str = None, metadata: dict = None, embeddings: list[float] = None, content: str = "content") -> kem_pb2.KEM:
    kem = kem_pb2.KEM()
    if id:
        kem.id = id
    kem.content_type = "text/plain"
    kem.content = content.encode('utf-8')
    if metadata:
        for k, v in metadata.items():
            kem.metadata[k] = str(v)
    if embeddings:
        kem.embeddings.extend(embeddings)
    return kem

import pytest
from unittest.mock import AsyncMock
from dcs_memory.services.glm.app.repositories.default_impl import DefaultGLMRepository
from dcs_memory.services.glm.app.config import GLMConfig


@pytest.mark.asyncio
class TestGLMBatchStoreKEMs:

    @pytest.fixture
    def mock_storage_repo(self):
        """Fixture to create an async-aware mock for the storage repository."""
        repo = MagicMock(spec=DefaultGLMRepository)
        repo.batch_store_kems = AsyncMock()
        # Mock the config attribute on the repository mock, as the servicer uses it
        repo.config = MagicMock(spec=GLMConfig)
        repo.config.DEFAULT_VECTOR_SIZE = TEST_DEFAULT_VECTOR_SIZE
        return repo

    @pytest.fixture
    def servicer(self, mock_storage_repo):
        """Fixture to create the servicer instance with the mocked repository."""
        # The servicer is now initialized with the repository injected directly
        return GlobalLongTermMemoryServicerImpl(storage_repository=mock_storage_repo)

    async def test_batch_store_all_successful_no_embeddings(self, servicer, mock_storage_repo):
        """Tests successful batch storage of KEMs without embeddings."""
        kems_in = [create_proto_kem(id=f"kem{i}") for i in range(2)]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)
        # The servicer should pass the valid KEMs to the repo
        mock_storage_repo.batch_store_kems.return_value = (kems_in, [])

        response = await servicer.BatchStoreKEMs(request, None)

        assert len(response.successfully_stored_kems) == 2
        assert len(response.failed_kem_references) == 0
        assert response.overall_error_message == ""
        # The servicer validates dimensions and passes on the valid list
        mock_storage_repo.batch_store_kems.assert_awaited_once_with([k for k in kems_in if k.id])


    async def test_batch_store_all_successful_with_embeddings(self, servicer, mock_storage_repo):
        """Tests successful batch storage of KEMs with valid embeddings."""
        kems_in = [create_proto_kem(id=f"kem{i}", embeddings=[0.1]*TEST_DEFAULT_VECTOR_SIZE) for i in range(2)]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)
        mock_storage_repo.batch_store_kems.return_value = (kems_in, [])

        response = await servicer.BatchStoreKEMs(request, None)

        assert len(response.successfully_stored_kems) == 2
        assert len(response.failed_kem_references) == 0
        mock_storage_repo.batch_store_kems.assert_awaited_once_with(kems_in)

    async def test_batch_store_one_fails_at_repo_level(self, servicer, mock_storage_repo):
        """Tests when the repository reports failure for one KEM."""
        kems_in = [create_proto_kem(id="kem1"), create_proto_kem(id="kem2")]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)
        # Simulate repository failing to store "kem2"
        mock_storage_repo.batch_store_kems.return_value = ([kems_in[0]], ["kem2"])

        response = await servicer.BatchStoreKEMs(request, None)

        assert len(response.successfully_stored_kems) == 1
        assert response.successfully_stored_kems[0].id == "kem1"
        assert len(response.failed_kem_references) == 1
        assert "kem2" in response.failed_kem_references

    async def test_batch_store_embedding_dimension_mismatch(self, servicer, mock_storage_repo):
        """Tests that the servicer filters out KEMs with incorrect embedding dimensions."""
        kems_in = [
            create_proto_kem(id="ok_kem", embeddings=[0.1]*TEST_DEFAULT_VECTOR_SIZE),
            create_proto_kem(id="bad_embedding_kem", embeddings=[0.2]*(TEST_DEFAULT_VECTOR_SIZE + 1))
        ]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)

        # The repo method will only be called with the valid KEMs.
        # We mock its return value based on what it receives.
        mock_storage_repo.batch_store_kems.return_value = ([kems_in[0]], [])

        response = await servicer.BatchStoreKEMs(request, None)

        # Assert that only the valid KEM was passed to the repository
        mock_storage_repo.batch_store_kems.assert_awaited_once()
        call_args, _ = mock_storage_repo.batch_store_kems.call_args
        assert len(call_args[0]) == 1
        assert call_args[0][0].id == "ok_kem"

        # Assert the final response reflects one success and one servicer-level failure
        assert len(response.successfully_stored_kems) == 1
        assert response.successfully_stored_kems[0].id == "ok_kem"
        assert len(response.failed_kem_references) == 1
        assert "bad_embedding_kem" in response.failed_kem_references


if __name__ == '__main__':
    unittest.main()


# --- New Test Class for RetrieveKEMs ---
from dcs_memory.services.glm.app.repositories.default_impl import DefaultGLMRepository
from dcs_memory.services.glm.app.config import GLMConfig
import asyncio

# Helper to run async methods in tests if needed
def async_test(f):
    def wrapper(*args, **kwargs):
        # Since the servicer methods are now async, we need a running event loop
        # to test them. `unittest.TestCase` doesn't support async tests directly
        # in older Python versions, but pytest-asyncio handles this.
        # If running without pytest, we'd need `asyncio.run`.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # 'RuntimeError: There is no current event loop...'
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Ensure the test itself is awaited
        if asyncio.iscoroutinefunction(f):
             loop.run_until_complete(f(*args, **kwargs))
        else:
             # For non-async test methods that might call async code indirectly
             # This setup is more complex. Assuming tests are written as async def.
             pass # Or handle sync test case
    return wrapper

@pytest.mark.asyncio
class TestGLMRetrieveKEMs:

    @pytest.fixture
    def mock_storage_repo(self):
        """Fixture to create an async-aware mock for the storage repository."""
        repo = MagicMock(spec=DefaultGLMRepository)
        repo.retrieve_kems = AsyncMock()
        # Add external_repos dict for testing dispatch logic
        repo.external_repos = {}
        return repo

    @pytest.fixture
    def servicer(self, mock_storage_repo):
        """Fixture to create the servicer instance with the mocked repository."""
        return GlobalLongTermMemoryServicerImpl(storage_repository=mock_storage_repo)

    async def test_retrieve_kems_dispatch_to_external_source(self, servicer, mock_storage_repo):
        """Tests that a query with data_source_name is dispatched to the correct external repo."""
        mock_external_source_name = "my_external_db"
        mock_external_repo = MagicMock(spec=BaseExternalRepository)
        mock_external_repo.retrieve_mapped_kems = AsyncMock(return_value=([], None))
        mock_storage_repo.external_repos[mock_external_source_name] = mock_external_repo

        query = glm_service_pb2.KEMQuery(data_source_name=mock_external_source_name)
        request = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=10, page_token="test_token")

        await servicer.RetrieveKEMs(request, MagicMock())

        # Verify the call was dispatched to the external repo, not the main one
        mock_storage_repo.retrieve_kems.assert_not_called()
        mock_external_repo.retrieve_mapped_kems.assert_awaited_once_with(
            query=query, page_size=10, page_token="test_token"
        )

    async def test_retrieve_kems_fall_back_to_native_if_no_data_source_name(self, servicer, mock_storage_repo):
        """Tests that a query without data_source_name calls the native repository."""
        query = glm_service_pb2.KEMQuery(text_query="native search")
        request = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=5)

        await servicer.RetrieveKEMs(request, MagicMock())

        mock_storage_repo.retrieve_kems.assert_awaited_once_with(
            query=query, page_size=5, page_token=""
        )

    async def test_retrieve_kems_error_if_unknown_data_source_name(self, servicer):
        """Tests that an unknown data_source_name returns an INVALID_ARGUMENT error."""
        query = glm_service_pb2.KEMQuery(data_source_name="non_existent_source")
        request = glm_service_pb2.RetrieveKEMsRequest(query=query)
        mock_context = MagicMock()
        mock_context.abort = MagicMock()

        await servicer.RetrieveKEMs(request, mock_context)

        mock_context.abort.assert_called_once()
        args, _ = mock_context.abort.call_args
        assert args[0] == grpc.StatusCode.INVALID_ARGUMENT
        assert "External data source 'non_existent_source' not available" in args[1]

# Need to import BaseExternalRepository for type hinting and MagicMock spec
from dcs_memory.services.glm.app.repositories.external_base import BaseExternalRepository
import grpc # For grpc.RpcError and grpc.StatusCode
