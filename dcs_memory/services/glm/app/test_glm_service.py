import unittest
from unittest.mock import MagicMock, patch, call
import uuid
import json
import sqlite3

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
from dcs_memory.services.glm.generated_grpc import kem_pb2, glm_service_pb2
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

class TestGLMBatchStoreKEMs(unittest.TestCase):

    def setUp(self):
        self.mock_qdrant_client_patch = patch('dcs_memory.services.glm.app.main.QdrantClient')
        self.MockQdrantClientClass = self.mock_qdrant_client_patch.start()
        self.mock_qdrant_instance = self.MockQdrantClientClass.return_value
        self.mock_qdrant_instance.get_collection.return_value = MagicMock()
        self.mock_qdrant_instance.recreate_collection.return_value = None
        self.mock_qdrant_instance.upsert.return_value = None

        self.mock_sqlite_conn = MagicMock()
        self.mock_sqlite_cursor = MagicMock()
        self.mock_sqlite_conn.cursor.return_value = self.mock_sqlite_cursor
        self.mock_sqlite_cursor.fetchone.return_value = None
        self.mock_sqlite_cursor.execute.return_value = None

        self.mock_sqlite3_connect_patch = patch('dcs_memory.services.glm.app.main.sqlite3.connect')
        self.mock_sqlite3_connect = self.mock_sqlite3_connect_patch.start()
        self.mock_sqlite3_connect.return_value = self.mock_sqlite_conn

        # Patch the config directly within dcs_memory.services.glm.app.main
        # This assumes 'config' is a module-level variable in main.py
        self.mock_config_patch = patch('dcs_memory.services.glm.app.main.config')
        self.mock_config = self.mock_config_patch.start()

        # Set attributes on the mocked config object
        self.mock_config.DEFAULT_VECTOR_SIZE = TEST_DEFAULT_VECTOR_SIZE
        # Mock other GLMConfig attributes if GlobalLongTermMemoryServicerImpl constructor or methods use them
        self.mock_config.GLM_STORAGE_BACKEND_TYPE = "default_sqlite_qdrant" # Ensure it tries to load the repo we are testing/mocking parts of
        self.mock_config.DB_FILENAME = "dummy_test.db" # Required by DefaultGLMRepository
        self.mock_config.QDRANT_HOST = "mockhost" # Required by DefaultGLMRepository
        self.mock_config.QDRANT_PORT = 1234 # Required by DefaultGLMRepository
        self.mock_config.QDRANT_COLLECTION = "mock_collection" # Required by DefaultGLMRepository
        self.mock_config.SQLITE_CONNECT_TIMEOUT_S = 5
        self.mock_config.QDRANT_CLIENT_TIMEOUT_S = 5
        self.mock_config.QDRANT_DEFAULT_DISTANCE_METRIC = "COSINE"


        # Patch DefaultGLMRepository itself to control its instantiation if needed,
        # or ensure its dependencies (SqliteKemRepository, QdrantKemRepository) are fully mocked.
        # For now, we are mocking QdrantClient and sqlite3.connect which are used by these repositories.
        # This should be sufficient if DefaultGLMRepository is well-behaved with these mocks.
        self.servicer = GlobalLongTermMemoryServicerImpl()


    def tearDown(self):
        self.mock_qdrant_client_patch.stop()
        self.mock_sqlite3_connect_patch.stop()
        self.mock_config_patch.stop()


    def test_batch_store_all_successful_no_embeddings(self):
        kems_in = [create_proto_kem(id=f"kem{i}") for i in range(2)]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)

        # Mock the store_kem method of the storage_repository
        async def mock_store_kem(kem):
            # Simulate successful storage and return the KEM (possibly with generated ID/timestamps)
            kem.id = kem.id or str(uuid.uuid4()) # Ensure ID
            return kem

        # Since batch_store_kems calls store_kem in a loop, we mock store_kem on the instance
        self.servicer.storage_repository.store_kem = MagicMock(side_effect=lambda k: asyncio.Future().set_result(mock_store_kem(k)))

        # Make batch_store_kems itself an async mock if it's declared async
        # Or mock its internal asyncio.run if it calls other async methods that way
        async def mock_batch_store_kems(kems_list):
            successful = []
            failed = []
            for k in kems_list:
                try:
                    # Simulate the check for embedding dimension
                    if k.embeddings and len(k.embeddings) != self.servicer.config.DEFAULT_VECTOR_SIZE:
                         failed.append(k.id or "unknown_ref")
                         continue
                    stored_k = await mock_store_kem(k) # Await the async mock
                    successful.append(stored_k)
                except Exception:
                    failed.append(k.id or "unknown_ref")
            return successful, failed

        with patch.object(self.servicer.storage_repository, 'batch_store_kems', side_effect=mock_batch_store_kems) as mock_repo_batch_store:
            response = self.servicer.BatchStoreKEMs(request, None)

            self.assertEqual(len(response.successfully_stored_kems), 2)
            self.assertEqual(len(response.failed_kem_references), 0)
            self.assertEqual(response.overall_error_message, "")
            mock_repo_batch_store.assert_called_once()


    def test_batch_store_all_successful_with_embeddings(self):
        kems_in = [create_proto_kem(id=f"kem{i}", embeddings=[0.1]*TEST_DEFAULT_VECTOR_SIZE) for i in range(2)]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)

        async def mock_store_kem(kem):
            kem.id = kem.id or str(uuid.uuid4())
            return kem

        async def mock_batch_store_kems(kems_list):
            successful = []
            for k in kems_list: successful.append(await mock_store_kem(k))
            return successful, []

        with patch.object(self.servicer.storage_repository, 'batch_store_kems', side_effect=mock_batch_store_kems) as mock_repo_batch_store:
            response = self.servicer.BatchStoreKEMs(request, None)

            self.assertEqual(len(response.successfully_stored_kems), 2)
            self.assertEqual(len(response.failed_kem_references), 0)
            mock_repo_batch_store.assert_called_once()


    def test_batch_store_sqlite_error_for_one_kem(self):
        kems_in = [
            create_proto_kem(id="kem1", embeddings=[0.1]*TEST_DEFAULT_VECTOR_SIZE),
            create_proto_kem(id="kem2", embeddings=[0.2]*TEST_DEFAULT_VECTOR_SIZE)
        ]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)

        async def mock_batch_store_kems_with_failure(kems_list):
            # Simulate failure for "kem2"
            successful = [kems_list[0]] # kem1 succeeds
            failed = ["kem2"]
            return successful, failed

        with patch.object(self.servicer.storage_repository, 'batch_store_kems', side_effect=mock_batch_store_kems_with_failure):
            response = self.servicer.BatchStoreKEMs(request, None)

            self.assertEqual(len(response.successfully_stored_kems), 1)
            self.assertEqual(response.successfully_stored_kems[0].id, "kem1")
            self.assertEqual(len(response.failed_kem_references), 1)
            self.assertIn("kem2", response.failed_kem_references)


    def test_batch_store_qdrant_error_for_one_kem(self):
        kems_in = [
            create_proto_kem(id="ok_kem", embeddings=[0.1]*TEST_DEFAULT_VECTOR_SIZE),
            create_proto_kem(id="qdrant_fail_kem", embeddings=[0.2]*TEST_DEFAULT_VECTOR_SIZE)
        ]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)

        async def mock_batch_store_kems_with_qdrant_failure(kems_list):
            # Simulate failure for "qdrant_fail_kem"
            successful = []
            failed = []
            for k in kems_list:
                if k.id == "ok_kem":
                    successful.append(k)
                elif k.id == "qdrant_fail_kem":
                    failed.append(k.id) # Or simulate the repo raising an error that BatchStoreKEMs then catches
            return successful, failed

        with patch.object(self.servicer.storage_repository, 'batch_store_kems', side_effect=mock_batch_store_kems_with_qdrant_failure):
            response = self.servicer.BatchStoreKEMs(request, None)

            self.assertEqual(len(response.successfully_stored_kems), 1)
            self.assertEqual(response.successfully_stored_kems[0].id, "ok_kem")
            self.assertEqual(len(response.failed_kem_references), 1)
            self.assertIn("qdrant_fail_kem", response.failed_kem_references)


if __name__ == '__main__':
    unittest.main()


# --- New Test Class for RetrieveKEMs ---
from dcs_memory.services.glm.app.repositories import DefaultGLMRepository, SqliteKemRepository
from dcs_memory.services.glm.app.config import GLMConfig
import asyncio # For running async repository methods if servicer calls them directly

# Helper to run async methods in tests if needed
def async_test(f):
    def wrapper(*args, **kwargs):
        asyncio.run(f(*args, **kwargs))
    return wrapper

class TestGLMRetrieveKEMs(unittest.TestCase):
    def setUp(self):
        # Config for an in-memory SQLite database
        self.config = GLMConfig(
            DB_FILENAME=":memory:",
            GLM_STORAGE_BACKEND_TYPE="default_sqlite_qdrant",
            QDRANT_HOST="", # Disable Qdrant for these SQLite focused tests
            DEFAULT_PAGE_SIZE=2 # Small page size for easier pagination testing
        )

        # Patch QdrantClient globally for DefaultGLMRepository to avoid real connections
        # We are testing SQLite behavior here.
        self.mock_qdrant_client_patch = patch('dcs_memory.services.glm.app.repositories.default_impl.QdrantClient')
        self.MockQdrantClientClass = self.mock_qdrant_client_patch.start()
        self.mock_qdrant_instance = self.MockQdrantClientClass.return_value
        self.mock_qdrant_instance.get_collections.return_value = MagicMock() # Allow DefaultGLMRepository to initialize
        self.mock_qdrant_instance.search_points.return_value = [] # Default Qdrant search result


        # The servicer will internally create DefaultGLMRepository,
        # which will use SqliteKemRepository with the in-memory DB.
        # We need to ensure 'app_dir' is correctly inferred or passed if DefaultGLMRepository needs it.
        # DefaultGLMRepository(config, app_dir) - app_dir is used for db_path.
        # For :memory:, app_dir doesn't matter as much for db_path construction.
        # Patch GLMConfig instance used by the servicer
        self.mock_main_config_patch = patch('dcs_memory.services.glm.app.main.config', self.config)
        self.mock_main_config_patch.start()

        # Patch app_dir used by DefaultGLMRepository if it's module level, or ensure servicer passes it.
        # In main.py, app_dir is module level.
        self.mock_app_dir_patch = patch('dcs_memory.services.glm.app.main.app_dir', '.') # Mock app_dir
        self.mock_app_dir_patch.start()


        self.servicer = GlobalLongTermMemoryServicerImpl() # This will init DefaultGLMRepository

        # Helper to directly interact with the SQLite repo for setup if needed
        self.sqlite_repo: SqliteKemRepository = self.servicer.storage_repository.sqlite_repo


        # Pre-populate some data
        self.kems_data = []
        for i in range(5): # 5 KEMs for pagination testing (2 pages of 2, 1 page of 1)
            ts = Timestamp()
            ts.FromSeconds(1700000000 + i * 100) # Vary timestamps for ordering
            kem_id = f"kem_id_{i}"
            metadata = {"type": f"type{i%2}", "source_system": "test_system", "index": str(i)}
            content = f"Content for KEM {i}"

            kem_proto = create_proto_kem(id=kem_id, metadata=metadata, content=content)
            kem_proto.created_at.CopyFrom(ts)
            kem_proto.updated_at.CopyFrom(ts) # For simplicity, updated_at = created_at

            self.kems_data.append(kem_proto)
            # Store directly using the repository for setup
            # The servicer's StoreKEM calls asyncio.run, which can be tricky in sync tests.
            # Direct repo call, assuming it's synchronous for setup.
            self.servicer.storage_repository.sqlite_repo.store_or_replace_kem(
                kem_id=kem_proto.id,
                content_type=kem_proto.content_type,
                content=kem_proto.content,
                metadata_json=json.dumps(dict(kem_proto.metadata)),
                created_at_iso=ts.ToDatetime().isoformat().split('.')[0], # Match format
                updated_at_iso=ts.ToDatetime().isoformat().split('.')[0]  # Match format
            )
        # KEMs sorted by updated_at DESC (which is ts DESC here): kem_id_4, kem_id_3, kem_id_2, kem_id_1, kem_id_0

    def tearDown(self):
        self.mock_qdrant_client_patch.stop()
        self.mock_main_config_patch.stop()
        self.mock_app_dir_patch.stop()
        # In-memory DB is automatically discarded

    def test_retrieve_with_metadata_filter(self):
        query = glm_service_pb2.KEMQuery(
            metadata_filters={"type": "type0"} # Should match kem_id_0, kem_id_2, kem_id_4
        )
        request = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=5)

        response = self.servicer.RetrieveKEMs(request, None)

        self.assertEqual(len(response.kems), 3)
        retrieved_ids = sorted([k.id for k in response.kems])
        self.assertEqual(retrieved_ids, ["kem_id_0", "kem_id_2", "kem_id_4"])
        self.assertEqual(response.next_page_token, "") # All results fit

    def test_retrieve_keyset_pagination(self):
        # Order by updated_at DESC, id DESC (default in DefaultGLMRepository.retrieve_kems)
        # Expected order: kem_id_4, kem_id_3, kem_id_2, kem_id_1, kem_id_0 (since updated_at is increasing with i)

        # Page 1
        query1 = glm_service_pb2.KEMQuery()
        request1 = glm_service_pb2.RetrieveKEMsRequest(query=query1, page_size=2) # page_size = 2
        response1 = self.servicer.RetrieveKEMs(request1, None)

        self.assertEqual(len(response1.kems), 2)
        self.assertEqual(response1.kems[0].id, "kem_id_4")
        self.assertEqual(response1.kems[1].id, "kem_id_3")
        self.assertIsNotNone(response1.next_page_token)
        self.assertNotEqual(response1.next_page_token, "")

        # Page 2
        query2 = glm_service_pb2.KEMQuery()
        request2 = glm_service_pb2.RetrieveKEMsRequest(query=query2, page_size=2, page_token=response1.next_page_token)
        response2 = self.servicer.RetrieveKEMs(request2, None)

        self.assertEqual(len(response2.kems), 2)
        self.assertEqual(response2.kems[0].id, "kem_id_2")
        self.assertEqual(response2.kems[1].id, "kem_id_1")
        self.assertIsNotNone(response2.next_page_token)
        self.assertNotEqual(response2.next_page_token, "")

        # Page 3
        query3 = glm_service_pb2.KEMQuery()
        request3 = glm_service_pb2.RetrieveKEMsRequest(query=query3, page_size=2, page_token=response2.next_page_token)
        response3 = self.servicer.RetrieveKEMs(request3, None)

        self.assertEqual(len(response3.kems), 1)
        self.assertEqual(response3.kems[0].id, "kem_id_0")
        self.assertEqual(response3.next_page_token, "") # Last page

    def test_retrieve_metadata_filter_with_pagination(self):
        # Filter for type1: kem_id_1, kem_id_3
        # Expected order: kem_id_3, kem_id_1
        query = glm_service_pb2.KEMQuery(metadata_filters={"type": "type1"})

        # Page 1
        request1 = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=1)
        response1 = self.servicer.RetrieveKEMs(request1, None)
        self.assertEqual(len(response1.kems), 1)
        self.assertEqual(response1.kems[0].id, "kem_id_3") # Highest updated_at for type1
        self.assertNotEqual(response1.next_page_token, "")

        # Page 2
        request2 = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=1, page_token=response1.next_page_token)
        response2 = self.servicer.RetrieveKEMs(request2, None)
        self.assertEqual(len(response2.kems), 1)
        self.assertEqual(response2.kems[0].id, "kem_id_1")
        self.assertEqual(response2.next_page_token, "") # Last page for this filter


    # --- Tests for External Data Source Dispatch ---

    async def mock_external_retrieve_kems(self, page_size: int, page_token: Optional[str]):
        # Simple mock implementation for BaseExternalRepository
        # In a real test for a connector, this would interact with a mock DB or return specific data
        mock_kem = create_proto_kem(id="ext_kem_1", content="External KEM content", metadata={"source": "external_db_A"})
        if page_token is None: # First page
            return [mock_kem], "next_ext_token"
        elif page_token == "next_ext_token": # Second page
            mock_kem_2 = create_proto_kem(id="ext_kem_2", content="External KEM content 2", metadata={"source": "external_db_A"})
            return [mock_kem_2], None # No more pages
        return [], None

    def test_retrieve_kems_dispatch_to_external_source(self):
        # 1. Configure an external data source in the config used by the servicer
        mock_external_source_name = "my_external_db"
        self.servicer.storage_repository.external_repos[mock_external_source_name] = MagicMock(spec=BaseExternalRepository)

        # Make the mocked method an async mock
        mock_external_repo_instance = self.servicer.storage_repository.external_repos[mock_external_source_name]

        # Wrap the synchronous mock_external_retrieve_kems in an async function for side_effect
        async def async_mock_retriever(*args, **kwargs):
            return await self.mock_external_retrieve_kems(*args, **kwargs)

        mock_external_repo_instance.retrieve_mapped_kems = MagicMock(side_effect=async_mock_retriever)

        # 2. Call RetrieveKEMs with data_source_name
        query = glm_service_pb2.KEMQuery(data_source_name=mock_external_source_name)
        request = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=1)

        response = self.servicer.RetrieveKEMs(request, None) # Servicer calls asyncio.run internally

        # 3. Assert that the external repository's method was called
        mock_external_repo_instance.retrieve_mapped_kems.assert_called_once_with(
            page_size=1,
            page_token=None # Initial call has no page_token
        )
        self.assertEqual(len(response.kems), 1)
        self.assertEqual(response.kems[0].id, "ext_kem_1")
        self.assertEqual(response.next_page_token, "next_ext_token")

        # Test fetching the next page from external source
        request_page2 = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=1, page_token="next_ext_token")
        response_page2 = self.servicer.RetrieveKEMs(request_page2, None)

        mock_external_repo_instance.retrieve_mapped_kems.assert_called_with(
            page_size=1,
            page_token="next_ext_token"
        )
        self.assertEqual(len(response_page2.kems), 1)
        self.assertEqual(response_page2.kems[0].id, "ext_kem_2")
        self.assertIsNone(response_page2.next_page_token or None) # Ensure it's empty or None


    def test_retrieve_kems_fall_back_to_native_if_no_data_source_name(self):
        # Ensure Qdrant mock is in place for native vector search path if triggered
        self.mock_qdrant_instance.search_points.return_value = [] # No Qdrant results

        # Spy on the native SQLite retrieval method
        with patch.object(self.servicer.storage_repository.sqlite_repo, 'retrieve_kems_from_db',
                          wraps=self.servicer.storage_repository.sqlite_repo.retrieve_kems_from_db) as mock_sqlite_retrieve:

            query = glm_service_pb2.KEMQuery() # No data_source_name
            request = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=2)

            response = self.servicer.RetrieveKEMs(request, None)

            mock_sqlite_retrieve.assert_called_once()
            self.assertEqual(len(response.kems), 2) # Should get from pre-populated native SQLite data
            self.assertEqual(response.kems[0].id, "kem_id_4") # Default order from setup

    def test_retrieve_kems_error_if_unknown_data_source_name(self):
        query = glm_service_pb2.KEMQuery(data_source_name="non_existent_source")
        request = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=2)

        # The servicer should catch InvalidQueryError from DefaultGLMRepository and abort
        # For unittest, we can check the context manager for grpc.RpcError (which abort raises)
        # This requires running the servicer method in a way that gRPC context is available or mocked.
        # Simpler: check if DefaultGLMRepository.retrieve_kems raises InvalidQueryError

        # Patch asyncio.run to avoid issues if called within a running loop (e.g. by async_test)
        # and to directly test the async method of the repository.
        with patch('asyncio.run') as mock_asyncio_run:
            # Define a side effect for asyncio.run that simply executes the coroutine
            async def immediate_coro_runner(coro):
                return await coro
            mock_asyncio_run.side_effect = immediate_coro_runner

            with self.assertRaises(grpc.RpcError) as context: # type: ignore
                 self.servicer.RetrieveKEMs(request, MagicMock()) # MagicMock for gRPC context

            self.assertEqual(context.exception.code(), grpc.StatusCode.INVALID_ARGUMENT) # type: ignore
            self.assertIn("External data source 'non_existent_source' not available", context.exception.details()) # type: ignore

# Need to import BaseExternalRepository for type hinting and MagicMock spec
from dcs_memory.services.glm.app.repositories.external_base import BaseExternalRepository
from typing import Optional # For Optional type hint
import grpc # For grpc.RpcError and grpc.StatusCode
