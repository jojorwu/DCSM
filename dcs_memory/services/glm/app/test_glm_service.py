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
