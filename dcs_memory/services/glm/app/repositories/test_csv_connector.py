import unittest
from unittest.mock import MagicMock, patch, mock_open
import asyncio
import csv
import io
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

# Project imports
from dcs_memory.services.glm.app.config import ExternalDataSourceConfig, GLMConfig
from dcs_memory.services.glm.app.repositories.csv_connector import CsvDirExternalRepository
from dcs_memory.services.glm.app.repositories.base import BackendUnavailableError, StorageError
from dcs_memory.services.glm.generated_grpc import kem_pb2, glm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp

# Helper to run async test methods
def async_test(f):
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper

class TestCsvDirExternalRepository(unittest.TestCase):

    def setUp(self):
        self.glm_config = GLMConfig()
        self.base_mapping_config_csv = {
            "directory_path": "/test/csvs",
            "id_column_name": "DocID",
            "content_column_names": ["Title", "Abstract"],
            "timestamp_column_name": "ModifiedDate"
        }
        self.default_csv_config = ExternalDataSourceConfig(
            name="test_csv_source",
            type="csv_dir",
            mapping_config=self.base_mapping_config_csv
        )

    def test_init_success_minimal_config(self):
        repo = CsvDirExternalRepository(self.default_csv_config, self.glm_config)
        self.assertEqual(repo.directory_path, "/test/csvs")
        self.assertEqual(repo.id_column_name, "DocID")
        self.assertEqual(repo.content_column_names, ["Title", "Abstract"])
        self.assertEqual(repo.timestamp_column_name, "ModifiedDate")
        self.assertEqual(repo.csv_delimiter, ",") # Default
        self.assertEqual(repo.encoding, "utf-8") # Default
        self.assertEqual(repo.created_at_column_name, "ModifiedDate") # Default

    def test_init_failure_missing_required_fields(self):
        incomplete_mapping = {"directory_path": "/test"}
        config = ExternalDataSourceConfig(name="test_csv_fail", type="csv_dir", mapping_config=incomplete_mapping)
        with self.assertRaises(ValueError) as context:
            CsvDirExternalRepository(config, self.glm_config)
        self.assertIn("Missing required mapping_config fields", str(context.exception))

    def test_init_failure_invalid_content_column_names(self):
        bad_mapping = {**self.base_mapping_config_csv, "content_column_names": []} # Empty list
        config = ExternalDataSourceConfig(name="test_csv_fail_content", type="csv_dir", mapping_config=bad_mapping)
        with self.assertRaises(ValueError) as context:
            CsvDirExternalRepository(config, self.glm_config)
        self.assertIn("'content_column_names' must be a non-empty list", str(context.exception))

        bad_mapping_type = {**self.base_mapping_config_csv, "content_column_names": "NotAList"}
        config_type = ExternalDataSourceConfig(name="test_csv_fail_content_type", type="csv_dir", mapping_config=bad_mapping_type)
        with self.assertRaises(ValueError) as context: # Caught by isinstance check
            CsvDirExternalRepository(config_type, self.glm_config)
        self.assertIn("'content_column_names' must be a non-empty list", str(context.exception))


    @patch('os.path.isdir')
    @patch('os.access')
    async def test_check_health_success(self, mock_access, mock_isdir):
        mock_isdir.return_value = True
        mock_access.return_value = True
        repo = CsvDirExternalRepository(self.default_csv_config, self.glm_config)
        healthy, msg = await repo.check_health()
        self.assertTrue(healthy)
        self.assertIn("is accessible", msg)
        mock_isdir.assert_called_once_with("/test/csvs")
        mock_access.assert_called_once_with("/test/csvs", os.R_OK)

    @patch('os.path.isdir')
    async def test_check_health_not_a_directory(self, mock_isdir):
        mock_isdir.return_value = False
        repo = CsvDirExternalRepository(self.default_csv_config, self.glm_config)
        healthy, msg = await repo.check_health()
        self.assertFalse(healthy)
        self.assertIn("not found or is not a directory", msg)

    @patch('os.path.isdir', return_value=True)
    @patch('os.access', return_value=False)
    async def test_check_health_not_readable(self, mock_access, mock_isdir):
        repo = CsvDirExternalRepository(self.default_csv_config, self.glm_config)
        healthy, msg = await repo.check_health()
        self.assertFalse(healthy)
        self.assertIn("not readable", msg)

    @async_test
    @patch('glob.glob')
    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.DictReader')
    async def test_retrieve_mapped_kems_basic_flow(self, MockDictReader, mock_file_open, mock_glob):
        repo = CsvDirExternalRepository(self.default_csv_config, self.glm_config)

        # Mock file system
        mock_glob.return_value = ["/test/csvs/file1.csv"]

        # Mock CSV content
        csv_data_file1 = [
            {"DocID": "csv1", "Title": "Report Alpha", "Abstract": "About A", "ModifiedDate": "2023-01-01T12:00:00Z", "Category": "Internal"},
            {"DocID": "csv2", "Title": "Paper Beta", "Abstract": "About B", "ModifiedDate": "2023-01-02T12:00:00Z", "Category": "External"},
            {"DocID": "csv3", "Title": "Doc Gamma", "Abstract": "About C", "ModifiedDate": "2023-01-01T10:00:00Z", "Category": "Internal"}, # Earlier time
        ]
        # This setup for DictReader is a bit tricky with mock_open.
        # It's often easier to mock DictReader's return value directly if open is complex.
        # Let's assume DictReader is called with the file object from mock_open.
        # The mock_open().read() or iteration needs to yield lines for DictReader.

        # Simplified: Mock DictReader to return our data directly
        MockDictReader.return_value = iter(csv_data_file1) # Make it an iterator

        # Configure metadata mapping
        repo.metadata_column_names = ["Category"]

        # Test: No filters, page_size large enough for all
        query = glm_service_pb2.KEMQuery()
        kems, next_token = await repo.retrieve_mapped_kems(internal_query=query, page_size=5, page_token=None)

        self.assertEqual(len(kems), 3)
        self.assertIsNone(next_token) # All items fit

        # Check sorting (updated_at DESC, id DESC)
        # Expected order: csv2 (2023-01-02), csv1 (2023-01-01T12), csv3 (2023-01-01T10)
        self.assertEqual(kems[0].id, "csv2")
        self.assertEqual(kems[1].id, "csv1")
        self.assertEqual(kems[2].id, "csv3")

        # Check content and metadata of one KEM
        kem_csv2 = kems[0]
        self.assertEqual(kem_csv2.content.decode('utf-8'), "Paper Beta About B")
        self.assertEqual(kem_csv2.metadata["Category"], "External")
        ts_csv2 = datetime(2023, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(kem_csv2.updated_at.seconds, int(ts_csv2.timestamp()))

        # Test: With metadata filter
        MockDictReader.return_value = iter(csv_data_file1) # Reset iterator
        query_filtered = glm_service_pb2.KEMQuery(metadata_filters={"Category": "Internal"})
        kems_filtered, _ = await repo.retrieve_mapped_kems(internal_query=query_filtered, page_size=5, page_token=None)

        self.assertEqual(len(kems_filtered), 2)
        retrieved_ids_filtered = sorted([k.id for k in kems_filtered])
        self.assertEqual(retrieved_ids_filtered, ["csv1", "csv3"])

        # Test: Pagination
        MockDictReader.return_value = iter(csv_data_file1) # Reset iterator for DictReader
        query_page = glm_service_pb2.KEMQuery()

        # Page 1
        kems_p1, token_p1 = await repo.retrieve_mapped_kems(internal_query=query_page, page_size=1, page_token=None)
        self.assertEqual(len(kems_p1), 1)
        self.assertEqual(kems_p1[0].id, "csv2") # Highest timestamp
        self.assertEqual(token_p1, "1") # Next offset

        # Page 2
        MockDictReader.return_value = iter(csv_data_file1) # Reset iterator
        kems_p2, token_p2 = await repo.retrieve_mapped_kems(internal_query=query_page, page_size=1, page_token=token_p1)
        self.assertEqual(len(kems_p2), 1)
        self.assertEqual(kems_p2[0].id, "csv1")
        self.assertEqual(token_p2, "2")

        # Page 3
        MockDictReader.return_value = iter(csv_data_file1) # Reset iterator
        kems_p3, token_p3 = await repo.retrieve_mapped_kems(internal_query=query_page, page_size=1, page_token=token_p2)
        self.assertEqual(len(kems_p3), 1)
        self.assertEqual(kems_p3[0].id, "csv3")
        self.assertIsNone(token_p3) # Last page

if __name__ == '__main__':
    unittest.main()
```
