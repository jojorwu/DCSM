import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import asyncio
from typing import List, Dict, Any, Optional

import asyncpg # For exception types

# Project imports
from dcs_memory.services.glm.app.config import ExternalDataSourceConfig, GLMConfig
from dcs_memory.services.glm.app.repositories.postgres_connector import PostgresExternalRepository
from dcs_memory.services.glm.app.repositories.base import BackendUnavailableError, StorageError
from dcs_memory.services.glm.generated_grpc import kem_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import datetime

# Helper to run async test methods
def async_test(f):
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper

class TestPostgresExternalRepository(unittest.TestCase):

    def setUp(self):
        self.glm_config = GLMConfig() # Default GLMConfig
        self.base_mapping_config = {
            "table_name": "test_table",
            "id_column": "doc_id",
            "content_column": "doc_content",
            "timestamp_sort_column": "updated_ts"
        }
        self.default_config = ExternalDataSourceConfig(
            name="test_pg",
            type="postgresql",
            connection_details={"host": "localhost", "user": "test"}, # Dummy details
            mapping_config=self.base_mapping_config
        )

    def test_init_success_minimal_config(self):
        repo = PostgresExternalRepository(self.default_config, self.glm_config)
        self.assertEqual(repo.table_name, "test_table")
        self.assertEqual(repo.id_column, "doc_id")
        self.assertEqual(repo.content_column, "doc_content")
        self.assertEqual(repo.timestamp_sort_column, "updated_ts")
        self.assertEqual(repo.content_type_value, "text/plain") # Default
        self.assertEqual(repo.id_tiebreaker_column, "doc_id") # Default
        self.assertIsNone(repo.metadata_json_column)
        self.assertEqual(repo.metadata_columns_map, {})

    def test_init_success_full_config(self):
        full_mapping = {
            **self.base_mapping_config,
            "content_type_value": "application/json",
            "id_tiebreaker_column": "another_id_col",
            "metadata_json_column": "meta_json_col",
            "metadata_columns_map": {"key1": "pg_col1"},
            "created_at_column": "created_ts_col",
            "updated_at_column": "updated_ts_col" # Explicitly same as sort for this test
        }
        config = ExternalDataSourceConfig(
            name="test_pg_full", type="postgresql",
            connection_details={"host": "dummy"}, mapping_config=full_mapping
        )
        repo = PostgresExternalRepository(config, self.glm_config)
        self.assertEqual(repo.content_type_value, "application/json")
        self.assertEqual(repo.id_tiebreaker_column, "another_id_col")
        self.assertEqual(repo.metadata_json_column, "meta_json_col")
        self.assertEqual(repo.metadata_columns_map, {"key1": "pg_col1"})
        self.assertEqual(repo.created_at_column, "created_ts_col")
        self.assertEqual(repo.updated_at_column, "updated_ts_col")


    def test_init_failure_missing_required_fields(self):
        incomplete_mapping = {"table_name": "test"}
        config = ExternalDataSourceConfig(
            name="test_pg_fail", type="postgresql",
            connection_details={"host": "dummy"}, mapping_config=incomplete_mapping
        )
        with self.assertRaises(ValueError) as context:
            PostgresExternalRepository(config, self.glm_config)
        self.assertIn("Missing required mapping_config fields", str(context.exception))

    @async_test
    @patch('asyncpg.create_pool')
    async def test_connect_success(self, mock_create_pool):
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn # For 'async with'
        mock_conn.fetchval.return_value = 1 # For test query
        mock_create_pool.return_value = mock_pool

        repo = PostgresExternalRepository(self.default_config, self.glm_config)
        await repo.connect()

        self.assertIsNotNone(repo.pool)
        mock_create_pool.assert_called_once()
        mock_conn.fetchval.assert_called_once_with("SELECT 1")
        await repo.disconnect() # Clean up

    @async_test
    @patch('asyncpg.create_pool')
    async def test_connect_failure_uri(self, mock_create_pool):
        mock_create_pool.side_effect = asyncpg.exceptions.InvalidConnectionNameError("test failure")

        config_uri = ExternalDataSourceConfig(
            name="test_pg_uri_fail", type="postgresql",
            connection_uri="postgresql://invalid", mapping_config=self.base_mapping_config
        )
        repo = PostgresExternalRepository(config_uri, self.glm_config)

        with self.assertRaises(BackendUnavailableError):
            await repo.connect()
        self.assertIsNone(repo.pool)

    @async_test
    async def test_check_health_not_connected(self):
        repo = PostgresExternalRepository(self.default_config, self.glm_config)
        healthy, msg = await repo.check_health()
        self.assertFalse(healthy)
        self.assertIn("Not connected", msg)

    @async_test
    @patch('asyncpg.create_pool')
    async def test_check_health_connected_healthy(self, mock_create_pool):
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetchval.return_value = 1
        mock_create_pool.return_value = mock_pool

        repo = PostgresExternalRepository(self.default_config, self.glm_config)
        await repo.connect()
        healthy, msg = await repo.check_health()
        self.assertTrue(healthy)
        self.assertIn("Healthy", msg)
        await repo.disconnect()

    @async_test
    @patch('asyncpg.create_pool')
    async def test_check_health_query_fails(self, mock_create_pool):
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetchval.side_effect = asyncpg.exceptions.PostgresError("DB error")
        mock_create_pool.return_value = mock_pool

        repo = PostgresExternalRepository(self.default_config, self.glm_config)
        await repo.connect() # This will succeed based on current connect logic
        healthy, msg = await repo.check_health()
        self.assertFalse(healthy)
        self.assertIn("Health check failed", msg)
        await repo.disconnect()

    @async_test
    @patch('asyncpg.create_pool')
    async def test_retrieve_mapped_kems_basic_mapping_no_pagination(self, mock_create_pool):
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # Mock asyncpg.Record manually or use MagicMock with spec
        # For simplicity, let's use MagicMock and configure .get for expected columns
        mock_pg_row1 = MagicMock()
        # Simulate asyncpg returning timezone-aware datetime for TIMESTAMPTZ
        aware_datetime_updated = datetime(2023, 1, 1, 10, 0, 0, microsecond=123456, tzinfo=timezone.utc)
        aware_datetime_created = datetime(2023, 1, 1, 9, 0, 0, microsecond=654321, tzinfo=timezone.utc)

        mock_pg_row1.get.side_effect = lambda key, default=None: {
            "doc_id": "pg_kem_1", "doc_content": "Content from PG 1",
            "updated_ts": aware_datetime_updated,
            "created_ts_col": aware_datetime_created,
            "meta_json_col": {"tag": "alpha", "version": 1.0},
            "pg_col_for_meta1": "value1"
        }.get(key, default)

        mock_conn.fetch.return_value = [mock_pg_row1] # Returns a list of records
        mock_create_pool.return_value = mock_pool

        mapping_config = {
            "table_name": "docs", "id_column": "doc_id", "content_column": "doc_content",
            "timestamp_sort_column": "updated_ts", "created_at_column": "created_ts_col",
            "metadata_json_column": "meta_json_col",
            "metadata_columns_map": {"custom_field": "pg_col_for_meta1"},
            "assume_naive_timestamp_is_utc": True # Explicit for clarity
        }
        config = ExternalDataSourceConfig(
            name="pg_test_retrieve", type="postgresql",
            connection_details={"host": "dummy"}, mapping_config=mapping_config
        )
        repo = PostgresExternalRepository(config, self.glm_config)
        await repo.connect()

        kems, next_token = await repo.retrieve_mapped_kems(page_size=5, page_token=None)
        await repo.disconnect()

        self.assertEqual(len(kems), 1)
        self.assertIsNone(next_token)

        kem1 = kems[0]
        self.assertEqual(kem1.id, "pg_kem_1")
        self.assertEqual(kem1.content.decode('utf-8'), "Content from PG 1")
        self.assertEqual(kem1.content_type, "text/plain") # Default

        # Timestamps (seconds since epoch & nanos)
        self.assertEqual(kem1.created_at.seconds, int(aware_datetime_created.timestamp()))
        self.assertEqual(kem1.created_at.nanos, aware_datetime_created.microsecond * 1000)
        self.assertEqual(kem1.updated_at.seconds, int(aware_datetime_updated.timestamp()))
        self.assertEqual(kem1.updated_at.nanos, aware_datetime_updated.microsecond * 1000)

        self.assertEqual(kem1.metadata["tag"], "alpha")
        self.assertEqual(kem1.metadata["version"], "1.0") # Pydantic converts numbers to string for map<string,string>
        self.assertEqual(kem1.metadata["custom_field"], "value1")

        # Verify the SQL query (simplistic check, better to assert on mock_conn.fetch call)
        # This requires knowing the exact generated query string.
        # Example: mock_conn.fetch.assert_called_once_with(expected_sql_str, *expected_params)
        # The query construction is complex, so this is just a placeholder for a real test.
        self.assertTrue(mock_conn.fetch.called)
        call_args = mock_conn.fetch.call_args[0] # (query_string, *params)
        query_string = call_args[0]
        query_params = call_args[1:] # The actual parameters passed after the query string

        self.assertIn('SELECT "created_ts_col", "doc_content", "doc_id", "meta_json_col", "pg_col_for_meta1", "updated_ts" FROM "docs"', query_string)
        self.assertIn('ORDER BY "updated_ts" DESC, "doc_id" DESC LIMIT $1', query_string)
        # Parameters for query_params start from $1. Here, only LIMIT is a parameter.
        self.assertEqual(query_params[0], 6) # page_size + 1


    @async_test
    @patch('asyncpg.create_pool')
    async def test_retrieve_with_single_metadata_filter(self, mock_create_pool):
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = [] # We are primarily testing query construction here
        mock_create_pool.return_value = mock_pool

        mapping_config_filterable = {
            **self.base_mapping_config,
            "filterable_metadata_columns": {"category": "pg_category_col"}
        }
        config = ExternalDataSourceConfig(
            name="pg_filter_test", type="postgresql",
            connection_details={"host": "dummy"}, mapping_config=mapping_config_filterable
        )
        repo = PostgresExternalRepository(config, self.glm_config)
        await repo.connect()

        query = glm_service_pb2.KEMQuery(metadata_filters={"category": "urgent"})
        await repo.retrieve_mapped_kems(internal_query=query, page_size=5, page_token=None)
        await repo.disconnect()

        self.assertTrue(mock_conn.fetch.called)
        call_args = mock_conn.fetch.call_args[0]
        query_string = call_args[0]
        query_params = call_args[1:]

        self.assertIn('WHERE "pg_category_col" = $1', query_string)
        self.assertIn('ORDER BY "updated_ts" DESC, "doc_id" DESC LIMIT $2', query_string)
        self.assertEqual(len(query_params), 2)
        self.assertEqual(query_params[0], "urgent") # Value for the filter
        self.assertEqual(query_params[1], 6)       # page_size + 1 for limit

    @async_test
    @patch('asyncpg.create_pool')
    async def test_retrieve_with_multiple_metadata_filters(self, mock_create_pool):
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []
        mock_create_pool.return_value = mock_pool

        mapping_config_filterable = {
            **self.base_mapping_config,
            "filterable_metadata_columns": {
                "category": "pg_category_col",
                "status": "pg_status_col"
            }
        }
        config = ExternalDataSourceConfig(
            name="pg_multi_filter_test", type="postgresql",
            connection_details={"host": "dummy"}, mapping_config=mapping_config_filterable
        )
        repo = PostgresExternalRepository(config, self.glm_config)
        await repo.connect()

        query = glm_service_pb2.KEMQuery(metadata_filters={"category": "urgent", "status": "pending"})
        await repo.retrieve_mapped_kems(internal_query=query, page_size=5, page_token=None)
        await repo.disconnect()

        self.assertTrue(mock_conn.fetch.called)
        call_args = mock_conn.fetch.call_args[0]
        query_string = call_args[0]
        query_params = call_args[1:]

        self.assertIn('WHERE "pg_category_col" = $1 AND "pg_status_col" = $2', query_string)
        self.assertIn('ORDER BY "updated_ts" DESC, "doc_id" DESC LIMIT $3', query_string)
        self.assertEqual(len(query_params), 3)
        self.assertEqual(query_params[0], "urgent")
        self.assertEqual(query_params[1], "pending")
        self.assertEqual(query_params[2], 6)

    @async_test
    @patch('asyncpg.create_pool')
    async def test_retrieve_ignores_non_filterable_metadata_key(self, mock_create_pool):
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []
        mock_create_pool.return_value = mock_pool

        mapping_config_filterable = {
            **self.base_mapping_config,
            "filterable_metadata_columns": {"category": "pg_category_col"} # Only category is filterable
        }
        config = ExternalDataSourceConfig(
            name="pg_ignore_filter_test", type="postgresql",
            connection_details={"host": "dummy"}, mapping_config=mapping_config_filterable
        )
        repo = PostgresExternalRepository(config, self.glm_config)
        await repo.connect()

        query = glm_service_pb2.KEMQuery(metadata_filters={"category": "urgent", "ignored_key": "some_value"})
        await repo.retrieve_mapped_kems(internal_query=query, page_size=5, page_token=None)
        await repo.disconnect()

        self.assertTrue(mock_conn.fetch.called)
        call_args = mock_conn.fetch.call_args[0]
        query_string = call_args[0]

        self.assertIn('WHERE "pg_category_col" = $1', query_string)
        self.assertNotIn('ignored_key', query_string) # Ensure the non-filterable key is not in the SQL
        self.assertIn('LIMIT $2', query_string)


    @async_test
    @patch('asyncpg.create_pool')
    async def test_retrieve_filter_with_pagination(self, mock_create_pool):
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock(spec=asyncpg.Connection)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # Mock data for two pages - ensure microseconds for robust ISO formatting
        # These should be UTC aware as asyncpg would return for TIMESTAMPTZ
        ts1_aware = datetime(2023, 1, 1, 10, 0, 0, microsecond=123000, tzinfo=timezone.utc)
        ts2_aware = datetime(2023, 1, 1, 9, 0, 0, microsecond=456000, tzinfo=timezone.utc)
        mock_pg_row1 = MagicMock()
        mock_pg_row1.get.side_effect = lambda k,d=None: {"doc_id":"id1", "updated_ts":ts1_aware, self.base_mapping_config.get("content_column"):"c1"}.get(k,d)
        mock_pg_row2 = MagicMock()
        mock_pg_row2.get.side_effect = lambda k,d=None: {"doc_id":"id2", "updated_ts":ts2_aware, self.base_mapping_config.get("content_column"):"c2"}.get(k,d)

        # First call to fetch (page 1)
        # page_size=1, so limit for query is 1+1=2. We return 2 rows, so there is a next page.
        mock_conn.fetch.return_value = [mock_pg_row1, mock_pg_row2]

        mapping_config_filterable = {
            **self.base_mapping_config,
            "filterable_metadata_columns": {"status": "pg_status_col"}
        }
        config = ExternalDataSourceConfig(
            name="pg_filter_page_test", type="postgresql",
            connection_details={"host": "dummy"}, mapping_config=mapping_config_filterable
        )
        repo = PostgresExternalRepository(config, self.glm_config)
        await repo.connect()

        query = glm_service_pb2.KEMQuery(metadata_filters={"status": "active"})

        # Page 1
        kems_p1, next_token_p1 = await repo.retrieve_mapped_kems(internal_query=query, page_size=1, page_token=None)

        self.assertEqual(len(kems_p1), 1)
        self.assertEqual(kems_p1[0].id, "id1")
        self.assertIsNotNone(next_token_p1)
        # Expected token format: YYYY-MM-DDTHH:MM:SS.ffffff+00:00|id
        expected_token_p1 = f"{ts1_aware.isoformat(timespec='microseconds')}|id1"
        self.assertEqual(next_token_p1, expected_token_p1)

        call_args_p1 = mock_conn.fetch.call_args[0]
        query_string_p1 = call_args_p1[0]
        query_params_p1 = call_args_p1[1:]
        self.assertIn('WHERE "pg_status_col" = $1', query_string_p1) # Filter
        self.assertIn('LIMIT $2', query_string_p1)                   # Limit
        self.assertEqual(query_params_p1[0], "active")              # Filter value
        self.assertEqual(query_params_p1[1], 2)                     # page_size(1) + 1

        # Setup for Page 2 call
        mock_conn.fetch.return_value = [mock_pg_row2] # Only one item left for page 2 (limit 1+1=2, returns 1)
        kems_p2, next_token_p2 = await repo.retrieve_mapped_kems(internal_query=query, page_size=1, page_token=next_token_p1)

        self.assertEqual(len(kems_p2), 1)
        self.assertEqual(kems_p2[0].id, "id2")
        self.assertIsNone(next_token_p2) # Last page because fetch returned fewer than page_size+1

        call_args_p2 = mock_conn.fetch.call_args[0]
        query_string_p2 = call_args_p2[0]
        query_params_p2 = call_args_p2[1:]

        # Check for filter AND keyset conditions
        self.assertIn('WHERE "pg_status_col" = $1 AND (("updated_ts" < $2) OR ("updated_ts" = $3 AND "doc_id" < $4))', query_string_p2)
        self.assertIn('LIMIT $5', query_string_p2)
        self.assertEqual(query_params_p2[0], "active") # status filter
        self.assertEqual(query_params_p2[1], ts1_aware) # keyset ts (datetime object)
        self.assertEqual(query_params_p2[2], ts1_aware) # keyset ts for = (datetime object)
        self.assertEqual(query_params_p2[3], "id1")     # keyset id
        self.assertEqual(query_params_p2[4], 2)         # limit (page_size + 1)

        await repo.disconnect()


if __name__ == '__main__':
    unittest.main()

```
