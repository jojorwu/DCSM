import asyncio
import asyncpg # For PostgreSQL connection
import json
import logging
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timezone # For robust timestamp handling

from dcs_memory.services.glm.generated_grpc import kem_pb2, glm_service_pb2
from dcs_memory.services.glm.app.config import ExternalDataSourceConfig, GLMConfig
from .external_base import BaseExternalRepository
from .base import StorageError, BackendUnavailableError, InvalidQueryError # Reusing exceptions

logger = logging.getLogger(__name__)

class PostgresExternalRepository(BaseExternalRepository):
    """
    External repository implementation for connecting to a PostgreSQL database.
    """

    def __init__(self, config: ExternalDataSourceConfig, glm_config: GLMConfig):
        super().__init__(config, glm_config)
        self.pool: Optional[asyncpg.Pool] = None

        # Validate and parse mapping_config for PostgreSQL
        self.table_name = self.config.mapping_config.get("table_name")
        self.id_column = self.config.mapping_config.get("id_column")
        self.content_column = self.config.mapping_config.get("content_column") # Can be text, bytea, or even json/jsonb
        self.content_type_value = self.config.mapping_config.get("content_type_value", "text/plain") # Default content type if not mapped from a column

        # Timestamp columns for ordering and keyset pagination
        self.timestamp_sort_column = self.config.mapping_config.get("timestamp_sort_column") # e.g., 'updated_at', 'created_at'
        self.id_tiebreaker_column = self.id_column # Usually the ID column serves as a unique tie-breaker

        # Metadata mapping:
        # Option 1: A single column containing JSON/JSONB
        self.metadata_json_column = self.config.mapping_config.get("metadata_json_column")
        # Option 2: A list of columns to be individually mapped into KEM metadata
        self.metadata_columns_map = self.config.mapping_config.get("metadata_columns_map", {}) # e.g., {"kem_meta_key": "pg_column_name"}

        # Created_at and Updated_at column mapping (optional, if not using timestamp_sort_column for both)
        self.created_at_column = self.config.mapping_config.get("created_at_column", self.timestamp_sort_column)
        self.updated_at_column = self.config.mapping_config.get("updated_at_column", self.timestamp_sort_column)

        # Default timezone assumption for naive timestamps from DB (if PG column is TIMESTAMP WITHOUT TIME ZONE)
        # For TIMESTAMPTZ, asyncpg should return timezone-aware datetimes.
        self.assume_naive_timestamp_is_utc = self.config.mapping_config.get("assume_naive_timestamp_is_utc", True)


        # Mapping for filterable metadata columns: KEM metadata key -> PG column name
        self.filterable_metadata_columns: Dict[str, str] = self.config.mapping_config.get("filterable_metadata_columns", {})
        if not isinstance(self.filterable_metadata_columns, dict):
            logger.warning(f"PostgreSQL connector '{self.config.name}': 'filterable_metadata_columns' in mapping_config is not a dictionary. Will be ignored.")
            self.filterable_metadata_columns = {}


        if not all([self.table_name, self.id_column, self.content_column, self.timestamp_sort_column]):
            raise ValueError(
                f"PostgreSQL connector '{self.config.name}': Missing required mapping_config fields: "
                f"'table_name', 'id_column', 'content_column', 'timestamp_sort_column'."
            )

        if not self.metadata_json_column and not self.metadata_columns_map:
            logger.warning(f"PostgreSQL connector '{self.config.name}': No metadata mapping configured ('metadata_json_column' or 'metadata_columns_map'). KEM metadata will be empty.")

        logger.info(f"PostgresExternalRepository '{self.config.name}' initialized. Table: {self.table_name}, ID: {self.id_column}, Content: {self.content_column}, TimestampSort: {self.timestamp_sort_column}")


    async def connect(self) -> None:
        if self.pool:
            logger.info(f"PostgreSQL connector '{self.config.name}': Already connected.")
            return

        try:
            logger.info(f"PostgreSQL connector '{self.config.name}': Attempting to connect...")
            if self.config.connection_uri:
                self.pool = await asyncpg.create_pool(dsn=self.config.connection_uri, min_size=1, max_size=5) # Example pool size
            elif self.config.connection_details:
                self.pool = await asyncpg.create_pool(**self.config.connection_details, min_size=1, max_size=5)
            else:
                raise ValueError("PostgreSQL connector: No connection_uri or connection_details provided.")

            if self.pool:
                 # Try a simple query to confirm connection
                async with self.pool.acquire() as connection:
                    await connection.fetchval("SELECT 1")
                logger.info(f"PostgreSQL connector '{self.config.name}': Connection pool established and test query successful.")
            else:
                raise BackendUnavailableError(f"PostgreSQL connector '{self.config.name}': Failed to create connection pool (pool is None).")

        except (asyncpg.exceptions.PostgresConnectionError, OSError, ValueError) as e: # Catch asyncpg specific and general connection errors
            logger.error(f"PostgreSQL connector '{self.config.name}': Connection failed: {e}", exc_info=True)
            self.pool = None # Ensure pool is None if connection fails
            raise BackendUnavailableError(f"PostgreSQL connector '{self.config.name}': Connection failed: {e}") from e
        except Exception as e_unhandled: # Catch any other unexpected errors during connect
            logger.error(f"PostgreSQL connector '{self.config.name}': Unhandled exception during connect: {e_unhandled}", exc_info=True)
            self.pool = None
            raise BackendUnavailableError(f"PostgreSQL connector '{self.config.name}': Unhandled exception during connect: {e_unhandled}") from e_unhandled


    async def disconnect(self) -> None:
        if self.pool:
            logger.info(f"PostgreSQL connector '{self.config.name}': Closing connection pool.")
            await self.pool.close()
            self.pool = None
            logger.info(f"PostgreSQL connector '{self.config.name}': Connection pool closed.")
        else:
            logger.info(f"PostgreSQL connector '{self.config.name}': Already disconnected or never connected.")

    async def check_health(self) -> Tuple[bool, str]:
        if not self.pool:
            return False, f"PostgreSQL connector '{self.config.name}': Not connected (no pool)."
        try:
            async with self.pool.acquire() as connection:
                result = await connection.fetchval("SELECT 1")
                if result == 1:
                    return True, f"PostgreSQL connector '{self.config.name}': Healthy."
                else:
                    return False, f"PostgreSQL connector '{self.config.name}': Health check query (SELECT 1) failed to return 1."
        except Exception as e:
            logger.error(f"PostgreSQL connector '{self.config.name}': Health check failed: {e}", exc_info=True)
            return False, f"PostgreSQL connector '{self.config.name}': Health check failed: {e}"

    async def retrieve_mapped_kems(
        self,
        internal_query: glm_service_pb2.KEMQuery, # Added internal_query
        page_size: int,
        page_token: Optional[str]
    ) -> Tuple[List[kem_pb2.KEM], Optional[str]]:
        # Phase 3a: Will be updated to use internal_query.metadata_filters
        # For now, largely ignores complex KEMQuery filters, focuses on basic table scan with pagination and mapping.
        if not self.pool:
            raise BackendUnavailableError(f"PostgreSQL connector '{self.config.name}': Not connected.")

        kems_list: List[kem_pb2.KEM] = []
        next_page_token_val: Optional[str] = None

        # Build list of columns to select
        select_columns = {self.id_column, self.content_column, self.timestamp_sort_column}
        if self.id_tiebreaker_column: select_columns.add(self.id_tiebreaker_column)
        if self.created_at_column: select_columns.add(self.created_at_column)
        if self.updated_at_column: select_columns.add(self.updated_at_column)
        if self.metadata_json_column: select_columns.add(self.metadata_json_column)
        for pg_col in self.metadata_columns_map.values(): select_columns.add(pg_col)

        # Ensure columns used for sorting are always selected for keyset pagination
        select_columns_str = ", ".join(f'"{col}"' for col in sorted(list(select_columns))) # Quote column names

        # Keyset pagination logic
        # For simplicity, assume timestamp_sort_column is a_timestamp and id_tiebreaker_column is a_uuid or string
        # Order: timestamp_sort_column DESC, id_tiebreaker_column DESC

        where_clauses: List[str] = []
        query_params: List[Any] = [] # Parameters for asyncpg, $1, $2, etc.
        param_idx = 1 # For asyncpg parameter numbering

        def _to_utc_datetime(dt: Optional[datetime]) -> Optional[datetime]:
            if dt is None:
                return None
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None: # Naive datetime
                if self.assume_naive_timestamp_is_utc:
                    return dt.replace(tzinfo=timezone.utc)
                else: # Try to localize to system timezone then convert to UTC (can be problematic)
                    logger.warning(f"Naive datetime '{dt}' found; localizing to system time then UTC. Explicit TIMESTAMPTZ in PG is safer.")
                    return dt.astimezone(timezone.utc)
            return dt.astimezone(timezone.utc) # Ensure it's UTC

        def _datetime_to_iso_utc_string(dt: Optional[datetime]) -> Optional[str]:
            if dt is None:
                return None
            utc_dt = _to_utc_datetime(dt)
            if utc_dt:
                return utc_dt.isoformat(timespec='microseconds') # Includes timezone offset like +00:00
            return None

        def _iso_utc_string_to_datetime(iso_str: str) -> Optional[datetime]:
            try:
                dt = datetime.fromisoformat(iso_str)
                return _to_utc_datetime(dt) # Ensure it's UTC for comparison
            except (ValueError, TypeError):
                logger.warning(f"Could not parse ISO string '{iso_str}' to datetime.")
                return None

        # 1. Add metadata filter conditions from KEMQuery
        if internal_query.metadata_filters and self.filterable_metadata_columns:
            for filter_key, filter_value in internal_query.metadata_filters.items():
                if filter_key in self.filterable_metadata_columns:
                    pg_column_name = self.filterable_metadata_columns[filter_key]
                    # For timestamp columns, we might need to parse filter_value if it's an ISO string
                    # and the PG column is a timestamp type. For now, assume direct equality for non-ts.
                    # This logic might need to be more type-aware based on pg_column_name's type.
                    where_clauses.append(f'"{pg_column_name}" = ${param_idx}')
                    query_params.append(filter_value) # asyncpg handles basic type conversion
                    param_idx += 1
                    logger.debug(f"PostgreSQL connector '{self.config.name}': Applying metadata filter: \"{pg_column_name}\" = '{filter_value}'")
                else:
                    logger.warning(f"PostgreSQL connector '{self.config.name}': Metadata filter key '{filter_key}' is not configured as filterable. Ignoring.")

        # 2. Add keyset pagination conditions (if page_token is present)
        if page_token:
            try:
                last_ts_iso_str, last_id_val_str = page_token.split('|', 1)
                last_ts_dt = _iso_utc_string_to_datetime(last_ts_iso_str)

                if last_ts_dt:
                    # For (timestamp_sort_column DESC, id_tiebreaker_column DESC):
                    # ( (timestamp_sort_column < $ts) OR
                    #   (timestamp_sort_column = $ts AND id_tiebreaker_column < $id) )
                    keyset_condition = (
                        f'(("{self.timestamp_sort_column}" < ${param_idx}) OR '
                        f' ("{self.timestamp_sort_column}" = ${param_idx+1} AND "{self.id_tiebreaker_column}" < ${param_idx+2}))'
                    )
                    where_clauses.append(keyset_condition)
                    # asyncpg expects datetime objects for timestamp columns, not strings.
                    query_params.extend([last_ts_dt, last_ts_dt, last_id_val_str]) # last_id_val_str might need casting depending on PG col type
                    param_idx += 3
                    logger.debug(f"PostgreSQL connector '{self.config.name}': Applying keyset pagination from token '{page_token}' (parsed ts: {last_ts_dt})")
                else:
                    logger.warning(f"PostgreSQL connector '{self.config.name}': Could not parse timestamp from page_token '{page_token}'. Ignoring for pagination.")
            except ValueError: # Handles split error
                logger.warning(f"PostgreSQL connector '{self.config.name}': Invalid page_token format '{page_token}'. Ignoring for pagination (fetching first page of filtered results).", exc_info=True)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Fetch one extra row to determine if there's a next page
        limit_for_query = page_size + 1

        sql_query = (
            f'SELECT {select_columns_str} FROM "{self.table_name}" '
            f'{where_sql} '
            f'ORDER BY "{self.timestamp_sort_column}" DESC, "{self.id_tiebreaker_column}" DESC '
            f'LIMIT ${param_idx}' # Limit parameter
        )
        query_params.append(limit_for_query)
        param_idx += 1

        logger.debug(f"PostgreSQL connector '{self.config.name}': Executing query: {sql_query} with params: {query_params}")

        try:
            async with self.pool.acquire() as connection:
                pg_rows = await connection.fetch(sql_query, *query_params)

            for row_idx, pg_row in enumerate(pg_rows):
                if row_idx == page_size: # This is the extra item indicating a next page
                    last_item_for_current_page = dict(pg_rows[page_size - 1]) # The actual last item of current page

                    next_ts_dt_obj = last_item_for_current_page.get(self.timestamp_sort_column) # Should be a datetime object from asyncpg
                    next_id_val = last_item_for_current_page.get(self.id_tiebreaker_column)

                    if next_ts_dt_obj is not None and next_id_val is not None:
                        # Convert timestamp to standardized ISO UTC string for the token
                        next_ts_iso_str = _datetime_to_iso_utc_string(next_ts_dt_obj)
                        if next_ts_iso_str:
                            next_page_token_val = f"{next_ts_iso_str}|{str(next_id_val)}"
                        else:
                            logger.error(f"PostgreSQL connector '{self.config.name}': Failed to format next_page_token timestamp for KEM ID {next_id_val}.")
                    else:
                        logger.error(f"PostgreSQL connector '{self.config.name}': Failed to generate next_page_token due to missing sort column values in last item: ts_col='{self.timestamp_sort_column}', id_col='{self.id_tiebreaker_column}'.")

                    # Truncate the list to the requested page_size
                    # This was missing, should be done after this block, before returning.
                    # Corrected: This break is correct, found_kems_list will be built up to page_size.
                    break

                kem = kem_pb2.KEM()
                kem.id = str(pg_row.get(self.id_column, "")) # Ensure ID is string

                # Content
                content_val = pg_row.get(self.content_column)
                if isinstance(content_val, bytes):
                    kem.content = content_val
                elif isinstance(content_val, str):
                    kem.content = content_val.encode('utf-8')
                elif content_val is not None: # Other types like JSON
                    kem.content = json.dumps(content_val).encode('utf-8')

                kem.content_type = self.content_type_value # Could also be mapped from a column

                # Timestamps - ensure they are UTC and then convert to protobuf Timestamp
                created_at_db_val = pg_row.get(self.created_at_column) # Expected to be datetime from asyncpg
                created_at_utc = _to_utc_datetime(created_at_db_val)
                if created_at_utc:
                    kem.created_at.FromSeconds(int(created_at_utc.timestamp()))
                    kem.created_at.nanos = created_at_utc.microsecond * 1000

                updated_at_db_val = pg_row.get(self.updated_at_column) # Expected to be datetime from asyncpg
                updated_at_utc = _to_utc_datetime(updated_at_db_val)
                if updated_at_utc:
                    kem.updated_at.FromSeconds(int(updated_at_utc.timestamp()))
                    kem.updated_at.nanos = updated_at_utc.microsecond * 1000
                elif self.updated_at_column == self.timestamp_sort_column and created_at_utc:
                    # If updated_at is same as sort col, and sort col was primary for KEM updated_at
                    # but was missing for some reason, use created_at if available, or log error.
                    # This case should be rare if sort column is reliable.
                    logger.warning(f"KEM ID {kem.id}: updated_at_column ('{self.updated_at_column}') value missing or unparseable, but it's also the sort column. This is problematic.")


                # Metadata
                if self.metadata_json_column:
                    meta_json_str_or_dict = pg_row.get(self.metadata_json_column)
                    if isinstance(meta_json_str_or_dict, dict): # If PG returns parsed JSON (jsonb)
                        for k, v in meta_json_str_or_dict.items(): kem.metadata[k] = str(v)
                    elif isinstance(meta_json_str_or_dict, str): # If it's a JSON string
                        try:
                            meta_dict = json.loads(meta_json_str_or_dict)
                            for k, v in meta_dict.items(): kem.metadata[k] = str(v)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse metadata_json_column '{self.metadata_json_column}' for KEM ID {kem.id}")
                elif self.metadata_columns_map:
                    for kem_key, pg_col_name in self.metadata_columns_map.items():
                        val = pg_row.get(pg_col_name)
                        if val is not None:
                            kem.metadata[kem_key] = str(val)

                kems_list.append(kem)

        except asyncpg.exceptions.PostgresError as e:
            logger.error(f"PostgreSQL connector '{self.config.name}': Database error during retrieve: {e}", exc_info=True)
            raise StorageError(f"PostgreSQL query failed for source '{self.config.name}': {e}") from e
        except Exception as e_unhandled:
            logger.error(f"PostgreSQL connector '{self.config.name}': Unhandled exception during retrieve: {e_unhandled}", exc_info=True)
            raise StorageError(f"Unhandled exception in PostgreSQL connector '{self.config.name}': {e_unhandled}") from e_unhandled

        return kems_list, next_page_token_val

```
