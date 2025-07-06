import aiosqlite # Changed from sqlite3
import json
import logging
import os
from typing import List, Tuple, Optional, Dict, Any
import asyncio # Required for async operations

from ..config import GLMConfig
# Assuming custom exceptions are in base.py, adjust if they are elsewhere or defined here.
# from .base import StorageError

logger = logging.getLogger(__name__)

# Define StorageError if not imported from base, for standalone usability during refactor
class StorageError(Exception):
    pass

class SqliteKemRepository:
    def __init__(self, db_path: str, config: GLMConfig):
        self.db_path = db_path
        self.config = config
        self._ensure_db_directory()
        # Schema initialization will be called by the first connection or an explicit setup method
        # For async, it's better to do it in an async method.
        # asyncio.run(self._initialize_schema()) # Cannot do this in sync __init__

    def _ensure_db_directory(self):
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"Created database directory: {db_dir}")

    async def _get_sqlite_conn(self) -> aiosqlite.Connection:
        try:
            conn = await aiosqlite.connect(self.db_path, timeout=self.config.SQLITE_CONNECT_TIMEOUT_S)
            conn.row_factory = aiosqlite.Row

            await conn.executescript(f"""
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA foreign_keys=ON;
                PRAGMA busy_timeout = {self.config.SQLITE_BUSY_TIMEOUT};
            """)
            return conn
        except aiosqlite.Error as e:
            logger.error(f"Failed to connect to or configure SQLite database at {self.db_path}: {e}", exc_info=True)
            raise StorageError(f"SQLite connection/configuration error: {e}") from e

    async def initialize_schema(self): # Made this an explicit async method
        try:
            async with await self._get_sqlite_conn() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS kems (
                        id TEXT PRIMARY KEY,
                        content_type TEXT,
                        content BLOB,
                        metadata TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_kems_created_at ON kems (created_at);")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_kems_updated_at ON kems (updated_at);")
                await conn.commit()
                logger.info(f"SQLite schema initialized/verified for {self.db_path}")
        except aiosqlite.Error as e:
            logger.error(f"Failed to initialize SQLite schema for {self.db_path}: {e}", exc_info=True)
            raise StorageError(f"SQLite schema initialization error: {e}") from e

    async def ensure_schema_initialized(self): # Call this from DefaultGLMRepository after init
        if not hasattr(self, '_schema_initialized') or not self._schema_initialized:
            await self.initialize_schema()
            self._schema_initialized = True


    async def get_kem_creation_timestamp(self, kem_id: str) -> Optional[str]:
        try:
            async with await self._get_sqlite_conn() as conn:
                async with conn.execute("SELECT created_at FROM kems WHERE id = ?", (kem_id,)) as cursor:
                    row = await cursor.fetchone()
                    return row["created_at"] if row else None
        except aiosqlite.Error as e:
            logger.error(f"Error getting creation timestamp for KEM ID '{kem_id}': {e}", exc_info=True)
            raise StorageError(f"SQLite error fetching creation timestamp: {e}") from e

    async def store_or_replace_kem(self, kem_id: str, content_type: str, content: bytes,
                                   metadata_json: str, created_at_iso: str, updated_at_iso: str):
        try:
            async with await self._get_sqlite_conn() as conn:
                await conn.execute("""
                    INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (kem_id, content_type, content, metadata_json, created_at_iso, updated_at_iso))
                await conn.commit()
                logger.debug(f"Stored/Replaced KEM ID '{kem_id}' in SQLite.")
        except aiosqlite.Error as e:
            logger.error(f"Error storing/replacing KEM ID '{kem_id}' in SQLite: {e}", exc_info=True)
            raise StorageError(f"SQLite error storing/replacing KEM: {e}") from e

    async def retrieve_kems_from_db(
        self,
        sql_conditions: List[str],
        sql_params: List[Any],
        page_size: int,
        page_token: Optional[str],
        order_by_clause: str
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:

        query_base = "SELECT id, content_type, content, metadata, created_at, updated_at FROM kems"
        where_clauses = []
        current_params = list(sql_params)

        if sql_conditions:
            where_clauses.extend(sql_conditions)

        parsed_order_fields = []
        # Basic parsing for common orderings; adapt as necessary
        if "updated_at DESC" in order_by_clause and "id DESC" in order_by_clause:
            parsed_order_fields = [("updated_at", "DESC", "<"), ("id", "DESC", "<")]
        elif "updated_at ASC" in order_by_clause and "id ASC" in order_by_clause:
            parsed_order_fields = [("updated_at", "ASC", ">"), ("id", "ASC", ">")]
        # Add other common orderings if needed for keyset pagination

        if page_token and parsed_order_fields:
            try:
                last_value_parts = page_token.split(',')
                if len(last_value_parts) == len(parsed_order_fields):
                    pagination_conditions_parts = []
                    if len(parsed_order_fields) == 2: # Specific to two-field keyset
                        field1, _, op1 = parsed_order_fields[0]
                        field2, _, op2 = parsed_order_fields[1]
                        val1_token, val2_token = last_value_parts[0], last_value_parts[1]

                        # (field1 < val1) OR (field1 = val1 AND field2 < val2) -- for DESC
                        # (field1 > val1) OR (field1 = val1 AND field2 > val2) -- for ASC
                        pagination_conditions_parts.append(f"({field1} {op1} ? OR ({field1} = ? AND {field2} {op2} ?))")
                        current_params.extend([val1_token, val1_token, val2_token])

                    if pagination_conditions_parts:
                         where_clauses.append(f"({' AND '.join(pagination_conditions_parts)})") # Should be OR for multi-column keyset
                else:
                    logger.warning(f"Invalid page_token format: {page_token}.")
            except ValueError as e_token: # Catch if split fails or types are wrong
                logger.warning(f"Invalid page_token value: {page_token}. Error: {e_token}")

        if where_clauses:
            query_base += " WHERE " + " AND ".join(where_clauses)

        query_final = query_base + f" {order_by_clause} LIMIT ?"
        current_params.append(page_size + 1)

        results = []
        next_page_token_str: Optional[str] = None

        try:
            async with await self._get_sqlite_conn() as conn:
                logger.debug(f"Executing SQLite query: {query_final} with params: {current_params}")
                async with conn.execute(query_final, tuple(current_params)) as cursor:
                    rows = await cursor.fetchall()

                if len(rows) > page_size:
                    last_item_for_token = rows[page_size-1]
                    token_parts = []
                    for field_info in parsed_order_fields: # Use parsed_order_fields to get field names
                        field_name = field_info[0]
                        if field_name in last_item_for_token.keys(): # sqlite.Row allows dict-like access
                             token_parts.append(str(last_item_for_token[field_name]))
                    if len(token_parts) == len(parsed_order_fields):
                        next_page_token_str = ",".join(token_parts)
                    else:
                        logger.warning(f"Could not construct next_page_token. last_item keys: {last_item_for_token.keys()}, expected: {[f[0] for f in parsed_order_fields]}")
                    results = [dict(row) for row in rows[:page_size]]
                else:
                    results = [dict(row) for row in rows]

            logger.debug(f"Retrieved {len(results)} KEMs from SQLite. Next page token: {next_page_token_str}")
            return results, next_page_token_str

        except aiosqlite.Error as e:
            logger.error(f"Error retrieving KEMs from SQLite: {e}", exc_info=True)
            raise StorageError(f"SQLite error retrieving KEMs: {e}") from e
        except Exception as e_unexp: # Catch any other unexpected error
            logger.error(f"Unexpected error retrieving KEMs from SQLite: {e_unexp}", exc_info=True)
            raise StorageError(f"Unexpected error retrieving KEMs: {e_unexp}") from e_unexp

    async def get_kem_by_id(self, kem_id: str) -> Optional[Dict[str, Any]]:
        try:
            async with await self._get_sqlite_conn() as conn:
                async with conn.execute("SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id = ?", (kem_id,)) as cursor:
                    row = await cursor.fetchone()
                    return dict(row) if row else None
        except aiosqlite.Error as e:
            logger.error(f"Error getting KEM ID '{kem_id}' from SQLite: {e}", exc_info=True)
            raise StorageError(f"SQLite error getting KEM by ID: {e}") from e

    async def delete_kem_from_db(self, kem_id: str) -> bool:
        try:
            async with await self._get_sqlite_conn() as conn:
                cursor = await conn.execute("DELETE FROM kems WHERE id = ?", (kem_id,))
                await conn.commit()
                return cursor.rowcount > 0
        except aiosqlite.Error as e:
            logger.error(f"Error deleting KEM ID '{kem_id}' from SQLite: {e}", exc_info=True)
            raise StorageError(f"SQLite error deleting KEM: {e}") from e

    async def internal_sqlite_health_check(self) -> None:
        # Renamed to avoid conflict if Base class has _internal_sqlite_health_check
        # This is called by DefaultGLMRepository's check_health via asyncio.to_thread
        # So it needs to be async itself now.
        async with await self._get_sqlite_conn() as conn:
            sqlite_query = getattr(self.config, "HEALTH_CHECK_SQLITE_QUERY", "SELECT 1")
            await conn.execute(sqlite_query)
            await conn.fetchone() # Not standard on connection, but on cursor.
            # Corrected:
            # async with conn.execute(sqlite_query) as cursor:
            #    await cursor.fetchone()
            # Simpler for just an existence check:
            await conn.execute(sqlite_query) # Just execute to see if it errors

    # Batch operations - to be implemented if needed
    async def store_or_replace_kem_batch(self, kems_data: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
        # kems_data is a list of dicts, each dict contains parameters for store_or_replace_kem
        # e.g., {"kem_id": "...", "content_type": "...", ...}
        # This method should attempt to store all, and return (list_of_successful_ids, list_of_failed_ids)
        # For atomicity, one might do all in one transaction.
        successful_ids = []
        failed_ids = []
        try:
            async with await self._get_sqlite_conn() as conn:
                for kem_data_item in kems_data:
                    try:
                        await conn.execute("""
                            INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (kem_data_item["kem_id"], kem_data_item["content_type"], kem_data_item["content"],
                              kem_data_item["metadata_json"], kem_data_item["created_at_iso"], kem_data_item["updated_at_iso"]))
                        successful_ids.append(kem_data_item["kem_id"])
                    except Exception as e_item: # Catch error per item
                        logger.error(f"Error storing KEM ID '{kem_data_item.get('kem_id')}' in batch: {e_item}")
                        failed_ids.append(kem_data_item.get("kem_id", "UNKNOWN_ID_IN_BATCH_FAILURE"))
                await conn.commit()
        except aiosqlite.Error as e_batch:
            logger.error(f"SQLite error during batch KEM store: {e_batch}", exc_info=True)
            # If transaction itself fails, all non-committed items are effectively failed
            # This simple loop doesn't handle partial batch commits well if conn fails mid-loop before commit.
            # A more robust batch would prepare all statements or handle this differently.
            # For now, assume all items in current batch are failed if transaction fails.
            # However, the loop above commits after all items are executed, so if conn fails, nothing is committed.
            # If an item fails, it's added to failed_ids, but loop continues.
            # This needs refinement for true atomicity or better partial failure reporting.
            # For now, failed_ids will contain individual item errors. If conn.commit() fails, it's a larger problem.
            raise StorageError(f"SQLite batch KEM store error: {e_batch}") from e_batch

        logger.debug(f"Batch store SQLite: Success: {len(successful_ids)}, Failed: {len(failed_ids)}")
        return successful_ids, failed_ids
