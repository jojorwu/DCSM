# Placeholder for GLM Repositories

import sqlite3
import logging
import threading # Added for thread-local storage
from typing import List, Dict, Optional, Any, Literal # Added Literal
from qdrant_client import QdrantClient, models
from google.protobuf.timestamp_pb2 import Timestamp

from .config import GLMConfig # Assuming GLMConfig is in the same app directory
# Assuming kem_pb2 might be needed for type hints if methods return/take parts of it directly,
# but ideally repositories work with more primitive types or app-specific domain models.
# from generated_grpc import kem_pb2 # Or a more generic KEM dataclass/dict type hint

logger = logging.getLogger(__name__)

class SqliteKemRepository:
    def __init__(self, db_path: str, config: GLMConfig):
        self.db_path = db_path
        self.config = config
        self._local = threading.local() # Initialize thread-local storage
        self._init_sqlite_schema() # Renamed to avoid confusion, schema init is once

    def _get_sqlite_conn(self) -> sqlite3.Connection:
        # Get or create a connection for the current thread
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            logger.info(f"Thread {threading.get_ident()}: No existing SQLite connection. Creating new one to {self.db_path}.")
            try:
                # Each thread gets its own connection object.
                # Default check_same_thread=True is appropriate and safe here.
                conn = sqlite3.connect(self.db_path, timeout=self.config.SQLITE_CONNECT_TIMEOUT_S) # Use configured timeout

                # Apply PRAGMA settings once per connection
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL;")
                logger.info(f"Thread {threading.get_ident()}: PRAGMA journal_mode=WAL set.")
                cursor.execute("PRAGMA synchronous=NORMAL;")
                logger.info(f"Thread {threading.get_ident()}: PRAGMA synchronous=NORMAL set.")
                cursor.execute("PRAGMA foreign_keys=ON;")
                logger.info(f"Thread {threading.get_ident()}: PRAGMA foreign_keys=ON set.")
                # busy_timeout from config or a sensible default
                busy_timeout = getattr(self.config, 'SQLITE_BUSY_TIMEOUT', 7500)
                cursor.execute(f"PRAGMA busy_timeout = {busy_timeout};")
                logger.info(f"Thread {threading.get_ident()}: PRAGMA busy_timeout = {busy_timeout} set.")

                self._local.conn = conn
            except sqlite3.Error as e:
                logger.critical(f"Thread {threading.get_ident()}: CRITICAL Error creating SQLite connection or setting PRAGMAs: {e}", exc_info=True)
                # If essential PRAGMAs fail, it's safer to not proceed with a potentially misconfigured connection.
                raise RuntimeError(f"Failed to initialize SQLite connection properly: {e}") from e
        return self._local.conn

    def _init_sqlite_schema(self):
        # This method ensures the schema exists. It uses a temporary connection
        # as it's called only once during repository initialization.
        # The regular _get_sqlite_conn() will be used by operational methods.
        logger.info(f"Repository: Initializing SQLite DB schema at path: {self.db_path}")
        try:
            # Use a temporary connection for schema initialization
            # This is separate from the thread-local connections used for operations.
            with sqlite3.connect(self.db_path, timeout=self.config.SQLITE_CONNECT_TIMEOUT_S) as conn: # Use configured timeout
                cursor = conn.cursor()
                # Set PRAGMAs for this schema initialization connection as well,
                # especially WAL mode, as it can affect how the DB file is structured initially.
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("PRAGMA synchronous=NORMAL;")
                cursor.execute("PRAGMA foreign_keys=ON;")

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS kems (
                    id TEXT PRIMARY KEY,
                    content_type TEXT,
                    content BLOB,
                    metadata TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
                ''')
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_created_at ON kems (created_at);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_updated_at ON kems (updated_at);")
                logger.info("Repository: Attempting to create JSON indexes (requires SQLite >= 3.38.0)...")
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_meta_type ON kems(json_extract(metadata, '$.type'));")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_meta_source_system ON kems(json_extract(metadata, '$.source_system'));")
                    logger.info("Repository: JSON indexes (meta_type, meta_source_system) creation attempted.")
                except sqlite3.Error as e_json_idx:
                    logger.warning(f"Repository: Could not create JSON indexes (SQLite < 3.38.0 or other error): {e_json_idx}.", exc_info=True)
                conn.commit()
            logger.info("Repository: 'kems' table and indexes in SQLite successfully initialized via Repository.")
        except Exception as e:
            logger.error(f"Repository: Error initializing SQLite via Repository: {e}", exc_info=True)
            raise # Re-raise to signal failure to the caller (servicer)

    def get_kem_creation_timestamp(self, kem_id: str) -> Optional[str]:
        """Retrieves only the created_at timestamp string for a given KEM ID."""
        with self._get_sqlite_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT created_at FROM kems WHERE id = ?", (kem_id,))
            row = cursor.fetchone()
            return row[0] if row else None

    def get_kems_creation_timestamps(self, kem_ids: List[str]) -> Dict[str, str]:
        """Retrieves created_at timestamps for a list of KEM IDs."""
        if not kem_ids:
            return {}
        placeholders = ','.join('?' for _ in kem_ids)
        query = f"SELECT id, created_at FROM kems WHERE id IN ({placeholders})"
        with self._get_sqlite_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(kem_ids))
            return {row[0]: row[1] for row in cursor.fetchall()}

    def store_or_replace_kem(self, kem_id: str, content_type: str, content: bytes, metadata_json: str, created_at_iso: str, updated_at_iso: str):
        """Stores or replaces a KEM record."""
        with self._get_sqlite_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (kem_id, content_type, content, metadata_json, created_at_iso, updated_at_iso))
            conn.commit()
            logger.debug(f"Repository: KEM ID '{kem_id}' stored/replaced in SQLite.")

    def get_full_kem_by_id(self, kem_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a full KEM record by its ID."""
        with self._get_sqlite_conn() as conn:
            conn.row_factory = sqlite3.Row # Access columns by name
            cursor = conn.cursor()
            cursor.execute("SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id = ?", (kem_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def update_kem_fields(self, kem_id: str, updated_at_iso: str, content_type: Optional[str] = None, content: Optional[bytes] = None, metadata_json: Optional[str] = None) -> bool:
        """Updates specific fields of a KEM. Returns True if a row was updated."""
        fields_to_update = []
        params = []

        if content_type is not None:
            fields_to_update.append("content_type = ?")
            params.append(content_type)
        if content is not None: # content can be empty bytes, so check for None explicitly
            fields_to_update.append("content = ?")
            params.append(content)
        if metadata_json is not None:
            fields_to_update.append("metadata = ?")
            params.append(metadata_json)

        # updated_at is always updated
        fields_to_update.append("updated_at = ?")
        params.append(updated_at_iso)

        # kem_id for the WHERE clause
        params.append(kem_id)

        set_clause = ", ".join(fields_to_update)
        query = f"UPDATE kems SET {set_clause} WHERE id = ?"

        with self._get_sqlite_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            conn.commit()
            logger.debug(f"Repository: Attempted update for KEM ID '{kem_id}'. Rows affected: {cursor.rowcount}")
            return cursor.rowcount > 0


    def delete_kem_by_id(self, kem_id: str) -> bool:
        """Deletes a KEM by its ID. Returns True if a row was deleted."""
        with self._get_sqlite_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM kems WHERE id = ?", (kem_id,))
            conn.commit()
            logger.debug(f"Repository: Attempted delete for KEM ID '{kem_id}'. Rows affected: {cursor.rowcount}")
            return cursor.rowcount > 0

    def retrieve_kems_from_db(self,
                              sql_conditions: List[str],
                              sql_params: List[Any],
                              page_size: int,
                              offset: int,
                              order_by_clause: str = "ORDER BY updated_at DESC" # Default order
                             ) -> Tuple[List[Dict[str, Any]], str]: # Returns (kems_list, next_page_token)
        """
        Retrieves KEMs based on a constructed SQL query, supporting filters, ordering, and pagination.
        """
        sql_where_clause = " WHERE " + " AND ".join(sql_conditions) if sql_conditions else ""

        base_query = f"SELECT id, content_type, content, metadata, created_at, updated_at FROM kems{sql_where_clause}"

        # For pagination check: count how many items would exist for the *next* page
        # This is more efficient than counting all matching items if only existence is needed.
        query_for_next_page_check = f"SELECT EXISTS({base_query} {order_by_clause} LIMIT 1 OFFSET ?)"

        # For current page results
        query_paginated = f"{base_query} {order_by_clause} LIMIT ? OFFSET ?"

        found_kems_list = []
        next_page_token_str = ""

        with self._get_sqlite_conn() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Fetch current page
            current_page_params = sql_params + [page_size, offset]
            logger.debug(f"Executing paginated query: {query_paginated} with params {current_page_params}")
            cursor.execute(query_paginated, current_page_params)
            rows = cursor.fetchall()
            found_kems_list = [dict(row) for row in rows]

            # Check if there's a next page
            if len(rows) == page_size:
                next_page_offset_for_check = offset + page_size
                logger.debug(f"Checking for next page with offset {next_page_offset_for_check}")
                cursor_count = conn.cursor() # Use a new cursor for this separate query
                cursor_count.execute(query_for_next_page_check, sql_params + [next_page_offset_for_check])
                if cursor_count.fetchone()[0] == 1: # EXISTS returns 1 if subquery has rows
                    next_page_token_str = str(next_page_offset_for_check)

        return found_kems_list, next_page_token_str

    def batch_store_or_replace_kems(self, kems_data: List[Dict[str, Any]]):
        """
        Stores or replaces a batch of KEM records in a single transaction.
        Each dict in kems_data should contain: 'id', 'content_type', 'content',
        'metadata_json', 'created_at_iso', 'updated_at_iso'.
        """
        if not kems_data:
            return

        sql = '''
            INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
            VALUES (:id, :content_type, :content, :metadata_json, :created_at_iso, :updated_at_iso)
        '''
        # executemany expects a list of tuples/dicts matching the placeholders.
        # We need to ensure the keys in our dicts match the named placeholders.

        # For simplicity and direct control, iterate and execute, but all within one transaction.
        # executemany with INSERT OR REPLACE is fine.

        params_list = []
        for kem_d in kems_data:
            params_list.append({
                "id": kem_d['id'],
                "content_type": kem_d['content_type'],
                "content": kem_d['content'],
                "metadata_json": kem_d['metadata_json'],
                "created_at_iso": kem_d['created_at_iso'],
                "updated_at_iso": kem_d['updated_at_iso'],
            })

        with self._get_sqlite_conn() as conn:
            cursor = conn.cursor()
            try:
                # It's generally better to execute individual statements in a loop
                # if you need fine-grained error handling per item, but for batch
                # INSERT OR REPLACE, executemany is more concise if errors are handled for the whole batch.
                # If one fails, the transaction will be rolled back by the 'with' statement if an exception propagates.
                cursor.executemany(sql, params_list)
                conn.commit()
                logger.debug(f"Repository: Batch of {len(kems_data)} KEMs stored/replaced in SQLite.")
            except sqlite3.Error as e_batch:
                logger.error(f"Repository: SQLite error during batch store/replace: {e_batch}", exc_info=True)
                # conn.rollback() # Handled by 'with' statement context exit on error
                raise # Re-raise to allow servicer to handle it


class QdrantKemRepository:
    def __init__(self, qdrant_client: QdrantClient, collection_name: str, default_vector_size: int, default_distance_metric: str): # Added default_distance_metric
        self.client = qdrant_client
        self.collection_name = collection_name
        self.default_vector_size = default_vector_size
        self.default_distance_metric_str = default_distance_metric.upper() # Store configured metric string

    def _get_qdrant_distance_metric(self) -> models.Distance:
        """Converts string distance metric from config to Qdrant's models.Distance enum."""
        if self.default_distance_metric_str == "COSINE":
            return models.Distance.COSINE
        elif self.default_distance_metric_str == "DOT":
            return models.Distance.DOT
        elif self.default_distance_metric_str == "EUCLID":
            return models.Distance.EUCLID
        else:
            logger.warning(
                f"Unsupported Qdrant distance metric '{self.default_distance_metric_str}' in config. "
                f"Defaulting to COSINE. Supported: COSINE, DOT, EUCLID."
            )
            return models.Distance.COSINE

    def ensure_collection(self):
        logger.info(f"Repository: Ensuring Qdrant collection '{self.collection_name}' exists.")
        try:
            self.client.get_collection(collection_name=self.collection_name)
            logger.info(f"Repository: Qdrant collection '{self.collection_name}' already exists.")
        except Exception as e:
            # More robust check for "not found" type errors from Qdrant client
            is_not_found_error = ("not found" in str(e).lower() or
                                  "404" in str(e).lower() or
                                  (hasattr(e, 'status_code') and e.status_code == 404) or
                                  (isinstance(e, ValueError) and "Collection" in str(e) and "not found" in str(e)) # For some client versions
                                 )
            if is_not_found_error:
                logger.info(f"Repository: Qdrant collection '{self.collection_name}' not found. Creating...")
                distance_metric = self._get_qdrant_distance_metric() # Use configured metric
                self.client.recreate_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(size=self.default_vector_size, distance=distance_metric)
                )
                logger.info(f"Repository: Qdrant collection '{self.collection_name}' created with distance metric {distance_metric}.")
            else:
                logger.error(f"Repository: Error checking/creating Qdrant collection '{self.collection_name}': {e}", exc_info=True)
                raise

    def upsert_point(self, point: models.PointStruct) -> None:
        """Upserts a single point to Qdrant."""
        if not self.client:
            logger.warning("Qdrant client not available in repository, skipping upsert.")
            return
        self.client.upsert(collection_name=self.collection_name, points=[point], wait=True)
        logger.debug(f"Repository: Point ID '{point.id}' upserted to Qdrant collection '{self.collection_name}'.")

    def upsert_points_batch(self, points: List[models.PointStruct]) -> None:
        """Upserts a batch of points to Qdrant."""
        if not self.client:
            logger.warning("Qdrant client not available in repository, skipping batch upsert.")
            return
        if not points:
            return
        self.client.upsert(collection_name=self.collection_name, points=points, wait=True) # wait=True for synchronous behavior matching old code
        logger.debug(f"Repository: Batch of {len(points)} points upserted to Qdrant collection '{self.collection_name}'.")

    def delete_points_by_ids(self, kem_ids: List[str]) -> None:
        """Deletes points from Qdrant by their IDs."""
        if not self.client:
            logger.warning("Qdrant client not available in repository, skipping delete.")
            return
        if not kem_ids:
            return
        self.client.delete_points(
            collection_name=self.collection_name,
            points_selector=models.PointIdsList(points=kem_ids),
            wait=True
        )
        logger.debug(f"Repository: Attempted delete for KEM IDs {kem_ids} in Qdrant collection '{self.collection_name}'.")

    def search_points(self, query_vector: List[float], query_filter: Optional[models.Filter], limit: int, offset: int = 0, with_vectors: bool = False) -> List[models.ScoredPoint]:
        """Searches points in Qdrant."""
        if not self.client:
            logger.warning("Qdrant client not available in repository, skipping search.")
            return []

        search_result = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=limit,
            offset=offset,
            with_payload=True, # Usually needed to get metadata or kem_id_ref
            with_vectors=with_vectors
        )
        logger.debug(f"Repository: Qdrant search returned {len(search_result)} points.")
        return search_result

    def retrieve_points_by_ids(self, kem_ids: List[str], with_vectors: bool = False) -> List[models.PointStruct]:
        """Retrieves points from Qdrant by their IDs."""
        if not self.client:
            logger.warning("Qdrant client not available in repository, skipping retrieve.")
            return []
        if not kem_ids:
            return []

        points = self.client.retrieve(
            collection_name=self.collection_name,
            ids=kem_ids,
            with_payload=True, # Usually needed
            with_vectors=with_vectors
        )
        logger.debug(f"Repository: Qdrant retrieve returned {len(points)} points for IDs {kem_ids}.")
        return points

# Example of a more structured KEM representation for internal use, if needed
# @dataclass
# class KemData:
#     id: str
#     content_type: str
#     content: bytes
#     metadata: Dict[str, Any]
#     created_at: Timestamp # Or datetime object
#     updated_at: Timestamp # Or datetime object
#     embeddings: Optional[List[float]] = None

logger.info("GLM Repositories module loaded.")
