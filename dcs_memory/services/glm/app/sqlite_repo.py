import aiosqlite
import logging
from typing import List, Dict, Optional, Any, Tuple
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import PointStruct

from dcs_memory.services.glm.app.config import GLMConfig

logger = logging.getLogger(__name__)

class SqliteKemRepository:
    def __init__(self, db_path: str, config: GLMConfig):
        self.db_path = db_path
        self.config = config
        self._db: Optional[aiosqlite.Connection] = None

    async def _get_db(self) -> aiosqlite.Connection:
        if self._db is None:
            self._db = await aiosqlite.connect(self.db_path, timeout=self.config.SQLITE_CONNECT_TIMEOUT_S)
            self._db.row_factory = aiosqlite.Row
            await self._db.execute("PRAGMA journal_mode=WAL;")
            await self._db.execute("PRAGMA synchronous=NORMAL;")
            await self._db.execute("PRAGMA foreign_keys=ON;")
            busy_timeout = getattr(self.config, 'SQLITE_BUSY_TIMEOUT', 7500)
            await self._db.execute(f"PRAGMA busy_timeout = {busy_timeout};")
            if self.config.SQLITE_CACHE_SIZE_KB is not None:
                await self._db.execute(f"PRAGMA cache_size = {self.config.SQLITE_CACHE_SIZE_KB};")
            await self._db.commit()
        return self._db

    async def initialize(self):
        """Initializes the database schema."""
        logger.info(f"Repository: Initializing SQLite DB schema at path: {self.db_path}")
        try:
            conn = await self._get_db()
            await conn.execute('''
            CREATE TABLE IF NOT EXISTS kems (
                id TEXT PRIMARY KEY,
                content_type TEXT,
                content BLOB,
                metadata TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            ''')
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_kems_created_at ON kems (created_at);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_kems_updated_at ON kems (updated_at);")

            try:
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_kems_meta_type ON kems(json_extract(metadata, '$.type'));")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_kems_meta_source_system ON kems(json_extract(metadata, '$.source_system'));")
            except aiosqlite.Error as e_json_idx:
                logger.warning(f"Could not create JSON indexes: {e_json_idx}")

            try:
                await conn.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS kems_fts USING fts5(
                    kem_id,
                    search_text,
                    tokenize = 'porter unicode61'
                )
                ''')
                await conn.execute('''
                CREATE TRIGGER IF NOT EXISTS kems_ai_fts AFTER INSERT ON kems
                BEGIN
                    INSERT INTO kems_fts (kem_id, search_text) VALUES (NEW.id, CAST(NEW.content AS TEXT) || ' ' || NEW.metadata);
                END;
                ''')
                await conn.execute('''
                CREATE TRIGGER IF NOT EXISTS kems_ad_fts AFTER DELETE ON kems
                BEGIN
                    DELETE FROM kems_fts WHERE kem_id = OLD.id;
                END;
                ''')
                await conn.execute('''
                CREATE TRIGGER IF NOT EXISTS kems_au_fts AFTER UPDATE ON kems
                BEGIN
                    DELETE FROM kems_fts WHERE kem_id = OLD.id;
                    INSERT INTO kems_fts (kem_id, search_text) VALUES (NEW.id, CAST(NEW.content AS TEXT) || ' ' || NEW.metadata);
                END;
                ''')
            except aiosqlite.Error as e_fts:
                logger.warning(f"Could not create FTS5 table: {e_fts}")

            await conn.commit()
            logger.info("Repository: SQLite schema initialized.")
        except Exception as e:
            logger.error(f"Repository: Error initializing SQLite: {e}", exc_info=True)
            raise

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def get_kem_creation_timestamp(self, kem_id: str) -> Optional[str]:
        conn = await self._get_db()
        async with conn.execute("SELECT created_at FROM kems WHERE id = ?", (kem_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def get_kems_creation_timestamps(self, kem_ids: List[str]) -> Dict[str, str]:
        if not kem_ids:
            return {}
        placeholders = ','.join('?' for _ in kem_ids)
        query = f"SELECT id, created_at FROM kems WHERE id IN ({placeholders})"
        conn = await self._get_db()
        async with conn.execute(query, tuple(kem_ids)) as cursor:
            rows = await cursor.fetchall()
            return {row[0]: row[1] for row in rows}

    async def store_or_replace_kem(self, kem_id: str, content_type: str, content: bytes, metadata_json: str, created_at_iso: str, updated_at_iso: str):
        conn = await self._get_db()
        await conn.execute('''
        INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (kem_id, content_type, content, metadata_json, created_at_iso, updated_at_iso))
        await conn.commit()
        logger.debug(f"Repository: KEM ID '{kem_id}' stored/replaced in SQLite.")

    async def get_full_kem_by_id(self, kem_id: str) -> Optional[Dict[str, Any]]:
        conn = await self._get_db()
        async with conn.execute("SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id = ?", (kem_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def delete_kem_by_id(self, kem_id: str) -> bool:
        conn = await self._get_db()
        cursor = await conn.execute("DELETE FROM kems WHERE id = ?", (kem_id,))
        await conn.commit()
        logger.debug(f"Repository: Attempted delete for KEM ID '{kem_id}'. Rows affected: {cursor.rowcount}")
        return cursor.rowcount > 0

    async def search_kems_by_text(self, text_query: str) -> List[str]:
        query = "SELECT kem_id FROM kems_fts WHERE search_text MATCH ? ORDER BY rank"
        conn = await self._get_db()
        try:
            async with conn.execute(query, (text_query,)) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
        except aiosqlite.Error as e_fts_search:
            logger.error(f"FTS search failed: {e_fts_search}", exc_info=True)
            return []

    async def retrieve_kems_from_db(self, sql_conditions: List[str], sql_params: List[Any], page_size: int, page_token: Optional[str], order_by_clause: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        paginated_sql_conditions = list(sql_conditions)
        paginated_sql_params = list(sql_params)

        primary_sort_col = "updated_at"
        secondary_sort_col = "id"
        primary_sort_order_desc = "DESC" in order_by_clause.upper() and primary_sort_col.upper() in order_by_clause.upper()

        if page_token:
            try:
                last_updated_at_str, last_id_str = page_token.split('|', 1)
                if primary_sort_order_desc:
                    keyset_condition = f"(({primary_sort_col} < ?) OR ({primary_sort_col} = ? AND {secondary_sort_col} < ?))"
                    paginated_sql_params.extend([last_updated_at_str, last_updated_at_str, last_id_str])
                else:
                    keyset_condition = f"(({primary_sort_col} > ?) OR ({primary_sort_col} = ? AND {secondary_sort_col} > ?))"
                    paginated_sql_params.extend([last_updated_at_str, last_updated_at_str, last_id_str])
                paginated_sql_conditions.append(keyset_condition)
            except ValueError:
                logger.warning(f"Invalid page_token format: '{page_token}'. Ignoring.")

        sql_where_clause = " WHERE " + " AND ".join(paginated_sql_conditions) if paginated_sql_conditions else ""
        query_paginated = f"SELECT id, content_type, content, metadata, created_at, updated_at FROM kems{sql_where_clause} {order_by_clause} LIMIT ?"
        paginated_sql_params.append(page_size + 1)

        conn = await self._get_db()
        async with conn.execute(query_paginated, tuple(paginated_sql_params)) as cursor:
            rows = await cursor.fetchall()

        found_kems_list = [dict(row) for row in rows]
        next_page_token_val: Optional[str] = None
        if len(found_kems_list) > page_size:
            last_item_for_current_page = found_kems_list[page_size - 1]
            next_token_updated_at = last_item_for_current_page.get(primary_sort_col)
            next_token_id = last_item_for_current_page.get(secondary_sort_col)
            if next_token_updated_at is not None and next_token_id is not None:
                next_page_token_val = f"{str(next_token_updated_at)}|{str(next_token_id)}"
            found_kems_list = found_kems_list[:page_size]

        return found_kems_list, next_page_token_val

    async def batch_store_or_replace_kems(self, kems_data: List[Dict[str, Any]]):
        if not kems_data:
            return
        sql = '''
            INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
            VALUES (:id, :content_type, :content, :metadata_json, :created_at_iso, :updated_at_iso)
        '''
        conn = await self._get_db()
        try:
            await conn.executemany(sql, kems_data)
            await conn.commit()
            logger.debug(f"Repository: Batch of {len(kems_data)} KEMs stored/replaced in SQLite.")
        except aiosqlite.Error as e_batch:
            logger.error(f"Repository: SQLite error during batch store/replace: {e_batch}", exc_info=True)
            raise

# The QdrantKemRepository remains synchronous as the qdrant-client library is synchronous.
# It will be called using asyncio.to_thread from the DefaultGLMRepository.
class QdrantKemRepository:
    def __init__(self, qdrant_client: QdrantClient, collection_name: str, default_vector_size: int, default_distance_metric: str):
        self.client = qdrant_client
        self.collection_name = collection_name
        self.default_vector_size = default_vector_size
        self.default_distance_metric_str = default_distance_metric.upper()

    def _get_qdrant_distance_metric(self) -> models.Distance:
        if self.default_distance_metric_str == "COSINE":
            return models.Distance.COSINE
        elif self.default_distance_metric_str == "DOT":
            return models.Distance.DOT
        elif self.default_distance_metric_str == "EUCLID":
            return models.Distance.EUCLID
        else:
            logger.warning(f"Unsupported Qdrant distance metric '{self.default_distance_metric_str}'. Defaulting to COSINE.")
            return models.Distance.COSINE

    def ensure_collection(self):
        logger.info(f"Repository: Ensuring Qdrant collection '{self.collection_name}' exists.")
        try:
            self.client.get_collection(collection_name=self.collection_name)
        except Exception as e:
            is_not_found_error = ("not found" in str(e).lower() or "404" in str(e).lower() or (hasattr(e, 'status_code') and e.status_code == 404))
            if is_not_found_error:
                distance_metric = self._get_qdrant_distance_metric()
                self.client.recreate_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(size=self.default_vector_size, distance=distance_metric)
                )
            else:
                logger.error(f"Repository: Error checking/creating Qdrant collection '{self.collection_name}': {e}", exc_info=True)
                raise

    def upsert_point(self, point: models.PointStruct):
        self.client.upsert(collection_name=self.collection_name, points=[point], wait=True)

    def upsert_points_batch(self, points: List[models.PointStruct]):
        if not points:
            return
        self.client.upsert(collection_name=self.collection_name, points=points, wait=True)

    def delete_points_by_ids(self, kem_ids: List[str]):
        if not kem_ids:
            return
        self.client.delete_points(
            collection_name=self.collection_name,
            points_selector=models.PointIdsList(points=kem_ids),
            wait=True
        )

    def search_points(self, query_vector: List[float], query_filter: Optional[models.Filter], limit: int, offset: int = 0, with_vectors: bool = False) -> List[models.ScoredPoint]:
        return self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=limit,
            offset=offset,
            with_payload=True,
            with_vectors=with_vectors
        )

    def retrieve_points_by_ids(self, kem_ids: List[str], with_vectors: bool = False) -> List[models.PointStruct]:
        if not kem_ids:
            return []
        return self.client.retrieve(
            collection_name=self.collection_name,
            ids=kem_ids,
            with_payload=True,
            with_vectors=with_vectors
        )

logger.info("GLM Repositories module loaded.")
