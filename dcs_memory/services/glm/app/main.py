import grpc
from concurrent import futures
import time
import sys
import os
import uuid
import json
import sqlite3
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import PointStruct
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict
import typing
import logging

# Import configuration
from .config import GLMConfig

# --- Start of gRPC code import block ---
current_script_path = os.path.abspath(__file__)
app_dir = os.path.dirname(current_script_path)
# --- End of app_dir definition ---

# Global configuration instance
config = GLMConfig()

# --- Logging setup ---
logging.basicConfig(
    level=config.get_log_level_int(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
# --- End of logging setup ---

service_root_dir = os.path.dirname(app_dir)
if service_root_dir not in sys.path:
    sys.path.insert(0, service_root_dir)

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2
from generated_grpc import glm_service_pb2_grpc
from google.protobuf import empty_pb2
# --- End of gRPC code import block ---

# _ONE_DAY_IN_SECONDS = 60 * 60 * 24 # Unused variable

class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self):
        logger.info("Initializing GlobalLongTermMemoryServicerImpl...")
        self.qdrant_client = None
        self.config = config

        self.sqlite_db_path = os.path.join(app_dir, self.config.DB_FILENAME)

        try:
            self.qdrant_client = QdrantClient(host=self.config.QDRANT_HOST, port=self.config.QDRANT_PORT, timeout=10)
            self.qdrant_client.get_collections() # Test connection
            logger.info(f"Qdrant client successfully connected to {self.config.QDRANT_HOST}:{self.config.QDRANT_PORT}")
            self._ensure_qdrant_collection()
        except Exception as e:
            logger.error(f"CRITICAL ERROR during Qdrant client initialization: {e}. Service may not work correctly.")
            self.qdrant_client = None

        self._init_sqlite()
        logger.info("GLM servicer initialized.")

    def _get_sqlite_conn(self):
        conn = sqlite3.connect(self.sqlite_db_path, timeout=10)
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute("PRAGMA foreign_keys=ON;")
            cursor.execute("PRAGMA busy_timeout = 7500;")
        except sqlite3.Error as e:
            logger.error(f"Error setting SQLite PRAGMA options: {e}", exc_info=True) # Already has exc_info=True
        return conn

    def _init_sqlite(self):
        logger.info(f"Initializing SQLite DB at path: {self.sqlite_db_path}")
        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor()
                if logger.isEnabledFor(logging.INFO):
                    jm = cursor.execute("PRAGMA journal_mode;").fetchone()
                    sm = cursor.execute("PRAGMA synchronous;").fetchone()
                    fk = cursor.execute("PRAGMA foreign_keys;").fetchone()
                    bt = cursor.execute("PRAGMA busy_timeout;").fetchone()
                    logger.info(f"SQLite PRAGMA at initialization: journal_mode={jm}, synchronous={sm}, foreign_keys={fk}, busy_timeout={bt}")

                cursor.execute('''
                CREATE TABLE IF NOT EXISTS kems (
                    id TEXT PRIMARY KEY,
                    content_type TEXT,
                    content BLOB,
                    metadata TEXT, /* JSON stored as TEXT */
                    created_at TEXT, /* ISO 8601 format */
                    updated_at TEXT  /* ISO 8601 format */
                )
                ''')
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_created_at ON kems (created_at);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_updated_at ON kems (updated_at);")

                # Add JSON indexes for frequently queried metadata fields
                # NOTE: Requires SQLite version 3.38.0+ for JSON expression indexes.
                # Example fields: 'type' and 'source_system'
                # These fields should ideally be defined in a config or as constants.
                # For now, using hypothetical field names directly.
                logger.info("Attempting to create JSON indexes on metadata fields (requires SQLite >= 3.38.0)...")
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_meta_type ON kems(json_extract(metadata, '$.type'));")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_meta_source_system ON kems(json_extract(metadata, '$.source_system'));")
                    logger.info("JSON indexes (meta_type, meta_source_system) creation attempted.")
                except sqlite3.Error as e_json_idx:
                    # Older SQLite versions might not support this. Log a warning but continue.
                    logger.warning(f"Could not create JSON indexes (SQLite < 3.38.0 or other error): {e_json_idx}. "
                                   "Filtering on these metadata fields might be slower.")

                conn.commit()
            logger.info("'kems' table and indexes in SQLite successfully initialized.")
        except Exception as e:
            logger.error(f"Error initializing SQLite: {e}", exc_info=True)

    def _ensure_qdrant_collection(self):
        if not self.qdrant_client:
            logger.warning("Qdrant client not initialized, skipping collection creation/check.")
            return
        try:
            try:
                collection_info = self.qdrant_client.get_collection(self.config.QDRANT_COLLECTION)
                logger.info(f"Collection '{self.config.QDRANT_COLLECTION}' already exists.")
                # Check existing configuration if collection exists
                if hasattr(collection_info.config.params.vectors, 'size'): # For standard vectors
                    current_vector_size = collection_info.config.params.vectors.size
                    current_distance = collection_info.config.params.vectors.distance
                elif isinstance(collection_info.config.params.vectors, dict): # For named vectors
                    logger.info("Detailed configuration check for named vectors is not yet implemented.")
                    current_vector_size = self.config.DEFAULT_VECTOR_SIZE # Assume match for now
                    current_distance = models.Distance.COSINE        # Assume match
                else: # Unknown vector configuration format
                     logger.warning(f"Could not determine vector configuration for collection '{self.config.QDRANT_COLLECTION}'. Skipping check.")
                     current_vector_size = self.config.DEFAULT_VECTOR_SIZE
                     current_distance = models.Distance.COSINE

                if current_vector_size != self.config.DEFAULT_VECTOR_SIZE or current_distance != models.Distance.COSINE:
                     logger.warning(f"Configuration of existing collection '{self.config.QDRANT_COLLECTION}' (size: {current_vector_size}, distance: {current_distance}) "
                                    f"does not match expected (size: {self.config.DEFAULT_VECTOR_SIZE}, distance: {models.Distance.COSINE}).")
                else:
                    logger.info(f"Collection configuration for '{self.config.QDRANT_COLLECTION}' is consistent.")
            except Exception as e_get_collection: # Catch error when getting collection (e.g., if it doesn't exist)
                if "not found" in str(e_get_collection).lower() or \
                   (hasattr(e_get_collection, 'status_code') and e_get_collection.status_code == 404): # type: ignore
                    logger.info(f"Collection '{self.config.QDRANT_COLLECTION}' not found. Creating new collection...")
                    self.qdrant_client.recreate_collection(
                        collection_name=self.config.QDRANT_COLLECTION,
                        vectors_config=models.VectorParams(size=self.config.DEFAULT_VECTOR_SIZE, distance=models.Distance.COSINE)
                    )
                    logger.info(f"Collection '{self.config.QDRANT_COLLECTION}' successfully created.")
                else: # Other error during get_collection
                    logger.error(f"Error getting Qdrant collection info for '{self.config.QDRANT_COLLECTION}': {e_get_collection}", exc_info=True)
        except Exception as e: # General error for recreate_collection etc.
            logger.error(f"Error ensuring Qdrant collection '{self.config.QDRANT_COLLECTION}': {e}", exc_info=True)

    def _kem_dict_to_proto(self, kem_data: dict) -> kem_pb2.KEM:
        kem_data_copy = kem_data.copy()
        if 'created_at' in kem_data_copy and not isinstance(kem_data_copy['created_at'], str):
            del kem_data_copy['created_at']
        if 'updated_at' in kem_data_copy and not isinstance(kem_data_copy['updated_at'], str):
            del kem_data_copy['updated_at']
        if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
             kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')
        return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)

    def _kem_from_db_row(self, row: sqlite3.Row, embeddings_map: typing.Optional[dict] = None) -> kem_pb2.KEM:
        kem_dict = dict(row)
        kem_dict['metadata'] = json.loads(kem_dict.get('metadata', '{}'))
        created_at_ts = Timestamp()
        created_at_str = kem_dict.get('created_at')
        if created_at_str: # Check if the string is not None or empty
            if not created_at_str.endswith("Z"): created_at_str += "Z" # Ensure Zulu timezone for FromJsonString
            created_at_ts.FromJsonString(created_at_str)
        kem_dict['created_at'] = created_at_ts

        updated_at_ts = Timestamp()
        updated_at_str = kem_dict.get('updated_at')
        if updated_at_str: # Check if the string is not None or empty
            if not updated_at_str.endswith("Z"): updated_at_str += "Z"
            updated_at_ts.FromJsonString(updated_at_str)
        kem_dict['updated_at'] = updated_at_ts
        if embeddings_map and kem_dict['id'] in embeddings_map:
            kem_dict['embeddings'] = embeddings_map[kem_dict['id']]
        else: # Ensure embeddings field is present even if empty
            kem_dict.setdefault('embeddings', [])
        return self._kem_dict_to_proto(kem_dict)

    def StoreKEM(self, request: glm_service_pb2.StoreKEMRequest, context) -> glm_service_pb2.StoreKEMResponse:
        kem = request.kem
        kem_id = kem.id if kem.id else str(uuid.uuid4())
        logger.info(f"StoreKEM: ID='{kem_id}' (client-provided ID='{request.kem.id}')")

        current_time_proto = Timestamp()
        current_time_proto.GetCurrentTime()
        final_created_at_proto = Timestamp()
        is_new_kem = True

        # Prepare KEM data (ID, timestamps)
        kem.id = kem_id # Ensure ID is set on the proto
        # Determine created_at (logic moved before SQLite transaction for clarity, actual check still needed inside for existing)
        # This is a preliminary created_at, will be finalized based on DB state.
        if kem.HasField("created_at") and kem.created_at.seconds > 0:
            preliminary_created_at = kem.created_at
        else:
            preliminary_created_at = current_time_proto

        kem.updated_at.CopyFrom(current_time_proto) # Always set/update updated_at

        sqlite_committed = False
        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor()

                # Check if KEM exists to determine final created_at
                cursor.execute("SELECT created_at FROM kems WHERE id = ?", (kem_id,))
                existing_row = cursor.fetchone()
                if existing_row:
                    is_new_kem = False
                    final_created_at_proto.FromJsonString(existing_row[0] + ("Z" if not existing_row[0].endswith("Z") else ""))
                else: # is_new_kem is True
                    final_created_at_proto.CopyFrom(preliminary_created_at)

                kem.created_at.CopyFrom(final_created_at_proto)

                # Perform SQLite INSERT/REPLACE (without commit yet)
                logger.debug(f"StoreKEM: Attempting SQLite INSERT/REPLACE for ID '{kem_id}'.")
                cursor.execute('''
                INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (kem.id, kem.content_type, kem.content,
                      json.dumps(dict(kem.metadata)),
                      kem.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''), # Store as naive UTC string, seconds precision
                      kem.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')))
                logger.debug(f"StoreKEM: SQLite INSERT/REPLACE for ID '{kem_id}' executed by cursor.")

                # Perform Qdrant operation if needed (before SQLite commit)
                if self.qdrant_client and kem.embeddings:
                    if len(kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                        msg = f"Embedding dimension ({len(kem.embeddings)}) does not match Qdrant collection configuration ({self.config.DEFAULT_VECTOR_SIZE})."
                        logger.error(msg)
                        # SQLite changes will be rolled back due to context abort / exception before commit.
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return glm_service_pb2.StoreKEMResponse()

                    logger.debug(f"StoreKEM: Attempting Qdrant upsert for ID '{kem_id}'.")
                    qdrant_payload = {"kem_id_ref": kem_id}
                    if kem.metadata:
                        for k, v_pb_val in kem.metadata.items():
                            qdrant_payload[f"md_{k}"] = v_pb_val
                    if kem.HasField("created_at"):
                        qdrant_payload["created_at_ts"] = kem.created_at.seconds
                    if kem.HasField("updated_at"):
                        qdrant_payload["updated_at_ts"] = kem.updated_at.seconds

                    self.qdrant_client.upsert(
                        collection_name=self.config.QDRANT_COLLECTION,
                        points=[PointStruct(id=kem_id, vector=list(kem.embeddings), payload=qdrant_payload)],
                        wait=True # Ensure Qdrant operation completes before SQLite commit
                    )
                    logger.info(f"Embeddings and payload for KEM ID '{kem_id}' successfully upserted to Qdrant.")

                # All operations successful, commit SQLite
                conn.commit()
                sqlite_committed = True
                logger.info(f"Metadata/content for KEM ID '{kem_id}' saved/updated in SQLite and Qdrant (if applicable), transaction committed.")

        except grpc.RpcError: # e.g. context.abort called
            logger.warning(f"StoreKEM: gRPC RpcError encountered for ID '{kem_id}'. SQLite commit status: {sqlite_committed}.")
            raise # Re-throw to be handled by gRPC server framework
        except Exception as e:
            msg = f"Error during StoreKEM for ID '{kem_id}': {e}. SQLite commit status: {sqlite_committed}."
            logger.error(msg, exc_info=True)
            # If SQLite was not committed, 'with conn:' handles rollback on exception.
            # If Qdrant failed, SQLite was not committed.
            context.abort(grpc.StatusCode.INTERNAL, msg); return glm_service_pb2.StoreKEMResponse()

        logger.info(f"KEM ID '{kem_id}' successfully saved/updated overall.")
        return glm_service_pb2.StoreKEMResponse(kem=kem)

    def RetrieveKEMs(self, request: glm_service_pb2.RetrieveKEMsRequest, context) -> glm_service_pb2.RetrieveKEMsResponse:
        query = request.query
        page_size = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
        offset = 0
        if request.page_token:
            try:
                offset = int(request.page_token)
                if offset < 0: offset = 0 # Offset cannot be negative
            except ValueError:
                logger.warning(f"Invalid page_token format: '{request.page_token}'. Using offset=0.")

        logger.info(f"RetrieveKEMs: query_filters={query.metadata_filters}, query_ids={list(query.ids)}, "
                    f"vector_query_present={bool(query.embedding_query)}, page_size={page_size}, offset={offset}")

        found_kems_proto = []
        next_offset_str = ""
        embeddings_from_qdrant = {}

        def _build_qdrant_filter_local(kem_query: glm_service_pb2.KEMQuery) -> typing.Optional[models.Filter]:
            q_conditions = []
            if kem_query.ids:
                # Qdrant point IDs are strings, KEM IDs are strings.
                q_conditions.append(models.HasIdCondition(has_id=list(kem_query.ids)))

            if kem_query.metadata_filters:
                for k, v_str in kem_query.metadata_filters.items():
                    # Assuming metadata values are simple types that Qdrant can match.
                    # Numbers might need specific handling if stored as numbers in Qdrant payload.
                    # For now, assuming string match is sufficient for md_<key>.
                    q_conditions.append(models.FieldCondition(key=f"md_{k}", match=models.MatchValue(value=v_str)))

            def add_ts_range_condition(field_name, ts_start, ts_end):
                gte_val, lte_val = None, None
                if ts_start and (ts_start.seconds > 0 or ts_start.nanos > 0): gte_val = ts_start.seconds
                if ts_end and (ts_end.seconds > 0 or ts_end.nanos > 0): lte_val = ts_end.seconds
                if gte_val is not None or lte_val is not None:
                    q_conditions.append(models.FieldCondition(key=field_name, range=models.Range(gte=gte_val, lte=lte_val)))

            add_ts_range_condition("created_at_ts", kem_query.created_at_start, kem_query.created_at_end)
            add_ts_range_condition("updated_at_ts", kem_query.updated_at_start, kem_query.updated_at_end)

            return models.Filter(must=q_conditions) if q_conditions else None

        if query.embedding_query:
            if not self.qdrant_client:
                context.abort(grpc.StatusCode.INTERNAL, "Qdrant service unavailable."); return glm_service_pb2.RetrieveKEMsResponse()
            if len(query.embedding_query) != self.config.DEFAULT_VECTOR_SIZE:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid vector dimension: {len(query.embedding_query)}, expected {self.config.DEFAULT_VECTOR_SIZE}"); return glm_service_pb2.RetrieveKEMsResponse()
            try:
                qdrant_filter_obj = _build_qdrant_filter_local(query)
                logger.debug(f"RetrieveKEMs (vector search): Qdrant filter object: {qdrant_filter_obj}")

                search_result = self.qdrant_client.search(
                    collection_name=self.config.QDRANT_COLLECTION,
                    query_vector=list(query.embedding_query),
                    query_filter=qdrant_filter_obj,
                    limit=page_size,
                    offset=offset,
                    with_vectors=True # Retrieve vectors to populate KEM proto
                )

                qdrant_ids_retrieved = [hit.id for hit in search_result]
                for hit in search_result:
                    if hit.vector: embeddings_from_qdrant[hit.id] = list(hit.vector)

                if not qdrant_ids_retrieved:
                    return glm_service_pb2.RetrieveKEMsResponse(kems=[], next_page_token="")

                # Fetch from SQLite using the order from Qdrant
                placeholders = ','.join('?' for _ in qdrant_ids_retrieved)
                sql_query_final = f"SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id IN ({placeholders}) ORDER BY INSTR(',' || ? || ',', ',' || id || ',')"
                # The INSTR trick preserves Qdrant's ordering by searching for ',id,' in ',id1,id2,id3,'
                ordered_ids_string = ',' + ','.join(qdrant_ids_retrieved) + ','

                with self._get_sqlite_conn() as conn:
                    conn.row_factory = sqlite3.Row; cursor = conn.cursor()
                    cursor.execute(sql_query_final, (ordered_ids_string,) + tuple(qdrant_ids_retrieved) )
                    for row_dict in cursor.fetchall():
                        found_kems_proto.append(self._kem_from_db_row(row_dict, embeddings_from_qdrant))

                if len(search_result) == page_size: # Potential next page if Qdrant returned a full page
                    # To confirm, Qdrant's SearchResponse usually has a `next_page_offset` if available.
                    # Or we can assume if limit items were returned, there might be more.
                    next_offset_str = str(offset + page_size)

            except Exception as e:
                logger.error(f"Qdrant error (RetrieveKEMs - vector search): {e}", exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"Qdrant error: {e}"); return glm_service_pb2.RetrieveKEMsResponse()
        else: # Non-vector search (SQLite primary)
            sql_conditions = []; sql_params = []
            if query.ids:
                placeholders = ','.join('?' for _ in query.ids)
                sql_conditions.append(f"id IN ({placeholders})"); sql_params.extend(query.ids)

            for key, value in query.metadata_filters.items():
                sql_conditions.append(f"json_extract(metadata, '$.{key}') = ?"); sql_params.append(value)

            def add_date_condition(field_name, proto_ts, op):
                if proto_ts.seconds > 0 or proto_ts.nanos > 0:
                    # SQLite stores dates as TEXT in ISO format (YYYY-MM-DD HH:MM:SS)
                    # Using timespec='seconds' ensures this format.
                    # replace('+00:00', '') is not strictly needed here if DB stores naive UTC and comparison is also naive.
                    # However, to be absolutely sure about format, let's ensure it matches the storage format.
                    # Stored format is: kem.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
                    sql_conditions.append(f"{field_name} {op} ?"); sql_params.append(proto_ts.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))

            add_date_condition("created_at", query.created_at_start, ">="); add_date_condition("created_at", query.created_at_end, "<=")
            add_date_condition("updated_at", query.updated_at_start, ">="); add_date_condition("updated_at", query.updated_at_end, "<=")

            sql_where_clause = " WHERE " + " AND ".join(sql_conditions) if sql_conditions else ""
            # Default sort for non-vector search; can be made configurable
            sql_query_ordered = f"SELECT id, content_type, content, metadata, created_at, updated_at FROM kems{sql_where_clause} ORDER BY updated_at DESC"
            sql_query_paginated = f"{sql_query_ordered} LIMIT ? OFFSET ?"

            try:
                with self._get_sqlite_conn() as conn:
                    conn.row_factory = sqlite3.Row; cursor = conn.cursor()
                    final_sql_params = sql_params + [page_size, offset]
                    cursor.execute(sql_query_paginated, final_sql_params)
                    rows = cursor.fetchall()

                    ids_from_sqlite = [row['id'] for row in rows]
                    if ids_from_sqlite and self.qdrant_client: # Fetch embeddings if Qdrant is available
                        try:
                            qdrant_points = self.qdrant_client.retrieve(
                                collection_name=self.config.QDRANT_COLLECTION,
                                ids=ids_from_sqlite,
                                with_vectors=True
                            )
                            for point in qdrant_points:
                                if point.vector: embeddings_from_qdrant[point.id] = list(point.vector)
                        except Exception as e_qd_retrieve:
                            logger.warning(f"Failed to retrieve embeddings for KEMs from Qdrant: {e_qd_retrieve}")

                    for row_dict in rows:
                        found_kems_proto.append(self._kem_from_db_row(row_dict, embeddings_from_qdrant))

                    if len(rows) == page_size: # Check if there might be a next page
                        cursor_count = conn.cursor()
                        # Check if there's at least one more record beyond the current page
                        count_params = sql_params + [offset + page_size]
                        cursor_count.execute(f"SELECT EXISTS({sql_query_ordered} LIMIT 1 OFFSET ?)", count_params)
                        if cursor_count.fetchone()[0] == 1:
                            next_offset_str = str(offset + page_size)
            except Exception as e:
                logger.error(f"SQLite error (RetrieveKEMs non-vector): {e}", exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"SQLite error: {e}"); return glm_service_pb2.RetrieveKEMsResponse()

        return glm_service_pb2.RetrieveKEMsResponse(kems=found_kems_proto, next_page_token=next_offset_str)

    def UpdateKEM(self, request: glm_service_pb2.UpdateKEMRequest, context) -> kem_pb2.KEM:
        kem_id = request.kem_id
        kem_data_update = request.kem_data_update
        if not kem_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID must be specified for update.")
            return kem_pb2.KEM()

        current_kem_dict_from_db = {}
        original_created_at_iso_str = ""
        final_embeddings_for_response = [] # Store embeddings for the response KEM

        # Timestamps
        ts_now = Timestamp()
        ts_now.GetCurrentTime()
        updated_at_iso_str = ts_now.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')

        sqlite_committed = False
        try:
            # Step 1: Retrieve current KEM state from SQLite to prepare updates
            with self._get_sqlite_conn() as conn_check:
                cursor_check = conn_check.cursor()
                cursor_check.execute("SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id = ?", (kem_id,))
                row = cursor_check.fetchone()
                if not row:
                    context.abort(grpc.StatusCode.NOT_FOUND, f"KEM with ID '{kem_id}' not found for update.")
                    return kem_pb2.KEM()

                current_kem_dict_from_db = dict(row)
                current_kem_dict_from_db['metadata'] = json.loads(current_kem_dict_from_db.get('metadata', '{}'))
                original_created_at_iso_str = current_kem_dict_from_db['created_at'] # Preserve original creation time string

            # Step 2: Prepare updated KEM data based on request
            updated_content_type = current_kem_dict_from_db['content_type']
            if kem_data_update.HasField("content_type"):
                updated_content_type = kem_data_update.content_type

            updated_content = current_kem_dict_from_db['content']
            if kem_data_update.HasField("content"):
                updated_content = kem_data_update.content.value

            updated_metadata_dict = current_kem_dict_from_db['metadata']
            if kem_data_update.metadata: # If metadata is provided in update, it replaces the old one
                updated_metadata_dict = dict(kem_data_update.metadata)

            # Step 3: Qdrant Operation (before SQLite commit)
            if self.qdrant_client:
                logger.debug(f"UpdateKEM: Preparing Qdrant update for ID '{kem_id}'.")
                qdrant_payload_update = {"kem_id_ref": kem_id}
                if updated_metadata_dict: # Use potentially updated metadata
                    for k, v_str in updated_metadata_dict.items():
                        qdrant_payload_update[f"md_{k}"] = v_str

                try:
                    created_at_proto_for_qdrant = Timestamp() # For Qdrant timestamp payload
                    created_at_proto_for_qdrant.FromJsonString(original_created_at_iso_str + ("Z" if not original_created_at_iso_str.endswith("Z") else ""))
                    qdrant_payload_update["created_at_ts"] = created_at_proto_for_qdrant.seconds
                except Exception as e_ts_create:
                    logger.warning(f"UpdateKEM: Failed to parse original_created_at_iso ('{original_created_at_iso_str}') for Qdrant payload: {e_ts_create}", exc_info=True)
                qdrant_payload_update["updated_at_ts"] = ts_now.seconds

                if kem_data_update.embeddings: # New embeddings provided
                    new_embeddings = list(kem_data_update.embeddings)
                    if len(new_embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                        msg = f"Invalid embedding dimension for KEM ID '{kem_id}': {len(new_embeddings)}, expected {self.config.DEFAULT_VECTOR_SIZE}"
                        logger.error(msg)
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return kem_pb2.KEM()

                    logger.info(f"UpdateKEM: Updating vector and payload for KEM ID '{kem_id}' in Qdrant.")
                    self.qdrant_client.upsert(
                        collection_name=self.config.QDRANT_COLLECTION,
                        points=[PointStruct(id=kem_id, vector=new_embeddings, payload=qdrant_payload_update)],
                        wait=True
                    )
                    final_embeddings_for_response = new_embeddings
                else: # No new embeddings provided, only update payload, preserve vector
                    logger.info(f"UpdateKEM: Updating payload (preserving vector) for KEM ID '{kem_id}' in Qdrant.")
                    self.qdrant_client.set_payload(
                        collection_name=self.config.QDRANT_COLLECTION,
                        payload=qdrant_payload_update,
                        points=[kem_id], # Qdrant expects list of point IDs
                        wait=True
                    )
                    # For response, try to fetch existing vector if not provided in update
                    points_resp = self.qdrant_client.retrieve(self.config.QDRANT_COLLECTION, ids=[kem_id], with_vectors=True)
                    if points_resp and points_resp[0].vector:
                        final_embeddings_for_response = list(points_resp[0].vector)
                    else: # No vector or couldn't retrieve
                        final_embeddings_for_response = []
                logger.info(f"UpdateKEM: Qdrant operation for ID '{kem_id}' successful.")

            # Step 4: Update SQLite (now that Qdrant op, if any, was successful)
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor()
                logger.debug(f"UpdateKEM: Attempting SQLite UPDATE for ID '{kem_id}'.")
                cursor.execute("UPDATE kems SET content_type = ?, content = ?, metadata = ?, updated_at = ? WHERE id = ?",
                               (updated_content_type, updated_content,
                                json.dumps(updated_metadata_dict),
                                updated_at_iso_str, kem_id))
                conn.commit()
                sqlite_committed = True
                logger.info(f"UpdateKEM: SQLite record for ID '{kem_id}' updated and committed.")

            # Step 5: Construct KEM to return
            response_kem_dict = {
                'id': kem_id,
                'content_type': updated_content_type,
                'content': updated_content, # content is already bytes if from db or from proto bytes_value
                'metadata': updated_metadata_dict,
                'created_at': original_created_at_iso_str, # Original creation time string
                'updated_at': updated_at_iso_str,       # New update time string
                'embeddings': final_embeddings_for_response
            }
            return self._kem_dict_to_proto(response_kem_dict)

        except grpc.RpcError:
            logger.warning(f"UpdateKEM: gRPC RpcError encountered for ID '{kem_id}'. SQLite commit status: {sqlite_committed}.")
            raise
        except Exception as e:
            logger.error(f"Error updating KEM ID '{kem_id}': {e}. SQLite commit status: {sqlite_committed}.", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Update error for KEM ID '{kem_id}': {e}"); return kem_pb2.KEM()

    def DeleteKEM(self, request: glm_service_pb2.DeleteKEMRequest, context) -> empty_pb2.Empty:
        kem_id = request.kem_id
        if not kem_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID must be specified for deletion.")
            return empty_pb2.Empty()

        try:
            # Step 1: Delete from Qdrant first
            if self.qdrant_client:
                logger.debug(f"DeleteKEM: Attempting to delete point for KEM ID '{kem_id}' from Qdrant.")
                try:
                    self.qdrant_client.delete_points(
                        collection_name=self.config.QDRANT_COLLECTION,
                        points_selector=models.PointIdsList(points=[kem_id]),
                        wait=True # Ensure operation completes
                    )
                    logger.info(f"Point for KEM ID '{kem_id}' successfully deleted/marked for deletion from Qdrant (or was not found).")
                except Exception as e_qd_del:
                    msg = f"Qdrant error while deleting point for KEM ID '{kem_id}': {e_qd_del}"
                    logger.error(msg, exc_info=True)
                    context.abort(grpc.StatusCode.INTERNAL, msg); return empty_pb2.Empty()

            # Step 2: Delete from SQLite
            logger.debug(f"DeleteKEM: Attempting to delete KEM ID '{kem_id}' from SQLite.")
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM kems WHERE id = ?", (kem_id,))
                conn.commit()
                if cursor.rowcount == 0:
                    logger.warning(f"KEM ID '{kem_id}' not found in SQLite for deletion (or was already deleted). Qdrant delete was attempted if configured.")
                else:
                    logger.info(f"KEM ID '{kem_id}' successfully deleted from SQLite.")

            logger.info(f"DeleteKEM: Successfully processed deletion for KEM ID '{kem_id}'.")
            return empty_pb2.Empty()

        except grpc.RpcError:
            logger.warning(f"DeleteKEM: gRPC RpcError encountered for ID '{kem_id}'.")
            raise
        except Exception as e:
            # This would typically catch errors from _get_sqlite_conn or other unexpected issues
            msg = f"Unexpected error in DeleteKEM for ID '{kem_id}': {e}"
            logger.error(msg, exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, msg); return empty_pb2.Empty()

    def BatchStoreKEMs(self, request: glm_service_pb2.BatchStoreKEMsRequest, context) -> glm_service_pb2.BatchStoreKEMsResponse:
        logger.info(f"BatchStoreKEMs: Received {len(request.kems)} KEMs for storage.")
        successfully_stored_kems_list = []
        failed_kem_references_list = []

        for idx, kem_in_req in enumerate(request.kems):
            # Create a mutable copy of the KEM from request
            current_kem_to_process = kem_pb2.KEM()
            current_kem_to_process.CopyFrom(kem_in_req)

            kem_id = current_kem_to_process.id if current_kem_to_process.id else str(uuid.uuid4())
            current_kem_to_process.id = kem_id

            current_time_proto = Timestamp(); current_time_proto.GetCurrentTime()
            final_created_at_proto = Timestamp()
            is_new_kem_in_db = True
            sqlite_persisted_this_op = False
            qdrant_persisted_this_op = True # Assume success if no embeddings or no Qdrant client

            # Step 1: Process KEMs for SQLite and prepare for Qdrant
            kems_for_sqlite_processing = [] # Tuples of (KEM_proto, original_client_id_or_index_ref)
            qdrant_points_to_upsert = []

            for idx, kem_in_req in enumerate(request.kems):
                current_kem_to_process = kem_pb2.KEM()
                current_kem_to_process.CopyFrom(kem_in_req)
                original_client_ref = kem_in_req.id if kem_in_req.id else f"req_idx_{idx}"

                kem_id = current_kem_to_process.id if current_kem_to_process.id else str(uuid.uuid4())
                current_kem_to_process.id = kem_id

                # Timestamp handling (common for SQLite and Qdrant payload)
                current_time_proto = Timestamp(); current_time_proto.GetCurrentTime()
                final_created_at_proto = Timestamp()
                is_new_kem_in_db_check_needed = True # Assume we need to check, can be optimized if client guarantees new IDs

                # Check for Qdrant viability BEFORE adding to SQLite processing list if embeddings are present
                if current_kem_to_process.embeddings:
                    if not self.qdrant_client:
                        logger.error(f"BatchStoreKEMs: Qdrant client not available. KEM ID '{kem_id}' (ref: {original_client_ref}) with embeddings cannot be stored.")
                        failed_kem_references_list.append(original_client_ref)
                        continue # Skip this KEM entirely, do not attempt SQLite persistence

                    if len(current_kem_to_process.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                        logger.error(f"BatchStoreKEMs: Invalid embedding dimension for KEM ID '{kem_id}' (ref: {original_client_ref}). Expected {self.config.DEFAULT_VECTOR_SIZE}, got {len(current_kem_to_process.embeddings)}.")
                        failed_kem_references_list.append(original_client_ref) # Early fail for this KEM
                        continue # Skip this KEM entirely

                # If all checks passed (or no embeddings), add to SQLite processing list
                kems_for_sqlite_processing.append({'proto': current_kem_to_process, 'original_ref': original_client_ref, 'final_created_at': final_created_at_proto, 'current_time': current_time_proto, 'is_new_check_needed': is_new_kem_in_db_check_needed})

                # If it has embeddings (and passed checks), also add to Qdrant list
                if current_kem_to_process.embeddings:
                    # qdrant_payload construction is deferred until after SQLite success confirms final timestamps
                    qdrant_points_to_upsert.append({'id': kem_id, 'vector': list(current_kem_to_process.embeddings),
                                                    'payload_kem_proto_ref': current_kem_to_process, # Will be updated by SQLite step
                                                    'original_ref': original_client_ref})

            # Step 2: Batch persist to SQLite
            processed_for_qdrant_kems_map = {} # kem_id -> KEM_proto (with final timestamps)

            with self._get_sqlite_conn() as conn: # Single transaction for all SQLite operations
                cursor = conn.cursor()

                # Pre-fetch existing created_at timestamps for KEMs that have an ID
                ids_to_check = [
                    kem_data['proto'].id
                    for kem_data in kems_for_sqlite_processing
                    if kem_data['proto'].id and kem_data['is_new_check_needed']
                ]
                existing_kems_created_at_map: Dict[str, str] = {}
                if ids_to_check:
                    placeholders = ','.join('?' for _ in ids_to_check)
                    cursor.execute(f"SELECT id, created_at FROM kems WHERE id IN ({placeholders})", tuple(ids_to_check))
                    for row in cursor.fetchall():
                        existing_kems_created_at_map[row[0]] = row[1]

                logger.debug(f"BatchStoreKEMs: Pre-fetched created_at for {len(existing_kems_created_at_map)} existing KEMs.")

                for kem_data_for_sqlite in kems_for_sqlite_processing:
                    kem_proto = kem_data_for_sqlite['proto']
                    original_ref = kem_data_for_sqlite['original_ref']
                    final_created_at_proto_local = kem_data_for_sqlite['final_created_at'] # This is a Timestamp object
                    current_time_proto_local = kem_data_for_sqlite['current_time']     # This is a Timestamp object

                    try:
                        kem_id_str = kem_proto.id # Should be populated at this point
                        if kem_id_str in existing_kems_created_at_map:
                            # KEM exists, use its stored created_at
                            created_at_str_from_db = existing_kems_created_at_map[kem_id_str]
                            final_created_at_proto_local.FromJsonString(created_at_str_from_db + ("Z" if not created_at_str_from_db.endswith("Z") else ""))
                        else:
                            # KEM is new or ID was generated server-side and thus not in existing_kems_created_at_map
                            # Use client-provided created_at if valid, otherwise current_time
                            if kem_proto.HasField("created_at") and kem_proto.created_at.seconds > 0:
                                final_created_at_proto_local.CopyFrom(kem_proto.created_at)
                            else:
                                final_created_at_proto_local.CopyFrom(current_time_proto_local)

                        kem_proto.created_at.CopyFrom(final_created_at_proto_local)
                        kem_proto.updated_at.CopyFrom(current_time_proto_local) # updated_at is always current time for the batch op

                        cursor.execute('''
                        INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ''', (kem_proto.id, kem_proto.content_type,
                              kem_proto.content, json.dumps(dict(kem_proto.metadata)),
                      kem_proto.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                      kem_proto.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')))

                        processed_for_qdrant_kems_map[kem_proto.id] = kem_proto # Mark as SQLite success
                    except Exception as e_sqlite_item:
                        logger.error(f"BatchStoreKEMs: SQLite error for KEM ID '{kem_proto.id}' (ref: {original_ref}): {e_sqlite_item}", exc_info=True)
                        failed_kem_references_list.append(original_ref)
                conn.commit() # Commit all successful SQLite operations

            # Step 3: Prepare final Qdrant points for those that succeeded SQLite
            final_qdrant_points_batch = []
            kems_with_embeddings_that_succeeded_sqlite = []

            for q_point_data in qdrant_points_to_upsert:
                kem_id = q_point_data['id']
                if kem_id in processed_for_qdrant_kems_map: # If SQLite part was successful
                    kem_proto_for_payload = processed_for_qdrant_kems_map[kem_id]
                    qdrant_payload = {"kem_id_ref": kem_id} # Rebuild payload with final timestamps
                    if kem_proto_for_payload.metadata:
                        for k, v_str in kem_proto_for_payload.metadata.items(): qdrant_payload[f"md_{k}"] = v_str
                    if kem_proto_for_payload.HasField("created_at"): qdrant_payload["created_at_ts"] = kem_proto_for_payload.created_at.seconds
                    if kem_proto_for_payload.HasField("updated_at"): qdrant_payload["updated_at_ts"] = kem_proto_for_payload.updated_at.seconds

                    final_qdrant_points_batch.append(PointStruct(id=kem_id, vector=q_point_data['vector'], payload=qdrant_payload))
                    kems_with_embeddings_that_succeeded_sqlite.append(kem_id) # Track for potential rollback
                # If not in processed_for_qdrant_kems_map, it means its SQLite part failed, already in failed_kem_references_list.

            # Step 4: Batch upsert to Qdrant
            qdrant_batch_success = True
            if final_qdrant_points_batch:
                if not self.qdrant_client: # Should have been caught earlier per KEM, but as a safeguard for the batch
                    logger.error("BatchStoreKEMs: Qdrant client not available for batch upsert.")
                    qdrant_batch_success = False
                else:
                    try:
                        logger.info(f"BatchStoreKEMs: Upserting {len(final_qdrant_points_batch)} points to Qdrant.")
                        self.qdrant_client.upsert(
                            collection_name=self.config.QDRANT_COLLECTION,
                            points=final_qdrant_points_batch
                        )
                    except Exception as e_qdrant_batch:
                        logger.error(f"BatchStoreKEMs: Qdrant batch upsert error: {e_qdrant_batch}", exc_info=True)
                        qdrant_batch_success = False

            # Step 5: Handle Qdrant failures: Rollback SQLite for affected KEMs
            if not qdrant_batch_success and kems_with_embeddings_that_succeeded_sqlite:
                logger.warning("BatchStoreKEMs: Qdrant batch upsert failed. Rolling back corresponding SQLite entries for KEMs with embeddings.")
                try:
                    with self._get_sqlite_conn() as conn_cleanup:
                        cursor_cleanup = conn_cleanup.cursor()
                        for kem_id_to_rollback in kems_with_embeddings_that_succeeded_sqlite:
                            # Find original_ref for logging/failed_kem_references_list
                            original_ref_to_fail = ""
                            for q_point_data in qdrant_points_to_upsert: # Find it back from original list
                                if q_point_data['id'] == kem_id_to_rollback:
                                    original_ref_to_fail = q_point_data['original_ref']
                                    break

                            if original_ref_to_fail not in failed_kem_references_list:
                                failed_kem_references_list.append(original_ref_to_fail)

                            if kem_id_to_rollback in processed_for_qdrant_kems_map: # Should be
                                del processed_for_qdrant_kems_map[kem_id_to_rollback] # Remove from successfully processed map

                            cursor_cleanup.execute("DELETE FROM kems WHERE id = ?", (kem_id_to_rollback,))
                            logger.info(f"BatchStoreKEMs: SQLite record for KEM ID '{kem_id_to_rollback}' (ref: {original_ref_to_fail}) deleted due to Qdrant batch error.")
                        conn_cleanup.commit()
                except Exception as e_cleanup:
                    logger.critical(f"BatchStoreKEMs: CRITICAL ERROR during SQLite rollback after Qdrant batch failure: {e_cleanup}")

            # Step 6: Populate successfully_stored_kems_list
            for kem_id, kem_proto in processed_for_qdrant_kems_map.items():
                 # Check if it was part of qdrant_points_to_upsert, and if that batch failed for it
                has_embeddings_for_this_kem = any(p['id'] == kem_id for p in qdrant_points_to_upsert)

                if has_embeddings_for_this_kem and not qdrant_batch_success:
                    # This KEM had embeddings, and the Qdrant batch failed. It should have been rolled back from SQLite
                    # and added to failed_kem_references_list. It should not be in successfully_stored_kems_list.
                    # The del from processed_for_qdrant_kems_map during rollback handles this.
                    pass
                else: # Either no embeddings, or embeddings with successful Qdrant batch (or Qdrant not involved)
                    successfully_stored_kems_list.append(kem_proto)

        logger.info(f"BatchStoreKEMs: Successfully processed {len(successfully_stored_kems_list)} KEMs overall. Failed references: {len(set(failed_kem_references_list))}.")
        unique_failed_refs = list(set(failed_kem_references_list))
        response = glm_service_pb2.BatchStoreKEMsResponse(
            successfully_stored_kems=successfully_stored_kems_list,
            failed_kem_references=unique_failed_refs
        )
        if response.failed_kem_references:
            response.overall_error_message = f"Failed to fully store {len(response.failed_kem_references)} KEMs from the batch."
        elif not request.kems and not successfully_stored_kems_list:
            response.overall_error_message = "Received an empty list of KEMs to store."

        return response

def serve():
    logger.info(f"GLM Configuration: Qdrant={config.QDRANT_HOST}:{config.QDRANT_PORT} (Collection: '{config.QDRANT_COLLECTION}'), "
                f"SQLite DB={os.path.join(app_dir, config.DB_FILENAME)}, gRPC Address={config.GRPC_LISTEN_ADDRESS}, LogLevel={config.LOG_LEVEL}")
    try:
        # Test Qdrant connection early
        client_test = QdrantClient(host=config.QDRANT_HOST, port=config.QDRANT_PORT, timeout=2)
        client_test.get_collections() # A simple call to check connectivity
        logger.info(f"Qdrant is available at {config.QDRANT_HOST}:{config.QDRANT_PORT}.")
    except Exception as e:
        logger.critical(f"CRITICAL ERROR: Qdrant is unavailable at {config.QDRANT_HOST}:{config.QDRANT_PORT}. Details: {e}. Server NOT STARTED.")
        return

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    try:
        servicer_instance = GlobalLongTermMemoryServicerImpl()
    except Exception as e: # Catch any exception during servicer initialization
        logger.critical(f"CRITICAL ERROR initializing GlobalLongTermMemoryServicerImpl: {e}", exc_info=True)
        return

    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(servicer_instance, server)
    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS)
    logger.info(f"Starting GLM server on {config.GRPC_LISTEN_ADDRESS}...")
    server.start()
    logger.info(f"GLM server started and listening on {config.GRPC_LISTEN_ADDRESS}.")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("GLM server stopping via KeyboardInterrupt...")
    finally:
        server.stop(grace=5) # Allow 5 seconds for ongoing RPCs to complete
        logger.info("GLM server stopped.")

if __name__ == '__main__':
    serve()
