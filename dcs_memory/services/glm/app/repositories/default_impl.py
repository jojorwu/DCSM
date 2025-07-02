import asyncio
import uuid
import json
import sqlite3 # For specific exception types
import logging
from typing import List, Tuple, Optional, Dict, Any

from qdrant_client import QdrantClient, models as qdrant_models # Alias to avoid conflict
from qdrant_client.http.models import PointStruct # Keep this direct import

from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict

from generated_grpc import kem_pb2, glm_service_pb2
from ..config import GLMConfig # Relative import for config within the same app
from .base import BasePersistentStorageRepository, StorageError, KemNotFoundError, BackendUnavailableError, InvalidQueryError
from .sqlite_repo import SqliteKemRepository # Previous sqlite_repo.py
from .qdrant_repo import QdrantKemRepository # Previous qdrant_repo.py

logger = logging.getLogger(__name__)

class DefaultGLMRepository(BasePersistentStorageRepository):
    """
    Default implementation of BasePersistentStorageRepository.
    It uses SqliteKemRepository for metadata/content and QdrantKemRepository for embeddings,
    orchestrating them to provide a unified storage interface.
    """

    def __init__(self, config: GLMConfig, app_dir: str):
        self.config = config

        db_path = os.path.join(app_dir, self.config.DB_FILENAME)
        try:
            self.sqlite_repo = SqliteKemRepository(db_path, self.config)
            logger.info("DefaultGLMRepository: SqliteKemRepository initialized successfully.")
        except Exception as e_sqlite_init:
            logger.critical(f"DefaultGLMRepository: CRITICAL ERROR initializing SqliteKemRepository: {e_sqlite_init}", exc_info=True)
            raise BackendUnavailableError(f"SQLite backend failed to initialize: {e_sqlite_init}") from e_sqlite_init

        self.qdrant_repo: Optional[QdrantKemRepository] = None
        if self.config.QDRANT_HOST:
            try:
                # TODO: QdrantClient initialization might need to handle potential TLS settings for connecting to Qdrant itself.
                # This is separate from GLM's gRPC server TLS.
                # For now, assuming QDRANT_HOST includes http/https scheme if needed by client.
                qdrant_client = QdrantClient(
                    host=self.config.QDRANT_HOST,
                    port=self.config.QDRANT_PORT,
                    timeout=self.config.QDRANT_CLIENT_TIMEOUT_S
                )
                # A light connection test - this might throw if Qdrant is down
                qdrant_client.get_collections()
                logger.info(f"DefaultGLMRepository: Qdrant client successfully connected to {self.config.QDRANT_HOST}:{self.config.QDRANT_PORT}.")

                self.qdrant_repo = QdrantKemRepository(
                    qdrant_client=qdrant_client,
                    collection_name=self.config.QDRANT_COLLECTION,
                    default_vector_size=self.config.DEFAULT_VECTOR_SIZE,
                    default_distance_metric=self.config.QDRANT_DEFAULT_DISTANCE_METRIC
                )
                self.qdrant_repo.ensure_collection()
                logger.info("DefaultGLMRepository: QdrantKemRepository initialized and collection ensured.")
            except Exception as e_qdrant_init:
                logger.error(f"DefaultGLMRepository: ERROR during Qdrant client/repository initialization: {e_qdrant_init}. Qdrant features will be unavailable.", exc_info=True)
                # We don't raise BackendUnavailableError here if Qdrant is optional for some operations.
                # Methods using qdrant_repo will need to check if it's None.
                self.qdrant_repo = None
        else:
            logger.warning("DefaultGLMRepository: QDRANT_HOST not configured. Qdrant features will be unavailable.")
            self.qdrant_repo = None

        logger.info("DefaultGLMRepository initialized.")

    # Helper methods (adapted from GlobalLongTermMemoryServicerImpl)
    def _kem_dict_to_proto(self, kem_data: dict) -> kem_pb2.KEM:
        # This helper might be better placed in a common utils or within the KEM proto generation itself if complex.
        # For now, keeping it here as it's specific to how this repo reconstructs protos.
        kem_data_copy = kem_data.copy()
        if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
             kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')

        # Handle metadata if it's a JSON string (from SQLite)
        if 'metadata' in kem_data_copy and isinstance(kem_data_copy['metadata'], str):
            try:
                kem_data_copy['metadata'] = json.loads(kem_data_copy['metadata'])
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse metadata JSON string in _kem_dict_to_proto: {kem_data_copy['metadata']}")
                kem_data_copy['metadata'] = {} # Default to empty dict

        # Handle timestamps if they are ISO strings (from SQLite)
        for ts_field_name in ['created_at', 'updated_at']:
            ts_val = kem_data_copy.get(ts_field_name)
            if isinstance(ts_val, str):
                ts_proto = Timestamp()
                try:
                    # Ensure 'Z' for UTC if naive, for FromJsonString
                    ts_str_for_parse = ts_val
                    if 'T' in ts_val and not ts_val.endswith("Z") and '+' not in ts_val and '-' not in ts_val[10:]:
                        ts_str_for_parse = ts_val + "Z"
                    ts_proto.FromJsonString(ts_str_for_parse)
                    kem_data_copy[ts_field_name] = ts_proto
                except Exception as e_ts:
                    logger.error(f"Error parsing timestamp string '{ts_val}' to proto for field '{ts_field_name}': {e}")
                    # Decide on fallback: remove or set to current time? For now, remove if unparseable.
                    if ts_field_name in kem_data_copy: del kem_data_copy[ts_field_name]

        return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)

    def _kem_from_db_dict_to_proto(self, kem_db_dict: Dict[str, Any],
                                   embeddings_list: Optional[List[float]] = None) -> kem_pb2.KEM:
        """
        Converts a dictionary (typically from SQLite row) and an optional embeddings list
        into a KEM protobuf object.
        """
        if not kem_db_dict:
            # This case should ideally be handled by callers, but as a safeguard:
            logger.error("_kem_from_db_dict_to_proto received empty or None kem_db_dict")
            raise ValueError("Cannot convert empty DB dictionary to KEM proto.")

        kem_for_proto = {}
        kem_for_proto['id'] = kem_db_dict.get('id')
        kem_for_proto['content_type'] = kem_db_dict.get('content_type')
        kem_for_proto['content'] = kem_db_dict.get('content') # Assumed bytes from DB

        metadata_json_str = kem_db_dict.get('metadata', '{}')
        try:
            kem_for_proto['metadata'] = json.loads(metadata_json_str)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse metadata JSON: {metadata_json_str} for KEM ID {kem_db_dict.get('id')}. Using empty metadata.")
            kem_for_proto['metadata'] = {}

        for ts_field_name_db, ts_field_name_proto in [('created_at', 'created_at'), ('updated_at', 'updated_at')]:
            ts_iso_str = kem_db_dict.get(ts_field_name_db)
            if ts_iso_str and isinstance(ts_iso_str, str):
                ts_proto = Timestamp()
                try:
                    # Ensure 'Z' for UTC if naive, for FromJsonString
                    ts_parse_str = ts_iso_str
                    if 'T' in ts_iso_str and not ts_iso_str.endswith("Z") and '+' not in ts_iso_str and '-' not in ts_iso_str[10:]:
                        ts_parse_str = ts_iso_str + "Z"
                    ts_proto.FromJsonString(ts_parse_str)
                    kem_for_proto[ts_field_name_proto] = ts_proto
                except Exception as e_ts:
                    logger.error(f"Error parsing timestamp string '{ts_iso_str}' to proto for field '{ts_field_name_proto}' in KEM ID {kem_db_dict.get('id')}: {e}")
                    # Fallback or error handling if necessary, e.g., remove the field or use a default
            elif isinstance(ts_iso_str, Timestamp): # If it's already a Timestamp proto (e.g. from another KEM)
                 kem_for_proto[ts_field_name_proto] = ts_iso_str


        if embeddings_list:
            kem_for_proto['embeddings'] = embeddings_list

        # Use ParseDict for robustness, it handles missing fields gracefully if KEM proto changes
        final_kem_proto = kem_pb2.KEM()
        ParseDict(kem_for_proto, final_kem_proto, ignore_unknown_fields=True)
        return final_kem_proto

    # --- Implementation of BasePersistentStorageRepository ABC methods ---

    async def store_kem(self, kem: kem_pb2.KEM) -> kem_pb2.KEM:
        # This method now encapsulates the logic previously in GLM Servicer's StoreKEM
        # including ID generation, timestamp management, and dual writes to SQLite & Qdrant.

        kem_to_store = kem_pb2.KEM() # Create a copy to modify
        kem_to_store.CopyFrom(kem)

        kem_id = kem_to_store.id if kem_to_store.id else str(uuid.uuid4())
        kem_to_store.id = kem_id
        has_embeddings = bool(kem_to_store.embeddings)

        current_time_proto = Timestamp(); current_time_proto.GetCurrentTime()

        # Preserve created_at if KEM exists, otherwise set to now or use provided
        existing_created_at_str = self.sqlite_repo.get_kem_creation_timestamp(kem_id)
        final_created_at_proto = Timestamp()
        if existing_created_at_str:
            try:
                parse_str = existing_created_at_str
                if 'T' in parse_str and not parse_str.endswith("Z") and '+' not in parse_str and '-' not in parse_str[10:]:
                    parse_str += "Z"
                final_created_at_proto.FromJsonString(parse_str)
            except Exception:
                logger.error(f"store_kem: Error parsing DB created_at '{existing_created_at_str}' for KEM_ID '{kem_id}'. Using current time.", exc_info=True)
                final_created_at_proto.CopyFrom(current_time_proto)
        elif kem_to_store.HasField("created_at") and kem_to_store.created_at.seconds > 0:
            final_created_at_proto.CopyFrom(kem_to_store.created_at)
        else:
            final_created_at_proto.CopyFrom(current_time_proto)

        kem_to_store.created_at.CopyFrom(final_created_at_proto)
        kem_to_store.updated_at.CopyFrom(current_time_proto)

        # 1. Store/Update Qdrant point if embeddings exist
        qdrant_op_done = False
        if self.qdrant_repo and has_embeddings:
            qdrant_payload = {"kem_id_ref": kem_id}
            if kem_to_store.metadata:
                for k, v_str in kem_to_store.metadata.items(): qdrant_payload[f"md_{k}"] = v_str # metadata is map<string,string>
            if kem_to_store.HasField("created_at"): qdrant_payload["created_at_ts"] = kem_to_store.created_at.seconds
            if kem_to_store.HasField("updated_at"): qdrant_payload["updated_at_ts"] = kem_to_store.updated_at.seconds

            try:
                point = PointStruct(id=kem_id, vector=list(kem_to_store.embeddings), payload=qdrant_payload)
                self.qdrant_repo.upsert_point(point)
                qdrant_op_done = True
            except Exception as e_qdrant:
                logger.error(f"store_kem: Qdrant error for ID '{kem_id}': {e_qdrant}", exc_info=True)
                if "connect" in str(e_qdrant).lower() or "unavailable" in str(e_qdrant).lower():
                    raise BackendUnavailableError(f"Qdrant unavailable: {e_qdrant}") from e_qdrant
                else:
                    raise StorageError(f"Qdrant processing error: {e_qdrant}") from e_qdrant

        # 2. Store/Update SQLite record
        try:
            self.sqlite_repo.store_or_replace_kem(
                kem_id=kem_to_store.id,
                content_type=kem_to_store.content_type,
                content=kem_to_store.content,
                metadata_json=json.dumps(dict(kem_to_store.metadata)), # Protobuf map to dict then to JSON string
                created_at_iso=kem_to_store.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                updated_at_iso=kem_to_store.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
            )
        except sqlite3.Error as e_sqlite: # Catch specific sqlite3.Error
            logger.error(f"store_kem: SQLite error for ID '{kem_id}': {e_sqlite}", exc_info=True)
            if qdrant_op_done and self.qdrant_repo: # If Qdrant op was done, try to roll it back
                logger.warning(f"store_kem: Attempting to roll back Qdrant upsert for KEM ID '{kem_id}' due to SQLite error.")
                try:
                    self.qdrant_repo.delete_points_by_ids([kem_id])
                    logger.info(f"store_kem: Qdrant rollback successful for KEM ID '{kem_id}'.")
                except Exception as e_qdrant_rollback:
                    logger.critical(f"store_kem: CRITICAL - Failed Qdrant rollback for KEM ID '{kem_id}': {e_qdrant_rollback}", exc_info=True)
                    # Original error is more relevant to propagate if rollback also fails
            if "unable to open" in str(e_sqlite) or "database is locked" in str(e_sqlite):
                 raise BackendUnavailableError(f"SQLite unavailable or busy: {e_sqlite}") from e_sqlite
            raise StorageError(f"SQLite processing error: {e_sqlite}") from e_sqlite
        except Exception as e_other_sqlite: # Catch other potential errors
            logger.error(f"store_kem: Unexpected error during SQLite operation for ID '{kem_id}': {e_other_sqlite}", exc_info=True)
            # Similar rollback logic for Qdrant if applicable
            if qdrant_op_done and self.qdrant_repo:
                logger.warning(f"store_kem: Attempting to roll back Qdrant upsert for KEM ID '{kem_id}' due to unexpected SQLite error.")
                try:
                    self.qdrant_repo.delete_points_by_ids([kem_id])
                except Exception as e_q_rb_unexpected:
                    logger.critical(f"store_kem: CRITICAL - Failed Qdrant rollback (unexpected SQLite err) for KEM ID '{kem_id}': {e_q_rb_unexpected}", exc_info=True)
            raise StorageError(f"Unexpected error during SQLite operation: {e_other_sqlite}") from e_other_sqlite

        return kem_to_store

    # ... other methods will be implemented progressively ...
    async def retrieve_kems(
        self,
        query: glm_service_pb2.KEMQuery,
        page_size: int,
        page_token: Optional[str]
    ) -> Tuple[List[kem_pb2.KEM], Optional[str]]:
        raise NotImplementedError()

    async def update_kem(
        self,
        kem_id: str,
        kem_update_data: kem_pb2.KEM,
    ) -> kem_pb2.KEM:
        raise NotImplementedError()

    async def delete_kem(self, kem_id: str) -> bool:
        raise NotImplementedError()

    async def batch_store_kems(
        self,
        kems: List[kem_pb2.KEM]
    ) -> Tuple[List[kem_pb2.KEM], List[str]]:
        raise NotImplementedError()

    async def check_health(self) -> Tuple[bool, str]:
        # This needs to run the checks for SQLite and Qdrant (if configured)
        # For now, a placeholder. The actual logic from main.py's _check_sqlite_health/_check_qdrant_health
        # will be adapted here.
        # Since this is an async method, the checks should ideally be async too if possible,
        # or run in an executor. For now, let's assume they can be called.

        # Simulating the logic that was in main.py's check_overall_health
        # Note: sqlite_repo methods are sync, qdrant_repo methods are sync.
        # This needs to be made truly async if the underlying repo calls become async.
        # For now, to match the ABC, we wrap sync calls if needed.

        # This is a simplified version for now. SqliteKemRepository methods are not async.
        # Qdrant client calls are sync.
        # To make this truly async, SqliteKemRepository and QdrantKemRepository would need async methods.
        # For this step, we'll assume sync execution within this async method for simplicity of refactoring.

        # Proper async implementation would involve:
        # loop = asyncio.get_event_loop()
        # sqlite_healthy = await loop.run_in_executor(None, self.sqlite_repo._check_sqlite_health_internal_logic)
        # qdrant_healthy = await loop.run_in_executor(None, self.qdrant_repo._check_qdrant_health_internal_logic)

        # For now, direct call for placeholder:
        sqlite_ok, qdrant_ok = True, True
        msg_parts = []

        try:
            conn = self.sqlite_repo._get_sqlite_conn()
            cursor = conn.cursor()
            sqlite_query = getattr(self.config, "HEALTH_CHECK_SQLITE_QUERY", "SELECT 1")
            cursor.execute(sqlite_query)
            cursor.fetchone()
            msg_parts.append("SQLite OK")
        except Exception as e_sqlite_check:
            logger.warning(f"DefaultGLMRepository Health Check: SQLite check failed: {e_sqlite_check}")
            sqlite_ok = False
            msg_parts.append(f"SQLite FAILED: {e_sqlite_check}")

        if self.qdrant_repo and self.qdrant_repo.client:
            try:
                self.qdrant_repo.client.get_collections() # Simple connectivity test
                msg_parts.append("Qdrant OK")
            except Exception as e_qdrant_check:
                logger.warning(f"DefaultGLMRepository Health Check: Qdrant check failed: {e_qdrant_check}")
                qdrant_ok = False
                msg_parts.append(f"Qdrant FAILED: {e_qdrant_check}")
        elif not self.config.QDRANT_HOST:
            msg_parts.append("Qdrant N/A (not configured)")
            qdrant_ok = True # Not a failure if not configured
        else: # Configured but client not initialized
            logger.warning("DefaultGLMRepository Health Check: Qdrant configured but client not available.")
            qdrant_ok = False
            msg_parts.append("Qdrant FAILED (client not initialized)")

        is_healthy = sqlite_ok and qdrant_ok
        return is_healthy, "; ".join(msg_parts)

# Need to import os for db_path
import os
```

**Note:** The `async def` methods in `DefaultGLMRepository` are calling synchronous repository methods. This is a temporary state during refactoring. A full async implementation would require the underlying `SqliteKemRepository` and `QdrantKemRepository` to also have async methods and use async database drivers (e.g., `aioqdrant`, `aiosqlite`). For now, I'm using them as placeholders for the interface. The `asyncio.run()` in the servicer for calling these is also a temporary bridge.

I've started with `__init__`, helper methods, `store_kem`, and a placeholder for `check_health`. I will now continue implementing the other abstract methods.
