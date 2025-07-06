import asyncio
import uuid
import json
# import sqlite3 # No longer directly needed here due to aiosqlite_repo
import aiosqlite # For aiosqlite.Error
import logging
from typing import List, Tuple, Optional, Dict, Any

from qdrant_client import AsyncQdrantClient, models as qdrant_models # For AsyncQdrantClient
from qdrant_client.http.models import PointStruct # Keep this direct import

from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict

from generated_grpc import kem_pb2, glm_service_pb2
from ..config import GLMConfig, ExternalDataSourceConfig
from .base import BasePersistentStorageRepository, StorageError, KemNotFoundError, BackendUnavailableError, InvalidQueryError
from .sqlite_repo import SqliteKemRepository
from .qdrant_repo import QdrantKemRepository
from .external_base import BaseExternalRepository

logger = logging.getLogger(__name__)

def get_external_repository_connector(config: ExternalDataSourceConfig, glm_config: GLMConfig) -> Optional[BaseExternalRepository]:
    # ... (content remains the same)
    if config.type == "postgresql":
        from .postgres_connector import PostgresExternalRepository
        try:
            return PostgresExternalRepository(config, glm_config)
        except Exception as e_pg_init:
            logger.error(f"Failed to initialize PostgresExternalRepository for '{config.name}': {e_pg_init}", exc_info=True)
            return None
    elif config.type == "csv_dir":
        from .csv_connector import CsvDirExternalRepository
        try:
            return CsvDirExternalRepository(config, glm_config)
        except Exception as e_csv_init:
            logger.error(f"Failed to initialize CsvDirExternalRepository for '{config.name}': {e_csv_init}", exc_info=True)
            return None
    else:
        logger.warning(f"Unsupported external data source type '{config.type}' for source '{config.name}'.")
        return None

class DefaultGLMRepository(BasePersistentStorageRepository):
    def __init__(self, config: GLMConfig, app_dir: str):
        self.config = config
        self.app_dir = app_dir # Store app_dir for db_path in async_init
        self.sqlite_repo: Optional[SqliteKemRepository] = None
        self.qdrant_repo: Optional[QdrantKemRepository] = None
        self.external_repos: Dict[str, BaseExternalRepository] = {}
        self._initialized: bool = False # Flag for async initialization

        # Synchronous parts of initialization
        if hasattr(self.config, 'external_data_sources') and self.config.external_data_sources:
            logger.info(f"Found {len(self.config.external_data_sources)} external data source configurations.")
            for ext_ds_config in self.config.external_data_sources:
                try:
                    logger.info(f"Initializing external data source connector: Name='{ext_ds_config.name}', Type='{ext_ds_config.type}'")
                    connector = get_external_repository_connector(ext_ds_config, self.config)
                    if connector:
                        self.external_repos[ext_ds_config.name] = connector
                        logger.info(f"Successfully initialized and registered external connector for '{ext_ds_config.name}'.")
                    else:
                        logger.warning(f"Failed to initialize or no connector available for external data source '{ext_ds_config.name}' of type '{ext_ds_config.type}'.")
                except Exception as e_ext_init:
                    logger.error(f"Error initializing external data source connector '{ext_ds_config.name}': {e_ext_init}", exc_info=True)
        else:
            logger.info("No external data sources configured for GLM.")

        logger.info("DefaultGLMRepository synchronous initialization part complete.")

    async def ensure_initialized(self):
        """Performs asynchronous parts of initialization."""
        if self._initialized:
            return

        # Initialize SqliteKemRepository (now async)
        db_path = os.path.join(self.app_dir, self.config.DB_FILENAME)
        try:
            self.sqlite_repo = SqliteKemRepository(db_path, self.config)
            await self.sqlite_repo.ensure_schema_initialized() # Call its async schema setup
            logger.info("DefaultGLMRepository: Async SqliteKemRepository initialized and schema ensured.")
        except Exception as e_sqlite_init:
            logger.critical(f"DefaultGLMRepository: CRITICAL ERROR initializing async SqliteKemRepository: {e_sqlite_init}", exc_info=True)
            raise BackendUnavailableError(f"Async SQLite backend failed to initialize: {e_sqlite_init}") from e_sqlite_init

        # Initialize QdrantKemRepository (now async)
        if self.config.QDRANT_HOST:
            try:
                self.qdrant_repo = QdrantKemRepository(
                    collection_name=self.config.QDRANT_COLLECTION,
                    default_vector_size=self.config.DEFAULT_VECTOR_SIZE,
                    default_distance_metric=self.config.QDRANT_DEFAULT_DISTANCE_METRIC,
                    qdrant_host=self.config.QDRANT_HOST,
                    qdrant_port=self.config.QDRANT_PORT,
                    # qdrant_api_key=self.config.QDRANT_API_KEY, # If you add API key to GLMConfig
                    qdrant_client_timeout_s=self.config.QDRANT_CLIENT_TIMEOUT_S
                )
                # Test connection and ensure collection exists (now an async method)
                await self.qdrant_repo.ensure_collection()
                logger.info("DefaultGLMRepository: Async QdrantKemRepository initialized and collection ensured.")
            except Exception as e_qdrant_init:
                logger.error(f"DefaultGLMRepository: ERROR during async QdrantKemRepository initialization: {e_qdrant_init}. Qdrant features will be unavailable.", exc_info=True)
                self.qdrant_repo = None # Keep it None if init fails
        else:
            logger.warning("DefaultGLMRepository: QDRANT_HOST not configured. Qdrant features will be unavailable.")
            self.qdrant_repo = None

        self._initialized = True
        logger.info("DefaultGLMRepository async initialization complete.")

    async def close_connections(self):
        """Closes any open connections, like the AsyncQdrantClient."""
        if self.qdrant_repo and hasattr(self.qdrant_repo, 'close'):
            try:
                await self.qdrant_repo.close()
                logger.info("DefaultGLMRepository: QdrantKemRepository connections closed.")
            except Exception as e_q_close:
                logger.error(f"Error closing QdrantKemRepository: {e_q_close}", exc_info=True)
        # aiosqlite connections are typically managed per-operation with async with, so no explicit repo-level close needed for sqlite_repo

    # ... (helper methods _kem_dict_to_proto, _kem_from_db_dict_to_proto remain the same) ...
    def _kem_dict_to_proto(self, kem_data: dict) -> kem_pb2.KEM:
        kem_data_copy = kem_data.copy()
        if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
             kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')
        if 'metadata' in kem_data_copy and isinstance(kem_data_copy['metadata'], str):
            try:
                kem_data_copy['metadata'] = json.loads(kem_data_copy['metadata'])
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse metadata JSON string in _kem_dict_to_proto: {kem_data_copy['metadata']}")
                kem_data_copy['metadata'] = {}
        for ts_field_name in ['created_at', 'updated_at']:
            ts_val = kem_data_copy.get(ts_field_name)
            if isinstance(ts_val, str):
                ts_proto = Timestamp()
                try:
                    ts_str_for_parse = ts_val
                    if 'T' in ts_val and not ts_val.endswith("Z") and '+' not in ts_val and '-' not in ts_val[10:]:
                        ts_str_for_parse = ts_val + "Z"
                    ts_proto.FromJsonString(ts_str_for_parse)
                    kem_data_copy[ts_field_name] = ts_proto
                except Exception as e_ts: # More specific error might be google.protobuf.json_format.ParseError
                    logger.error(f"Error parsing timestamp string '{ts_val}' to proto for field '{ts_field_name}': {e_ts}", exc_info=True)
                    if ts_field_name in kem_data_copy: del kem_data_copy[ts_field_name]
        return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)

    def _kem_from_db_dict_to_proto(self, kem_db_dict: Dict[str, Any],
                                   embeddings_list: Optional[List[float]] = None) -> kem_pb2.KEM:
        if not kem_db_dict:
            logger.error("_kem_from_db_dict_to_proto received empty or None kem_db_dict")
            raise ValueError("Cannot convert empty DB dictionary to KEM proto.")
        kem_for_proto = {}
        kem_for_proto['id'] = kem_db_dict.get('id')
        kem_for_proto['content_type'] = kem_db_dict.get('content_type')
        kem_for_proto['content'] = kem_db_dict.get('content')
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
                    ts_parse_str = ts_iso_str
                    if 'T' in ts_iso_str and not ts_iso_str.endswith("Z") and '+' not in ts_iso_str and '-' not in ts_iso_str[10:]:
                        ts_parse_str = ts_iso_str + "Z"
                    ts_proto.FromJsonString(ts_parse_str)
                    kem_for_proto[ts_field_name_proto] = ts_proto
                except Exception as e_ts:
                    logger.error(f"Error parsing timestamp string '{ts_iso_str}' to proto for field '{ts_field_name_proto}' in KEM ID {kem_db_dict.get('id')}: {e_ts}", exc_info=True)
            elif isinstance(ts_iso_str, Timestamp):
                 kem_for_proto[ts_field_name_proto] = ts_iso_str
        if embeddings_list:
            kem_for_proto['embeddings'] = embeddings_list
        final_kem_proto = kem_pb2.KEM()
        ParseDict(kem_for_proto, final_kem_proto, ignore_unknown_fields=True)
        return final_kem_proto

    async def store_kem(self, kem: kem_pb2.KEM) -> kem_pb2.KEM:
        if not self.sqlite_repo: raise BackendUnavailableError("SQLite repository not initialized")

        kem_to_store = kem_pb2.KEM(); kem_to_store.CopyFrom(kem)
        kem_id = kem_to_store.id if kem_to_store.id else str(uuid.uuid4())
        kem_to_store.id = kem_id
        has_embeddings = bool(kem_to_store.embeddings)
        current_time_proto = Timestamp(); current_time_proto.GetCurrentTime()

        existing_created_at_str = await self.sqlite_repo.get_kem_creation_timestamp(kem_id)
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

        qdrant_op_done = False
        if self.qdrant_repo and has_embeddings:
            qdrant_payload = {"kem_id_ref": kem_id}
            if kem_to_store.metadata:
                for k, v_str in kem_to_store.metadata.items(): qdrant_payload[f"md_{k}"] = v_str
            if kem_to_store.HasField("created_at"): qdrant_payload["created_at_ts"] = kem_to_store.created_at.seconds
            if kem_to_store.HasField("updated_at"): qdrant_payload["updated_at_ts"] = kem_to_store.updated_at.seconds
            try:
                point = PointStruct(id=kem_id, vector=list(kem_to_store.embeddings), payload=qdrant_payload)
                await self.qdrant_repo.upsert_point(point) # Now await directly
                qdrant_op_done = True
            except Exception as e_qdrant: # Catch specific Qdrant client errors if possible
                logger.error(f"store_kem: Qdrant error for ID '{kem_id}': {e_qdrant}", exc_info=True)
                if "connect" in str(e_qdrant).lower() or "unavailable" in str(e_qdrant).lower(): # Basic check
                    raise BackendUnavailableError(f"Qdrant unavailable: {e_qdrant}") from e_qdrant
                else:
                    raise StorageError(f"Qdrant processing error: {e_qdrant}") from e_qdrant
        try:
            await self.sqlite_repo.store_or_replace_kem(
                kem_id=kem_to_store.id,
                content_type=kem_to_store.content_type,
                content=kem_to_store.content,
                metadata_json=json.dumps(dict(kem_to_store.metadata)),
                created_at_iso=kem_to_store.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                updated_at_iso=kem_to_store.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
            )
        except aiosqlite.Error as e_sqlite:
            logger.error(f"store_kem: SQLite error for ID '{kem_id}': {e_sqlite}", exc_info=True)
            if qdrant_op_done and self.qdrant_repo:
                logger.warning(f"store_kem: Attempting to roll back Qdrant upsert for KEM ID '{kem_id}'.")
                try:
                    await self.qdrant_repo.delete_points_by_ids([kem_id]) # Now await directly
                    logger.info(f"store_kem: Qdrant rollback successful for KEM ID '{kem_id}'.")
                except Exception as e_qdrant_rollback:
                    logger.critical(f"store_kem: CRITICAL - Failed Qdrant rollback for KEM ID '{kem_id}': {e_qdrant_rollback}", exc_info=True)
            if "unable to open" in str(e_sqlite).lower() or "database is locked" in str(e_sqlite).lower():
                 raise BackendUnavailableError(f"SQLite unavailable or busy: {e_sqlite}") from e_sqlite
            raise StorageError(f"SQLite processing error: {e_sqlite}") from e_sqlite
        except Exception as e_other_sqlite:
            logger.error(f"store_kem: Unexpected error during SQLite op for ID '{kem_id}': {e_other_sqlite}", exc_info=True)
            if qdrant_op_done and self.qdrant_repo: # Rollback Qdrant if it succeeded
                try: await self.qdrant_repo.delete_points_by_ids([kem_id])
                except Exception as e_q_rb: logger.critical(f"store_kem: CRITICAL - Qdrant rollback failed: {e_q_rb}", exc_info=True)
            raise StorageError(f"Unexpected error during SQLite operation: {e_other_sqlite}") from e_other_sqlite
        return kem_to_store

    async def retrieve_kems(
        self, query: glm_service_pb2.KEMQuery, page_size: int, page_token: Optional[str]
    ) -> Tuple[List[kem_pb2.KEM], Optional[str]]:
        if not self.sqlite_repo: raise BackendUnavailableError("SQLite repository not initialized")

        if query.data_source_name and query.data_source_name in self.external_repos:
            # ... (external repo logic remains same, assuming external_repo.retrieve_mapped_kems is async) ...
            external_repo = self.external_repos[query.data_source_name]
            logger.info(f"Dispatching retrieve_kems query to external data source: '{query.data_source_name}'")
            try:
                return await external_repo.retrieve_mapped_kems(internal_query=query, page_size=page_size, page_token=page_token)
            except Exception as e_ext_retrieve:
                logger.error(f"Error retrieving KEMs from external source '{query.data_source_name}': {e_ext_retrieve}", exc_info=True)
                if isinstance(e_ext_retrieve, (StorageError, BackendUnavailableError, InvalidQueryError)): raise
                raise StorageError(f"Failed to retrieve from external source '{query.data_source_name}': {e_ext_retrieve}") from e_ext_retrieve
        elif query.data_source_name:
            logger.warning(f"Requested external data source '{query.data_source_name}' not found or not configured.")
            raise InvalidQueryError(f"External data source '{query.data_source_name}' not available.")

        sql_conditions: List[str] = []; sql_params: List[Any] = []
        if query.ids:
            sql_conditions.append(f"id IN ({','.join('?' for _ in query.ids)})"); sql_params.extend(list(query.ids))
        if query.metadata_filters:
            for key, value in query.metadata_filters.items():
                sql_conditions.append(f"json_extract(metadata, '$.{key}') = ?"); sql_params.append(value)
        if query.HasField("created_at_start"):
            sql_conditions.append("created_at >= ?"); sql_params.append(query.created_at_start.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))
        if query.HasField("created_at_end"):
            sql_conditions.append("created_at <= ?"); sql_params.append(query.created_at_end.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))
        if query.HasField("updated_at_start"):
            sql_conditions.append("updated_at >= ?"); sql_params.append(query.updated_at_start.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))
        if query.HasField("updated_at_end"):
            sql_conditions.append("updated_at <= ?"); sql_params.append(query.updated_at_end.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))

        if self.qdrant_repo and query.embedding_query:
            logger.info("retrieve_kems: Using Qdrant for embedding query.")
            try:
                q_filters_list = []
                if query.metadata_filters:
                    for k, v in query.metadata_filters.items():
                        q_filters_list.append(qdrant_models.FieldCondition(key=f"md_{k}", match=qdrant_models.MatchValue(value=v)))
                if query.HasField("created_at_start"): q_filters_list.append(qdrant_models.FieldCondition(key="created_at_ts", range=qdrant_models.Range(gte=query.created_at_start.seconds)))
                if query.HasField("created_at_end"): q_filters_list.append(qdrant_models.FieldCondition(key="created_at_ts", range=qdrant_models.Range(lte=query.created_at_end.seconds)))

                qdrant_filter_model = qdrant_models.Filter(must=q_filters_list) if q_filters_list else None
                qdrant_offset = int(page_token) if page_token and page_token.isdigit() else 0

                scored_points = await self.qdrant_repo.search_points(
                    query_vector=list(query.embedding_query),
                    query_filter=qdrant_filter_model,
                    limit=page_size,
                    offset=qdrant_offset,
                    with_vectors=True # Ensure vectors are returned
                )
                if not scored_points: return [], None

                kem_ids_from_qdrant = [sp.id for sp in scored_points] # Ensure ID is string
                qdrant_embeddings_map = {str(sp.id): list(sp.vector) for sp in scored_points if sp.vector} # Ensure ID is string for map key

                # Fetch these KEMs from SQLite
                kems_from_db_dicts, _ = await self.sqlite_repo.retrieve_kems_from_db(
                    sql_conditions=[f"id IN ({','.join('?' for _ in kem_ids_from_qdrant)})"],
                    sql_params=kem_ids_from_qdrant,
                    page_size=len(kem_ids_from_qdrant), # Fetch all found by Qdrant
                    page_token=None, # No SQLite pagination here, Qdrant handled it
                    order_by_clause="" # Order will be determined by Qdrant, or re-sort after
                )

                # Re-order based on Qdrant results and merge embeddings
                ordered_kems_from_db_map = {str(db_dict['id']): db_dict for db_dict in kems_from_db_dicts}
                kem_protos = []
                for q_id in kem_ids_from_qdrant: # Iterate in Qdrant's order
                    db_dict = ordered_kems_from_db_map.get(str(q_id))
                    if db_dict:
                        embeddings = qdrant_embeddings_map.get(str(q_id))
                        kem_protos.append(self._kem_from_db_dict_to_proto(db_dict, embeddings_list=embeddings))

                next_page_token_str = str(qdrant_offset + len(scored_points)) if len(scored_points) == page_size else None
                return kem_protos, next_page_token_str

            except Exception as e_qdrant_search:
                logger.error(f"Error during Qdrant vector search in retrieve_kems: {e_qdrant_search}", exc_info=True)
                raise StorageError(f"Qdrant vector search failed: {e_qdrant_search}") from e_qdrant_search
        else: # SQLite only path (no embedding query or Qdrant not available)
            logger.info("retrieve_kems: Using SQLite for metadata/ID query.")
            order_by_clause = "ORDER BY updated_at DESC, id DESC"
            kems_db_dicts, next_page_token_str = await self.sqlite_repo.retrieve_kems_from_db(
                sql_conditions, sql_params, page_size, page_token, order_by_clause
            )
            kem_protos = [self._kem_from_db_dict_to_proto(db_dict) for db_dict in kems_db_dicts]
            return kem_protos, next_page_token_str

    async def update_kem(self, kem_id: str, kem_update_data: kem_pb2.KEM) -> kem_pb2.KEM:
        if not self.sqlite_repo: raise BackendUnavailableError("SQLite repository not initialized")
        # This requires fetching, merging, then calling store_kem.
        # store_kem already handles created_at/updated_at logic.
        # For a true partial update, more granular SQL would be needed.
        # For now, let's simulate by get-modify-store.

        # TODO: This is NOT a true partial update. It's a get-then-full-replace.
        # A real partial update would only modify specified fields in SQLite and Qdrant.
        # This requires significant changes to sqlite_repo and qdrant_repo.
        # For now, this placeholder will re-use store_kem, which is an upsert.
        # The `kem_update_data` KEM object should have its fields set for desired updates.
        # We need to merge it with the existing KEM.

        # Fetch existing KEM data (content and metadata) from SQLite
        existing_kem_dict = await self.sqlite_repo.get_kem_by_id(kem_id)
        if not existing_kem_dict:
            raise KemNotFoundError(f"KEM ID '{kem_id}' not found for update.")

        # Convert DB dict to KEM proto to easily merge with update_data
        # If existing KEM had embeddings, they are not in SQLite dict.
        # If update_data has new embeddings, they will be used. If it clears them, they will be cleared.
        # If update_data doesn't specify embeddings, existing ones (if any in Qdrant) might remain
        # unless store_kem explicitly clears them if not provided.

        # Create a KEM proto from the existing data
        # For simplicity, we'll assume for now that an update might replace all fields
        # provided in kem_update_data, and use store_kem for its upsert logic.
        # This means kem_update_data should be a "complete" KEM for the fields being changed.
        # A true PATCH is more complex.

        # Let's assume kem_update_data contains the full new state for fields it wants to change,
        # and store_kem will handle the replacement. We need to ensure its ID is set.
        kem_to_store = kem_pb2.KEM()
        kem_to_store.CopyFrom(kem_update_data) # Start with the update data
        kem_to_store.id = kem_id # Ensure ID is the one being updated

        # If content/content_type/metadata are not in kem_update_data, they won't be changed by store_kem
        # if store_kem was a true patch. But it's an INSERT OR REPLACE.
        # So, we must provide the *full* KEM state to store_kem.

        # This is a simplified approach: make kem_update_data the new KEM, just ensure ID.
        # A proper update would fetch, merge, then store.
        # For now, this will behave like an overwrite if `store_kem` is used.
        # The current `store_kem` is an upsert, so it will replace.

        # To make it a bit more like an update:
        # If a field is not set in kem_data_update, it should ideally retain its old value.
        # ParseDict doesn't merge like proto MergeFrom.
        # We need to construct the full KEM to be stored.

        # This is still not a true partial update, but store_kem will ensure timestamps are handled.
        if not kem_update_data.HasField("content_type") and existing_kem_dict.get("content_type"):
            kem_to_store.content_type = existing_kem_dict["content_type"]
        if not kem_update_data.HasField("content") and existing_kem_dict.get("content") is not None: # content can be empty bytes
            kem_to_store.content = existing_kem_dict["content"]

        # Merge metadata
        if kem_update_data.metadata:
            # If update has metadata, it completely replaces old metadata (Protobuf map behavior)
            # If we want a patch-like merge for metadata, it needs custom logic here.
            # For now, direct assignment is fine as per KEM.metadata field.
            pass # It's already in kem_to_store from CopyFrom
        elif existing_kem_dict.get("metadata"):
             # If update has no metadata, but existing did, keep existing.
            try:
                existing_meta_dict = json.loads(existing_kem_dict["metadata"])
                for k, v in existing_meta_dict.items():
                    kem_to_store.metadata[k] = str(v) # Ensure string values
            except json.JSONDecodeError:
                logger.warning(f"UpdateKEM: Could not parse existing metadata for {kem_id}")


        # Embeddings: if kem_update_data has embeddings, they are used.
        # If kem_update_data.embeddings is empty, it implies clearing them.
        # If kem_update_data does not specify embeddings field at all, store_kem will use what's there.
        # This depends on how `HasField` works for `repeated` fields if they are empty.
        # `bool(kem_to_store.embeddings)` in `store_kem` checks if list is non-empty.

        logger.info(f"UpdateKEM: Proceeding to store updated KEM for ID '{kem_id}'.")
        return await self.store_kem(kem_to_store) # Re-use store_kem for upsert logic


    async def delete_kem(self, kem_id: str) -> bool:
        if not self.sqlite_repo: raise BackendUnavailableError("SQLite repository not initialized")

        # 1. Delete from Qdrant (if it exists and has embeddings)
        # We don't know for sure if it had embeddings without a GET, but try delete anyway.
        if self.qdrant_repo:
            try:
                await self.qdrant_repo.delete_points_by_ids([kem_id])
                logger.info(f"DeleteKEM: Qdrant delete attempted for KEM ID '{kem_id}'.")
            except Exception as e_q_del:
                # Log error but proceed to delete from SQLite, as Qdrant might not have the point.
                logger.warning(f"DeleteKEM: Error during Qdrant delete for KEM ID '{kem_id}': {e_q_del}. Proceeding with SQLite delete.", exc_info=True)

        # 2. Delete from SQLite
        try:
            deleted_from_sqlite = await self.sqlite_repo.delete_kem_from_db(kem_id)
            if deleted_from_sqlite:
                logger.info(f"DeleteKEM: Successfully deleted KEM ID '{kem_id}' from SQLite.")
            else:
                logger.info(f"DeleteKEM: KEM ID '{kem_id}' not found in SQLite (or already deleted).")
            return deleted_from_sqlite # True if deleted from SQLite, even if Qdrant part had issues or no point
        except Exception as e_sql_del:
            logger.error(f"DeleteKEM: Error deleting KEM ID '{kem_id}' from SQLite: {e_sql_del}", exc_info=True)
            raise StorageError(f"SQLite delete error for KEM ID '{kem_id}'") from e_sql_del


    async def batch_store_kems(
        self, kems: List[kem_pb2.KEM]
    ) -> Tuple[List[kem_pb2.KEM], List[str]]:
        if not self.sqlite_repo: raise BackendUnavailableError("SQLite repository not initialized")

        successful_kems: List[kem_pb2.KEM] = []
        failed_kem_references: List[str] = []

        # For simplicity, process one by one. True batch with transactionality is more complex.
        for kem_orig in kems:
            kem_id_ref = kem_orig.id or "new_kem_in_batch"
            try:
                stored_kem = await self.store_kem(kem_orig) # Reuses single store_kem logic
                successful_kems.append(stored_kem)
            except Exception as e_single_store:
                logger.error(f"BatchStoreKEMs: Failed to store KEM (ref: {kem_id_ref}): {e_single_store}", exc_info=False)
                failed_kem_references.append(kem_id_ref)

        return successful_kems, failed_kem_references

    async def check_health(self) -> Tuple[bool, str]:
        if not self.sqlite_repo : # Qdrant repo can be None
            return False, "Repositories not initialized."

        all_ok = True
        msg_parts = []

        # 1. Check SQLite health
        try:
            await self.sqlite_repo.internal_sqlite_health_check()
            msg_parts.append("NativeSQLite:OK")
        except Exception as e_sqlite_check:
            logger.warning(f"DefaultGLMRepository Health Check: Native SQLite check failed: {e_sqlite_check}", exc_info=True)
            all_ok = False
            msg_parts.append(f"NativeSQLite:FAIL ({type(e_sqlite_check).__name__}: {e_sqlite_check})")

        # 2. Check Qdrant health (if configured and initialized)
        if self.qdrant_repo: # If Qdrant was configured and successfully initialized
            try:
                await self.qdrant_repo.get_collection_info() # Uses async client method
                msg_parts.append("NativeQdrant:OK")
            except Exception as e_qdrant_check:
                logger.warning(f"DefaultGLMRepository Health Check: Native Qdrant check failed: {e_qdrant_check}", exc_info=True)
                all_ok = False
                msg_parts.append(f"NativeQdrant:FAIL ({type(e_qdrant_check).__name__}: {e_qdrant_check})")
        elif self.config.QDRANT_HOST: # Configured but self.qdrant_repo is None (init failed)
             logger.warning("DefaultGLMRepository Health Check: Native Qdrant configured but client not available (init failed?).")
             all_ok = False
             msg_parts.append("NativeQdrant:FAIL (client not initialized or init failed)")
        else: # Not configured
            msg_parts.append("NativeQdrant:N/A (not configured)")

        # 3. Check health of external repositories
        if self.external_repos:
            for name, repo in self.external_repos.items():
                try:
                    ext_healthy, ext_msg = await repo.check_health() # Assuming these are async
                    if ext_healthy:
                        msg_parts.append(f"ExternalRepo({name}):OK ({ext_msg})")
                    else:
                        all_ok = False
                        msg_parts.append(f"ExternalRepo({name}):FAIL ({ext_msg})")
                except Exception as e_ext_health:
                    logger.error(f"Error during health check for external repo '{name}': {e_ext_health}", exc_info=True)
                    all_ok = False
                    msg_parts.append(f"ExternalRepo({name}):FAIL (Exception)")
        else:
            msg_parts.append("ExternalRepos:N/A (none configured)")

        return all_ok, "; ".join(msg_parts)

import os # For db_path in __init__
from qdrant_client import AsyncQdrantClient # Ensure AsyncQdrantClient is imported
import aiosqlite # For aiosqlite.Error type hint in store_kem
from qdrant_client.http.models import ScoredPoint # For type hint in retrieve_kems
