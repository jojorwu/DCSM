import asyncio
import uuid
import json
import sqlite3 # For specific exception types
import logging
from typing import List, Tuple, Optional, Dict, Any

from qdrant_client import QdrantClient, models as qdrant_models # Alias to avoid conflict
from qdrant_client.http.models import PointStruct # Keep this direct import

import base64
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict

from dcs_memory.services.glm.generated_grpc import kem_pb2, glm_service_pb2
from dcs_memory.services.glm.app.config import GLMConfig, ExternalDataSourceConfig
from dcs_memory.services.glm.app.repositories.base import BasePersistentStorageRepository, StorageError, KemNotFoundError, BackendUnavailableError, InvalidQueryError
from dcs_memory.services.glm.app.sqlite_repo import SqliteKemRepository, QdrantKemRepository
from dcs_memory.services.glm.app.repositories.external_base import BaseExternalRepository

logger = logging.getLogger(__name__)

# Placeholder for dynamic connector loading/factory
# In a real system, this might use entry points or a more robust plugin mechanism.
def get_external_repository_connector(config: ExternalDataSourceConfig, glm_config: GLMConfig) -> Optional[BaseExternalRepository]:
    if config.type == "postgresql":
        from .postgres_connector import PostgresExternalRepository # Now this file exists
        try:
            return PostgresExternalRepository(config, glm_config)
        except Exception as e_pg_init:
            logger.error(f"Failed to initialize PostgresExternalRepository for '{config.name}': {e_pg_init}", exc_info=True)
            return None
    elif config.type == "csv_dir":
        from .csv_connector import CsvDirExternalRepository # Import new connector
        try:
            return CsvDirExternalRepository(config, glm_config)
        except Exception as e_csv_init:
            logger.error(f"Failed to initialize CsvDirExternalRepository for '{config.name}': {e_csv_init}", exc_info=True)
            return None
    # Add other types here:
    # elif config.type == "mongodb":
    #     from .mongodb_connector import MongoDBExternalRepository
    #     return MongoDBExternalRepository(config, glm_config)
    else:
        logger.warning(f"Unsupported external data source type '{config.type}' for source '{config.name}'.")
        return None


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

        # Initialize external repositories
        self.external_repos: Dict[str, BaseExternalRepository] = {}
        if hasattr(self.config, 'external_data_sources') and self.config.external_data_sources:
            logger.info(f"Found {len(self.config.external_data_sources)} external data source configurations.")
            for ext_ds_config in self.config.external_data_sources:
                try:
                    logger.info(f"Initializing external data source: Name='{ext_ds_config.name}', Type='{ext_ds_config.type}'")
                    connector = get_external_repository_connector(ext_ds_config, self.config)
                    if connector:
                        self.external_repos[ext_ds_config.name] = connector
                        logger.info(f"Successfully initialized and registered external connector for '{ext_ds_config.name}'.")
                    else:
                        logger.warning(f"Failed to initialize or no connector available for external data source '{ext_ds_config.name}' of type '{ext_ds_config.type}'.")
                except Exception as e_ext_init:
                    logger.error(f"Error initializing external data source '{ext_ds_config.name}': {e_ext_init}", exc_info=True)
        else:
            logger.info("No external data sources configured for GLM.")

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
        into a KEM protobuf object. This function prepares a JSON-like dictionary
        that is compatible with protobuf's ParseDict utility.
        """
        if not kem_db_dict:
            logger.error("_kem_from_db_dict_to_proto received empty or None kem_db_dict")
            raise ValueError("Cannot convert empty DB dictionary to KEM proto.")

        kem_for_proto = {
            'id': kem_db_dict.get('id'),
            'content_type': kem_db_dict.get('content_type')
        }

        # content (bytes) must be base64 encoded for ParseDict
        content_bytes = kem_db_dict.get('content')
        if content_bytes is not None:
            kem_for_proto['content'] = base64.b64encode(content_bytes).decode('ascii')

        # metadata is a JSON string from DB, needs to be parsed into a dict for ParseDict
        metadata_json_str = kem_db_dict.get('metadata', '{}')
        try:
            kem_for_proto['metadata'] = json.loads(metadata_json_str)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse metadata JSON: {metadata_json_str} for KEM ID {kem_db_dict.get('id')}. Using empty metadata.")
            kem_for_proto['metadata'] = {}

        # Timestamps are ISO strings from DB. ParseDict can handle them directly.
        for ts_field_name in ['created_at', 'updated_at']:
            ts_iso_str = kem_db_dict.get(ts_field_name)
            if ts_iso_str and isinstance(ts_iso_str, str):
                # Ensure the timestamp string is in the format ParseDict expects (RFC 3339)
                ts_parse_str = ts_iso_str
                if 'T' in ts_iso_str and not ts_iso_str.endswith("Z") and '+' not in ts_iso_str and '-' not in ts_iso_str[10:]:
                    ts_parse_str = ts_iso_str + "Z"
                kem_for_proto[ts_field_name] = ts_parse_str

        if embeddings_list:
            kem_for_proto['embeddings'] = embeddings_list

        # Use ParseDict for robust conversion from a dictionary to a protobuf message
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
                created_at_iso=kem_to_store.created_at.ToDatetime().isoformat(timespec='microseconds').replace('+00:00', ''),
                updated_at_iso=kem_to_store.updated_at.ToDatetime().isoformat(timespec='microseconds').replace('+00:00', '')
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
        # Dispatch to external repository if data_source_name is specified and valid
        if query.data_source_name and query.data_source_name in self.external_repos:
            external_repo = self.external_repos[query.data_source_name]
            logger.info(f"Dispatching retrieve_kems query to external data source: '{query.data_source_name}'")
            try:
                # External repo's retrieve_mapped_kems needs to be an async method.
                # It is responsible for its own internal query translation, data fetching, mapping to KEMs, and pagination.
                return await external_repo.retrieve_mapped_kems(
                    internal_query=query, # Pass the full query
                    page_size=page_size,
                    page_token=page_token
                )
            except Exception as e_ext_retrieve:
                logger.error(f"Error retrieving KEMs from external source '{query.data_source_name}': {e_ext_retrieve}", exc_info=True)
                # Decide on error propagation: re-raise a specific error or return empty with logging?
                # For now, re-raise as a StorageError or let specific exception propagate if it's informative.
                if isinstance(e_ext_retrieve, (StorageError, BackendUnavailableError, InvalidQueryError)):
                    raise
                raise StorageError(f"Failed to retrieve from external source '{query.data_source_name}': {e_ext_retrieve}") from e_ext_retrieve
        elif query.data_source_name: # Specified but not found/configured
            logger.warning(f"Requested external data source '{query.data_source_name}' not found or not configured.")
            raise InvalidQueryError(f"External data source '{query.data_source_name}' not available.")

        # If not dispatched to external, proceed with native SQLite/Qdrant logic
        # This method will primarily handle SQLite based filtering (IDs, metadata, timestamps).
        # Vector search (embedding_query) or text_query would typically involve Qdrant
        # and then potentially retrieving full KEMs from SQLite.
        # For this implementation, we'll focus on filters applicable to SQLite.

        sql_conditions: List[str] = []
        sql_params: List[Any] = []

        # ID filters are handled below, in conjunction with search results.

        # 2. Handle metadata filters
        # This uses json_extract, which benefits from JSON indexes if available (SQLite >= 3.38.0)
        if query.metadata_filters:
            for key, value in query.metadata_filters.items():
                # Basic sanitization for key to prevent issues if keys could be non-alphanumeric,
                # though json_extract path syntax is quite flexible.
                # For now, assume keys are valid JSON path components (e.g. no dots if not intended for nesting).
                # If keys could contain special characters like '.', they might need specific handling
                # in the path string e.g. '$."key.with.dots"'. For simple keys, '$.key' is fine.
                # Assuming simple keys for now.
                sanitized_key = key # Add more robust sanitization if keys can be complex
                sql_conditions.append(f"json_extract(metadata, '$.{sanitized_key}') = ?")
                sql_params.append(value)

        # 3. Handle timestamp filters
        if query.HasField("created_at_start"):
            sql_conditions.append("created_at >= ?")
            sql_params.append(query.created_at_start.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))
        if query.HasField("created_at_end"):
            sql_conditions.append("created_at <= ?")
            sql_params.append(query.created_at_end.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))
        if query.HasField("updated_at_start"):
            sql_conditions.append("updated_at >= ?")
            sql_params.append(query.updated_at_start.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))
        if query.HasField("updated_at_end"):
            sql_conditions.append("updated_at <= ?")
            sql_params.append(query.updated_at_end.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))

        # --- Search Logic ---
        # Determine the primary set of IDs to filter on.
        # Text search (FTS) is performed first. If user also provided a list of IDs,
        # the result is the intersection of the FTS results and the provided ID list.
        kem_id_subset = None
        if query.text_query:
            logger.info(f"Performing FTS search for: '{query.text_query}'")
            kem_id_subset = await asyncio.to_thread(
                self.sqlite_repo.search_kems_by_text, query.text_query
            )
            if not kem_id_subset:
                return [], None # Text search returned no results, so the final result is empty.

            if query.ids:
                # Intersect with the user-provided list of IDs if present
                kem_id_subset = list(set(kem_id_subset) & set(query.ids))
                if not kem_id_subset:
                    return [], None # Intersection is empty, so no results.

        elif query.ids:
            # No text search, but a list of IDs was provided.
            kem_id_subset = list(query.ids)

        # If we have a definitive subset of IDs, add it as a primary filter condition.
        if kem_id_subset is not None:
            placeholders = ",".join("?" for _ in kem_id_subset)
            sql_conditions.append(f"id IN ({placeholders})")
            sql_params.extend(kem_id_subset)


        # --- Vector Search (if requested) ---
        # If embedding_query is present, it takes precedence and talks to Qdrant.
        # It will use other filters (metadata, etc.) to pre-filter in Qdrant if possible.
        # Its results (a list of IDs) will overwrite any previous ID-based filters.
        if query.embedding_query:
            if not self.qdrant_repo:
                logger.warning("Received embedding_query, but Qdrant repository is not available. Cannot perform vector search.")
            else:
                logger.info("Performing vector search via Qdrant. This may override any FTS/ID-list filters.")
                try:
                    # Construct Qdrant filter from KEMQuery (simplified example)
                    q_filters = []
                    if query.metadata_filters:
                        for k, v in query.metadata_filters.items():
                            q_filters.append(qdrant_models.FieldCondition(key=f"md_{k}", match=qdrant_models.MatchValue(value=v)))
                    # Timestamp filters for Qdrant (assuming timestamps are stored as seconds in payload)
                    if query.HasField("created_at_start"): q_filters.append(qdrant_models.FieldCondition(key="created_at_ts", range=qdrant_models.Range(gte=query.created_at_start.seconds)))
                    if query.HasField("created_at_end"): q_filters.append(qdrant_models.FieldCondition(key="created_at_ts", range=qdrant_models.Range(lte=query.created_at_end.seconds)))
                    # (Add updated_at similarly if needed)

                    if q_filters:
                        qdrant_filter = qdrant_models.Filter(must=q_filters)

                    # Pagination for Qdrant: Qdrant uses offset. page_token needs to be interpretable as offset.
                    qdrant_offset = 0
                    if page_token:
                        try:
                            qdrant_offset = int(page_token)
                        except ValueError:
                            logger.warning(f"Invalid page_token for Qdrant offset: {page_token}. Defaulting to 0.")
                            qdrant_offset = 0

                    scored_points = await asyncio.to_thread(
                        self.qdrant_repo.search_points,
                        query_vector=list(query.embedding_query),
                        query_filter=qdrant_filter,
                        limit=page_size,
                        offset=qdrant_offset
                    )
                    kem_ids_from_qdrant = [sp.id for sp in scored_points]
                    if not kem_ids_from_qdrant:
                        return [], None # No results from Qdrant

                    # Replace existing ID filters if Qdrant results are primary
                    sql_conditions = []
                    sql_params = []
                    id_placeholders = ','.join('?' for _ in kem_ids_from_qdrant)
                    sql_conditions.append(f"id IN ({id_placeholders})")
                    sql_params.extend(kem_ids_from_qdrant)

                    # Determine next page token for Qdrant results
                    next_qdrant_page_token = None
                    if len(scored_points) == page_size:
                        # Qdrant itself doesn't directly give a "next page token" like this,
                        # it just returns results for the current offset+limit.
                        # We infer there might be more if we received a full page.
                        # The next token would be the next offset.
                        potential_next_offset = qdrant_offset + page_size
                        # To confirm, one could do a count or a query for offset=potential_next_offset, limit=1
                        # For simplicity here, if we got a full page, we assume there could be more.
                        next_qdrant_page_token = str(potential_next_offset)


                    # Now fetch these KEMs from SQLite
                    # The order from Qdrant might be lost unless we re-order in Python after fetching.
                    # Or, if SQLite query can preserve Qdrant's order (e.g. using CASE WHEN with IDs).
                    # For now, default SQLite ordering will apply or keyset pagination's order.
                    kems_from_db_dicts, sqlite_next_page_token = await asyncio.to_thread(
                        self.sqlite_repo.retrieve_kems_from_db,
                        sql_conditions,
                        sql_params,
                        page_size, # Fetch up to page_size, Qdrant might have returned fewer relevant IDs.
                        0 # When fetching by specific IDs from Qdrant, usually no further SQLite pagination needed on these IDs.
                          # Or, if Qdrant returns more IDs than page_size, this needs careful handling.
                          # Let's assume for now Qdrant's limit is the primary driver here.
                    )
                    # If Qdrant was used, its next_page_token is authoritative for this path.
                    final_next_page_token = next_qdrant_page_token

                    # Convert dicts to protos
                    # Note: embeddings are not directly retrieved from SQLite in this path.
                    # Qdrant search results (scored_points) might contain embeddings if requested.
                    # We might need to merge Qdrant embeddings with SQLite data.
                    # For now, _kem_from_db_dict_to_proto doesn't combine with Qdrant embeddings.
                    # This requires more sophisticated merging if KEM proto should have Qdrant's latest embeddings.
                    kem_protos = [self._kem_from_db_dict_to_proto(db_dict) for db_dict in kems_from_db_dicts]
                    return kem_protos, final_next_page_token

                except Exception as e_qdrant_search:
                    logger.error(f"Error during Qdrant search in retrieve_kems: {e_qdrant_search}", exc_info=True)
                    raise StorageError(f"Qdrant search failed: {e_qdrant_search}")


        # Pagination: page_token is now passed directly to the SQLite repository,
        # which handles keyset pagination based on this token.

        # Default order by, can be made configurable or based on query.
        # This MUST match the expectations of the keyset pagination logic in SqliteKemRepository.
        order_by_clause = "ORDER BY updated_at DESC, id DESC" # Ensure stable order for pagination

        kems_db_dicts, next_page_token_str = await asyncio.to_thread(
            self.sqlite_repo.retrieve_kems_from_db,
            sql_conditions,
            sql_params,
            page_size,
            page_token, # Pass the original page_token for keyset pagination
            order_by_clause
        )

        # Convert dicts to KEM protos
        # TODO: If Qdrant was involved, embeddings might need to be fetched/merged separately.
        # For now, assuming embeddings are not part of this SQLite-focused retrieval path,
        # or they are already in the SQLite content/metadata if stored there (unlikely for raw vectors).
        # The KEM proto has an embeddings field, so if not populated here, it will be empty.
        kem_protos = []
        for db_dict in kems_db_dicts:
            # If embeddings are needed and not in SQLite, this is where they'd be fetched from Qdrant
            # using db_dict['id'] and merged into the KEM proto.
            # For now, passing None for embeddings_list.
            kem_protos.append(self._kem_from_db_dict_to_proto(db_dict, embeddings_list=None))

        return kem_protos, next_page_token_str


    async def update_kem(
        self,
        kem_id: str,
        kem_update_data: kem_pb2.KEM,
    ) -> kem_pb2.KEM:
        # 1. Fetch the existing KEM data from SQLite.
        # Use a synchronous, thread-safe call to the SQLite repository.
        existing_kem_dict = await asyncio.to_thread(
            self.sqlite_repo.get_full_kem_by_id, kem_id
        )
        if not existing_kem_dict:
            raise KemNotFoundError(f"KEM with id '{kem_id}' not found.")

        # 2. Convert the database dictionary to a KEM protobuf object.
        # We don't need embeddings for this, as they are not stored in SQLite.
        existing_kem_proto = self._kem_from_db_dict_to_proto(existing_kem_dict)

        # 3. Merge the fields from the update request into the existing KEM proto.
        # MergeFrom intelligently overwrites fields present in kem_update_data.
        existing_kem_proto.MergeFrom(kem_update_data)

        # 4. Use the existing store_kem method to save the updated KEM.
        # store_kem handles ID preservation, timestamp updates, and dual-writes
        # to SQLite and Qdrant, ensuring consistency.
        updated_kem = await self.store_kem(existing_kem_proto)

        return updated_kem

    async def delete_kem(self, kem_id: str) -> bool:
        # 1. Delete from SQLite first. This is our source of truth.
        was_deleted = await asyncio.to_thread(
            self.sqlite_repo.delete_kem_by_id, kem_id
        )

        if not was_deleted:
            # KEM was not in SQLite, so nothing to do in Qdrant either.
            logger.info(f"delete_kem: KEM with id '{kem_id}' not found in SQLite. No action taken.")
            return False

        # 2. If deletion from SQLite was successful, delete from Qdrant.
        if self.qdrant_repo:
            try:
                # This method is synchronous in the client, so no need for to_thread
                # if the method itself doesn't block for long I/O.
                # However, for consistency with other async calls, we can use it.
                await asyncio.to_thread(self.qdrant_repo.delete_points_by_ids, [kem_id])
                logger.info(f"delete_kem: Successfully deleted KEM '{kem_id}' from Qdrant.")
            except Exception as e_qdrant:
                # Log the error but don't re-raise, as the primary data in SQLite
                # has already been deleted. This maintains data integrity in our
                # source of truth, at the cost of a potential orphan in Qdrant.
                logger.error(f"delete_kem: Failed to delete KEM '{kem_id}' from Qdrant after deleting from SQLite: {e_qdrant}", exc_info=True)
                # We still return True because the KEM was successfully deleted from the primary store.

        return True

    async def batch_store_kems(
        self,
        kems: List[kem_pb2.KEM]
    ) -> Tuple[List[kem_pb2.KEM], List[str]]:
        if not kems:
            return [], []

        processed_kems = []
        qdrant_points = []
        failed_references = []

        # First, prepare all KEMs and points
        # This includes generating IDs and timestamps.
        # We can fetch all existing creation timestamps in one batch.
        kem_ids_to_check = [kem.id for kem in kems if kem.id]
        existing_timestamps = await asyncio.to_thread(
            self.sqlite_repo.get_kems_creation_timestamps, kem_ids_to_check
        )

        for kem in kems:
            kem_to_store = kem_pb2.KEM()
            kem_to_store.CopyFrom(kem)

            kem_id = kem_to_store.id if kem_to_store.id else str(uuid.uuid4())
            kem_to_store.id = kem_id

            current_time_proto = Timestamp()
            current_time_proto.GetCurrentTime()

            # Preserve created_at if it exists
            final_created_at_proto = Timestamp()
            existing_ts_str = existing_timestamps.get(kem_id)
            if existing_ts_str:
                try:
                    final_created_at_proto.FromJsonString(existing_ts_str + "Z")
                except Exception:
                    final_created_at_proto.CopyFrom(current_time_proto)
            else:
                final_created_at_proto.CopyFrom(current_time_proto)

            kem_to_store.created_at.CopyFrom(final_created_at_proto)
            kem_to_store.updated_at.CopyFrom(current_time_proto)

            processed_kems.append(kem_to_store)

            # Prepare Qdrant point if embeddings exist
            if self.qdrant_repo and kem_to_store.embeddings:
                qdrant_payload = {"kem_id_ref": kem_id}
                # ... (add other metadata to payload if needed) ...
                point = PointStruct(id=kem_id, vector=list(kem_to_store.embeddings), payload=qdrant_payload)
                qdrant_points.append(point)

        # Now, perform batch operations
        try:
            # 1. Batch store to SQLite
            sqlite_batch_data = [
                {
                    "id": k.id,
                    "content_type": k.content_type,
                    "content": k.content,
                    "metadata_json": json.dumps(dict(k.metadata)),
                    "created_at_iso": k.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                    "updated_at_iso": k.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                } for k in processed_kems
            ]
            await asyncio.to_thread(self.sqlite_repo.batch_store_or_replace_kems, sqlite_batch_data)

            # 2. Batch store to Qdrant
            if self.qdrant_repo and qdrant_points:
                await asyncio.to_thread(self.qdrant_repo.upsert_points_batch, qdrant_points)

            # If both succeed, all KEMs were successful
            return processed_kems, []

        except Exception as e:
            logger.error(f"batch_store_kems: An error occurred during batch processing: {e}", exc_info=True)
            # If any part of the batch fails, we treat the whole batch as failed for simplicity.
            # A more granular implementation could try to identify which KEMs failed.
            failed_references = [kem.id for kem in kems]
            return [], failed_references

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
        all_ok = True
        msg_parts = []

        # 1. Check native SQLite health
        try:
            conn = self.sqlite_repo._get_sqlite_conn()
            cursor = conn.cursor()
            sqlite_query = getattr(self.config, "HEALTH_CHECK_SQLITE_QUERY", "SELECT 1")
            cursor.execute(sqlite_query)
            cursor.fetchone()
            msg_parts.append("NativeSQLite:OK")
        except Exception as e_sqlite_check:
            logger.warning(f"DefaultGLMRepository Health Check: Native SQLite check failed: {e_sqlite_check}")
            all_ok = False
            msg_parts.append(f"NativeSQLite:FAIL ({e_sqlite_check})")

        # 2. Check native Qdrant health (if configured)
        if self.qdrant_repo and self.qdrant_repo.client:
            try:
                self.qdrant_repo.client.get_collections() # Simple connectivity test
                msg_parts.append("NativeQdrant:OK")
            except Exception as e_qdrant_check:
                logger.warning(f"DefaultGLMRepository Health Check: Native Qdrant check failed: {e_qdrant_check}")
                all_ok = False
                msg_parts.append(f"NativeQdrant:FAIL ({e_qdrant_check})")
        elif not self.config.QDRANT_HOST:
            msg_parts.append("NativeQdrant:N/A (not configured)")
        else: # Configured but Qdrant client not initialized (e.g. init failed)
            logger.warning("DefaultGLMRepository Health Check: Native Qdrant configured but client not available.")
            all_ok = False
            msg_parts.append("NativeQdrant:FAIL (client not initialized)")

        # 3. Check health of external repositories
        if self.external_repos:
            for name, repo in self.external_repos.items():
                try:
                    # Need to run async health check method
                    # If check_health itself is defined as async in BaseExternalRepository
                    ext_healthy, ext_msg = await repo.check_health()
                    if ext_healthy:
                        msg_parts.append(f"ExternalRepo({name}):OK ({ext_msg})")
                    else:
                        all_ok = False
                        msg_parts.append(f"ExternalRepo({name}):FAIL ({ext_msg})")
                except Exception as e_ext_health:
                    logger.error(f"Error during health check for external repo '{name}': {e_ext_health}", exc_info=True)
                    all_ok = False
                    msg_parts.append(f"ExternalRepo({name}):FAIL (Exception: {e_ext_health})")
        else:
            msg_parts.append("ExternalRepos:N/A (none configured)")

        return all_ok, "; ".join(msg_parts)

# Need to import os for db_path
import os
