import asyncio
import uuid
import json
import logging
from typing import List, Tuple, Optional, Dict, Any
import base64

from qdrant_client import models as qdrant_models, AsyncQdrantClient
from qdrant_client.http.models import PointStruct
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict

from dcs_memory.generated_grpc import kem_pb2, glm_service_pb2
from dcs_memory.services.glm.app.config import GLMConfig, ExternalDataSourceConfig
from dcs_memory.services.glm.app.repositories.base import BasePersistentStorageRepository, StorageError, KemNotFoundError, BackendUnavailableError, InvalidQueryError
from dcs_memory.services.glm.app.sqlite_repository import SqliteKemRepository
from dcs_memory.services.glm.app.qdrant_repository import QdrantKemRepository
from dcs_memory.services.glm.app.repositories.external_base import BaseExternalRepository
import os

logger = logging.getLogger(__name__)

def get_external_repository_connector(config: ExternalDataSourceConfig, glm_config: GLMConfig) -> Optional[BaseExternalRepository]:
    if config.type == "postgresql":
        from .postgres_connector import PostgresExternalRepository
        try:
            return PostgresExternalRepository(config, glm_config)
        except Exception as e:
            logger.error(f"Failed to initialize PostgresExternalRepository: {e}", exc_info=True)
            return None
    return None

class DefaultGLMRepository(BasePersistentStorageRepository):
    def __init__(self, config: GLMConfig, app_dir: str):
        self.config = config
        db_path = os.path.join(app_dir, self.config.DB_FILENAME)
        self.sqlite_repo = SqliteKemRepository(db_path, self.config)
        self.qdrant_repo: Optional[QdrantKemRepository] = None
        self.external_repos: Dict[str, BaseExternalRepository] = {}

    async def initialize(self):
        await self.sqlite_repo.initialize()
        logger.info("DefaultGLMRepository: SqliteKemRepository initialized.")

        if self.config.QDRANT_HOST:
            try:
                qdrant_client = AsyncQdrantClient(
                    host=self.config.QDRANT_HOST,
                    port=self.config.QDRANT_PORT,
                    timeout=self.config.QDRANT_CLIENT_TIMEOUT_S
                )
                await qdrant_client.get_collections()
                self.qdrant_repo = QdrantKemRepository(
                    qdrant_client=qdrant_client,
                    collection_name=self.config.QDRANT_COLLECTION,
                    default_vector_size=self.config.DEFAULT_VECTOR_SIZE,
                    default_distance_metric=self.config.QDRANT_DEFAULT_DISTANCE_METRIC
                )
                await self.qdrant_repo.ensure_collection()
                logger.info("DefaultGLMRepository: QdrantKemRepository initialized.")
            except Exception as e:
                logger.error(f"Qdrant client/repository initialization failed: {e}", exc_info=True)
                self.qdrant_repo = None
        else:
            logger.warning("QDRANT_HOST not configured. Qdrant features will be unavailable.")

        if hasattr(self.config, 'external_data_sources'):
            for ext_ds_config in self.config.external_data_sources:
                connector = get_external_repository_connector(ext_ds_config, self.config)
                if connector:
                    self.external_repos[ext_ds_config.name] = connector

        logger.info("DefaultGLMRepository initialized.")

    def _kem_from_db_dict_to_proto(self, kem_db_dict: Dict[str, Any], embeddings_list: Optional[List[float]] = None) -> kem_pb2.KEM:
        kem_for_proto = {'id': kem_db_dict.get('id'), 'content_type': kem_db_dict.get('content_type')}
        content_bytes = kem_db_dict.get('content')
        if content_bytes is not None:
            kem_for_proto['content'] = base64.b64encode(content_bytes).decode('ascii')
        metadata_json_str = kem_db_dict.get('metadata', '{}')
        try:
            kem_for_proto['metadata'] = json.loads(metadata_json_str)
        except json.JSONDecodeError:
            kem_for_proto['metadata'] = {}
        for ts_field_name in ['created_at', 'updated_at']:
            ts_iso_str = kem_db_dict.get(ts_field_name)
            if ts_iso_str and isinstance(ts_iso_str, str):
                ts_parse_str = ts_iso_str
                if 'T' in ts_iso_str and not ts_iso_str.endswith("Z") and '+' not in ts_iso_str and '-' not in ts_iso_str[10:]:
                    ts_parse_str = ts_iso_str + "Z"
                kem_for_proto[ts_field_name] = ts_parse_str
        if embeddings_list:
            kem_for_proto['embeddings'] = embeddings_list
        final_kem_proto = kem_pb2.KEM()
        ParseDict(kem_for_proto, final_kem_proto, ignore_unknown_fields=True)
        return final_kem_proto

    async def store_kem(self, kem: kem_pb2.KEM) -> kem_pb2.KEM:
        kem_to_store = kem_pb2.KEM()
        kem_to_store.CopyFrom(kem)
        kem_id = kem_to_store.id or str(uuid.uuid4())
        kem_to_store.id = kem_id
        has_embeddings = bool(kem_to_store.embeddings)
        current_time_proto = Timestamp()
        current_time_proto.GetCurrentTime()
        existing_created_at_str = await self.sqlite_repo.get_kem_creation_timestamp(kem_id)
        final_created_at_proto = Timestamp()
        if existing_created_at_str:
            try:
                final_created_at_proto.FromJsonString(existing_created_at_str + "Z")
            except Exception:
                final_created_at_proto.CopyFrom(current_time_proto)
        else:
            final_created_at_proto.CopyFrom(current_time_proto)
        kem_to_store.created_at.CopyFrom(final_created_at_proto)
        kem_to_store.updated_at.CopyFrom(current_time_proto)
        qdrant_op_done = False
        if self.qdrant_repo and has_embeddings:
            qdrant_payload = {"kem_id_ref": kem_id}
            if kem_to_store.metadata:
                for k, v_str in kem_to_store.metadata.items():
                    qdrant_payload[f"md_{k}"] = v_str
            if kem_to_store.HasField("created_at"):
                qdrant_payload["created_at_ts"] = kem_to_store.created_at.seconds
            if kem_to_store.HasField("updated_at"):
                qdrant_payload["updated_at_ts"] = kem_to_store.updated_at.seconds
            try:
                point = PointStruct(id=kem_id, vector=list(kem_to_store.embeddings), payload=qdrant_payload)
                await self.qdrant_repo.upsert_point(point)
                qdrant_op_done = True
            except Exception as e_qdrant:
                raise StorageError(f"Qdrant processing error: {e_qdrant}") from e_qdrant
        try:
            await self.sqlite_repo.store_or_replace_kem(
                kem_id=kem_to_store.id,
                content_type=kem_to_store.content_type,
                content=kem_to_store.content,
                metadata_json=json.dumps(dict(kem_to_store.metadata)),
                created_at_iso=kem_to_store.created_at.ToDatetime().isoformat(timespec='microseconds').replace('+00:00', ''),
                updated_at_iso=kem_to_store.updated_at.ToDatetime().isoformat(timespec='microseconds').replace('+00:00', '')
            )
        except Exception as e_sqlite:
            if qdrant_op_done and self.qdrant_repo:
                try:
                    await self.qdrant_repo.delete_points_by_ids([kem_id])
                except Exception as e_qdrant_rollback:
                    logger.critical(f"CRITICAL - Failed Qdrant rollback for KEM ID '{kem_id}': {e_qdrant_rollback}")
            raise StorageError(f"SQLite processing error: {e_sqlite}") from e_sqlite
        return kem_to_store

    async def retrieve_kems(self, query: glm_service_pb2.KEMQuery, page_size: int, page_token: Optional[str]) -> Tuple[List[kem_pb2.KEM], Optional[str]]:
        sql_conditions, sql_params = [], []
        kem_id_subset = None
        if query.text_query:
            kem_id_subset = await self.sqlite_repo.search_kems_by_text(query.text_query)
            if not kem_id_subset: return [], None
            if query.ids:
                kem_id_subset = list(set(kem_id_subset) & set(query.ids))
                if not kem_id_subset: return [], None
        elif query.ids:
            kem_id_subset = list(query.ids)
        if kem_id_subset is not None:
            sql_conditions.append(f"id IN ({','.join('?' for _ in kem_id_subset)})")
            sql_params.extend(kem_id_subset)
        if query.metadata_filters:
            for key, value in query.metadata_filters.items():
                sql_conditions.append(f"json_extract(metadata, '$.{key}') = ?")
                sql_params.append(value)
        kems_db_dicts, next_page_token_str = await self.sqlite_repo.retrieve_kems_from_db(sql_conditions, sql_params, page_size, page_token, "ORDER BY updated_at DESC, id DESC")
        kem_protos = [self._kem_from_db_dict_to_proto(db_dict) for db_dict in kems_db_dicts]
        return kem_protos, next_page_token_str

    async def update_kem(self, kem_id: str, kem_update_data: kem_pb2.KEM) -> kem_pb2.KEM:
        existing_kem_dict = await self.sqlite_repo.get_full_kem_by_id(kem_id)
        if not existing_kem_dict:
            raise KemNotFoundError(f"KEM with id '{kem_id}' not found.")
        existing_kem_proto = self._kem_from_db_dict_to_proto(existing_kem_dict)
        existing_kem_proto.MergeFrom(kem_update_data)
        return await self.store_kem(existing_kem_proto)

    async def delete_kem(self, kem_id: str) -> bool:
        was_deleted = await self.sqlite_repo.delete_kem_by_id(kem_id)
        if not was_deleted:
            return False
        if self.qdrant_repo:
            try:
                await self.qdrant_repo.delete_points_by_ids([kem_id])
            except Exception as e_qdrant:
                logger.error(f"Failed to delete KEM '{kem_id}' from Qdrant: {e_qdrant}", exc_info=True)
        return True

    async def batch_store_kems(self, kems: List[kem_pb2.KEM]) -> Tuple[List[kem_pb2.KEM], List[str]]:
        if not kems: return [], []
        processed_kems, qdrant_points, failed_references = [], [], []
        kem_ids_to_check = [kem.id for kem in kems if kem.id]
        existing_timestamps = await self.sqlite_repo.get_kems_creation_timestamps(kem_ids_to_check)
        for kem in kems:
            kem_to_store = kem_pb2.KEM()
            kem_to_store.CopyFrom(kem)
            kem_id = kem_to_store.id or str(uuid.uuid4())
            kem_to_store.id = kem_id
            current_time_proto = Timestamp()
            current_time_proto.GetCurrentTime()
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
            if self.qdrant_repo and kem_to_store.embeddings:
                qdrant_payload = {"kem_id_ref": kem_id}
                point = PointStruct(id=kem_id, vector=list(kem_to_store.embeddings), payload=qdrant_payload)
                qdrant_points.append(point)
        try:
            sqlite_batch_data = [{"id": k.id, "content_type": k.content_type, "content": k.content, "metadata_json": json.dumps(dict(k.metadata)), "created_at_iso": k.created_at.ToDatetime().isoformat(timespec='microseconds').replace('+00:00', ''), "updated_at_iso": k.updated_at.ToDatetime().isoformat(timespec='microseconds').replace('+00:00', '')} for k in processed_kems]
            await self.sqlite_repo.batch_store_or_replace_kems(sqlite_batch_data)
            if self.qdrant_repo and qdrant_points:
                await self.qdrant_repo.upsert_points_batch(qdrant_points)
            return processed_kems, []
        except Exception as e:
            logger.error(f"batch_store_kems: An error occurred: {e}", exc_info=True)
            failed_references = [kem.id for kem in kems]
            return [], failed_references

    async def check_health(self) -> Tuple[bool, str]:
        all_ok, msg_parts = True, []
        try:
            conn = await self.sqlite_repo._get_db()
            await conn.execute("SELECT 1")
            msg_parts.append("NativeSQLite:OK")
        except Exception as e:
            all_ok = False
            msg_parts.append(f"NativeSQLite:FAIL ({e})")
        return all_ok, "; ".join(msg_parts)
