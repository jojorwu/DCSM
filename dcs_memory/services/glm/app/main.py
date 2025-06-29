import grpc
from concurrent import futures
import time # Keep time for server wait, though not directly used in servicer logic now
import sys
import os
import uuid
import json
import sqlite3 # Keep for potential direct use in repositories if needed, or for type hints
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import PointStruct
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict
import typing # Keep for type hints
import logging

# Import configuration
from .config import GLMConfig
from .repositories import SqliteKemRepository, QdrantKemRepository # Import repositories

# --- Start of gRPC code import block ---
current_script_path = os.path.abspath(__file__)
app_dir = os.path.dirname(current_script_path) # Used for DB path
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

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2
from generated_grpc import glm_service_pb2_grpc
from google.protobuf import empty_pb2
# --- End of gRPC code import block ---

class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self):
        logger.info("Initializing GlobalLongTermMemoryServicerImpl...")
        self.config = config

        db_path = os.path.join(app_dir, self.config.DB_FILENAME)
        try:
            self.sqlite_repo = SqliteKemRepository(db_path, self.config)
            logger.info("SqliteKemRepository initialized successfully.")
        except Exception as e_sqlite_init:
            logger.critical(f"CRITICAL ERROR initializing SqliteKemRepository: {e_sqlite_init}", exc_info=True)
            raise

        self.qdrant_repo = None
        try:
            if self.config.QDRANT_HOST:
                qdrant_client = QdrantClient(host=self.config.QDRANT_HOST, port=self.config.QDRANT_PORT, timeout=10)
                qdrant_client.get_collections()
                logger.info(f"Qdrant client successfully connected to {self.config.QDRANT_HOST}:{self.config.QDRANT_PORT}")

                self.qdrant_repo = QdrantKemRepository(qdrant_client, self.config.QDRANT_COLLECTION, self.config.DEFAULT_VECTOR_SIZE)
                self.qdrant_repo.ensure_collection()
                logger.info("QdrantKemRepository initialized successfully.")
            else:
                logger.warning("QDRANT_HOST not configured. Qdrant features will be unavailable.")
        except Exception as e_qdrant_init:
            logger.error(f"ERROR during Qdrant client/repository initialization: {e_qdrant_init}. Qdrant features will be unavailable.", exc_info=True)
            self.qdrant_repo = None

        logger.info("GLM servicer initialized.")

    def _kem_dict_to_proto(self, kem_data: dict) -> kem_pb2.KEM:
        kem_data_copy = kem_data.copy()
        if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
             kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')
        return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)

    def _kem_from_db_dict(self, kem_db_dict: typing.Dict[str, typing.Any], embeddings_map: typing.Optional[typing.Dict[str, typing.List[float]]] = None) -> kem_pb2.KEM:
        if not kem_db_dict:
            logger.error("_kem_from_db_dict received empty or None kem_db_dict")
            return kem_pb2.KEM()

        kem_dict_for_proto = kem_db_dict.copy()
        metadata_str = kem_dict_for_proto.get('metadata', '{}')
        try:
            kem_dict_for_proto['metadata'] = json.loads(metadata_str)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse metadata JSON: {metadata_str} for KEM ID {kem_db_dict.get('id')}. Using empty metadata.", exc_info=True)
            kem_dict_for_proto['metadata'] = {}

        for ts_field_name in ['created_at', 'updated_at']:
            ts_str = kem_dict_for_proto.get(ts_field_name)
            if ts_str and isinstance(ts_str, str):
                ts_proto = Timestamp()
                if not ts_str.endswith("Z") and '+' not in ts_str and '-' not in ts_str[10:]:
                    ts_str_for_parse = ts_str + "Z"
                else:
                    ts_str_for_parse = ts_str
                try:
                    ts_proto.FromJsonString(ts_str_for_parse)
                    kem_dict_for_proto[ts_field_name] = ts_proto
                except Exception as e_ts_parse:
                     logger.warning(f"Failed to parse timestamp string '{ts_str}' (tried as '{ts_str_for_parse}') for field '{ts_field_name}' in KEM ID {kem_db_dict.get('id')}: {e_ts_parse}", exc_info=True)
                     if ts_field_name in kem_dict_for_proto: del kem_dict_for_proto[ts_field_name]
            elif isinstance(ts_str, Timestamp):
                 pass
            else:
                 if ts_field_name in kem_dict_for_proto: del kem_dict_for_proto[ts_field_name]

        if embeddings_map and kem_dict_for_proto.get('id') in embeddings_map:
            kem_dict_for_proto['embeddings'] = embeddings_map[kem_dict_for_proto['id']]
        else:
            kem_dict_for_proto.setdefault('embeddings', [])

        return self._kem_dict_to_proto(kem_dict_for_proto)

    def StoreKEM(self, request: glm_service_pb2.StoreKEMRequest, context) -> glm_service_pb2.StoreKEMResponse:
        kem = request.kem
        kem_id = kem.id if kem.id else str(uuid.uuid4())
        logger.info(f"StoreKEM: ID='{kem_id}' (client-provided ID='{request.kem.id}')")

        current_time_proto = Timestamp()
        current_time_proto.GetCurrentTime()

        kem.id = kem_id

        final_created_at_proto = Timestamp()
        existing_created_at_str = self.sqlite_repo.get_kem_creation_timestamp(kem_id)

        if existing_created_at_str:
            try:
                parse_str = existing_created_at_str + ("Z" if not existing_created_at_str.endswith("Z") and '+' not in existing_created_at_str and '-' not in existing_created_at_str[10:] else "")
                final_created_at_proto.FromJsonString(parse_str)
            except Exception as e_parse:
                 logger.error(f"StoreKEM: Error parsing existing created_at '{existing_created_at_str}' from DB for KEM ID '{kem_id}': {e_parse}", exc_info=True)
                 final_created_at_proto.CopyFrom(current_time_proto)
        elif kem.HasField("created_at") and kem.created_at.seconds > 0:
            final_created_at_proto.CopyFrom(kem.created_at)
        else:
            final_created_at_proto.CopyFrom(current_time_proto)

        kem.created_at.CopyFrom(final_created_at_proto)
        kem.updated_at.CopyFrom(current_time_proto)

        if self.qdrant_repo and kem.embeddings:
            if len(kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                msg = f"Embedding dimension ({len(kem.embeddings)}) does not match Qdrant config ({self.config.DEFAULT_VECTOR_SIZE})."
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return glm_service_pb2.StoreKEMResponse()

            qdrant_payload = {"kem_id_ref": kem_id}
            if kem.metadata:
                for k, v_pb_val in kem.metadata.items(): qdrant_payload[f"md_{k}"] = v_pb_val
            if kem.HasField("created_at"): qdrant_payload["created_at_ts"] = kem.created_at.seconds
            if kem.HasField("updated_at"): qdrant_payload["updated_at_ts"] = kem.updated_at.seconds

            try:
                point = PointStruct(id=kem_id, vector=list(kem.embeddings), payload=qdrant_payload)
                self.qdrant_repo.upsert_point(point)
                logger.info(f"StoreKEM: Embeddings for KEM ID '{kem_id}' saved to Qdrant.")
            except Exception as e_qdrant:
                msg = f"StoreKEM: Qdrant error for ID '{kem_id}': {e_qdrant}"
                logger.error(msg, exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, msg); return glm_service_pb2.StoreKEMResponse()

        try:
            self.sqlite_repo.store_or_replace_kem(
                kem_id=kem.id,
                content_type=kem.content_type,
                content=kem.content,
                metadata_json=json.dumps(dict(kem.metadata)),
                created_at_iso=kem.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                updated_at_iso=kem.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
            )
            logger.info(f"StoreKEM: Metadata/content for KEM ID '{kem_id}' saved to SQLite.")
        except Exception as e_sqlite:
            msg = f"StoreKEM: SQLite error for ID '{kem_id}': {e_sqlite}"
            logger.error(msg, exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, msg); return glm_service_pb2.StoreKEMResponse()

        logger.info(f"KEM ID '{kem_id}' successfully saved/updated.")
        return glm_service_pb2.StoreKEMResponse(kem=kem)

    def RetrieveKEMs(self, request: glm_service_pb2.RetrieveKEMsRequest, context) -> glm_service_pb2.RetrieveKEMsResponse:
        query = request.query
        page_size = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
        offset = 0
        if request.page_token:
            try:
                offset = int(request.page_token)
                if offset < 0: offset = 0
            except ValueError:
                logger.warning(f"Invalid page_token format: '{request.page_token}'. Using offset=0.")

        logger.info(f"RetrieveKEMs: query_filters={query.metadata_filters}, query_ids={list(query.ids)}, "
                    f"vector_query_present={bool(query.embedding_query)}, page_size={page_size}, offset={offset}")

        found_kems_proto_list: typing.List[kem_pb2.KEM] = []
        next_page_token_str = ""

        if query.embedding_query:
            if not self.qdrant_repo:
                context.abort(grpc.StatusCode.INTERNAL, "Qdrant service not available for vector search."); return glm_service_pb2.RetrieveKEMsResponse()
            if len(query.embedding_query) != self.config.DEFAULT_VECTOR_SIZE:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid vector dimension: {len(query.embedding_query)}, expected {self.config.DEFAULT_VECTOR_SIZE}"); return glm_service_pb2.RetrieveKEMsResponse()

            try:
                q_conditions = []
                if query.ids: q_conditions.append(models.HasIdCondition(has_id=list(query.ids)))
                if query.metadata_filters:
                    for k, v_str in query.metadata_filters.items(): q_conditions.append(models.FieldCondition(key=f"md_{k}", match=models.MatchValue(value=v_str)))

                def add_ts_range_q(field_name, ts_start, ts_end, conds):
                    gte, lte = (ts_start.seconds if ts_start.seconds > 0 or ts_start.nanos > 0 else None), \
                               (ts_end.seconds if ts_end.seconds > 0 or ts_end.nanos > 0 else None)
                    if gte is not None or lte is not None: conds.append(models.FieldCondition(key=field_name, range=models.Range(gte=gte, lte=lte)))

                add_ts_range_q("created_at_ts", query.created_at_start, query.created_at_end, q_conditions)
                add_ts_range_q("updated_at_ts", query.updated_at_start, query.updated_at_end, q_conditions)

                q_filter = models.Filter(must=q_conditions) if q_conditions else None

                search_hits = self.qdrant_repo.search_points(
                    query_vector=list(query.embedding_query), q_filter=q_filter,
                    limit=page_size, offset=offset, with_vectors=True
                )
                if not search_hits: return glm_service_pb2.RetrieveKEMsResponse(kems=[], next_page_token="")

                q_ids = [hit.id for hit in search_hits]
                embed_map = {hit.id: list(hit.vector) for hit in search_hits if hit.vector}

                ordered_instr_clause = f"ORDER BY INSTR('," + ",".join(q_ids) + ",', ',' || id || ',')"
                db_kems_dicts, _ = self.sqlite_repo.retrieve_kems_from_db(
                    sql_conditions=[f"id IN ({','.join('?' for _ in q_ids)})"], sql_params=q_ids,
                    page_size=len(q_ids), offset=0, order_by_clause=ordered_instr_clause
                )
                for db_dict in db_kems_dicts:
                    found_kems_proto_list.append(self._kem_from_db_dict(db_dict, embed_map))

                if len(search_hits) == page_size: next_page_token_str = str(offset + page_size)

            except Exception as e_q_retrieve:
                logger.error(f"RetrieveKEMs Qdrant/SQLite error (vector search): {e_q_retrieve}", exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"RetrieveKEMs (vector) error: {e_q_retrieve}"); return glm_service_pb2.RetrieveKEMsResponse()
        else: # Non-vector search
            sql_conds, sql_pars = [], []
            if query.ids:
                sql_conds.append(f"id IN ({','.join('?' for _ in query.ids)})"); sql_pars.extend(list(query.ids))
            for k, v in query.metadata_filters.items():
                sql_conds.append(f"json_extract(metadata, '$.{k}') = ?"); sql_pars.append(v)

            def add_date_cond_sql(fname, ts_s, ts_e, conds, pars):
                if ts_s.seconds > 0 or ts_s.nanos > 0:
                    conds.append(f"{fname} >= ?"); pars.append(ts_s.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))
                if ts_e.seconds > 0 or ts_e.nanos > 0:
                    conds.append(f"{fname} <= ?"); pars.append(ts_e.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''))

            add_date_cond_sql("created_at", query.created_at_start, query.created_at_end, sql_conds, sql_pars)
            add_date_cond_sql("updated_at", query.updated_at_start, query.updated_at_end, sql_conds, sql_pars)

            try:
                db_kems_dicts, next_page_token_str = self.sqlite_repo.retrieve_kems_from_db(
                    sql_conditions=sql_conds, sql_params=sql_pars,
                    page_size=page_size, offset=offset
                )

                ids_from_page = [d['id'] for d in db_kems_dicts]
                embed_map_page = {}
                if ids_from_page and self.qdrant_repo:
                    try:
                        q_points = self.qdrant_repo.retrieve_points_by_ids(ids_from_page, with_vectors=True)
                        for p in q_points:
                            if p.vector: embed_map_page[p.id] = list(p.vector)
                    except Exception as e_q_emb: logger.warning(f"Failed to retrieve embeddings for non-vector search: {e_q_emb}", exc_info=True)

                for db_dict in db_kems_dicts:
                    found_kems_proto_list.append(self._kem_from_db_dict(db_dict, embed_map_page))

            except Exception as e_sql_retrieve:
                logger.error(f"RetrieveKEMs SQLite error (non-vector search): {e_sql_retrieve}", exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"RetrieveKEMs (non-vector) error: {e_sql_retrieve}"); return glm_service_pb2.RetrieveKEMsResponse()

        return glm_service_pb2.RetrieveKEMsResponse(kems=found_kems_proto_list, next_page_token=next_page_token_str)

    def UpdateKEM(self, request: glm_service_pb2.UpdateKEMRequest, context) -> kem_pb2.KEM:
        kem_id = request.kem_id
        kem_data_update = request.kem_data_update
        if not kem_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID must be specified for update.")
            return kem_pb2.KEM()

        kem_current_db_dict = self.sqlite_repo.get_full_kem_by_id(kem_id)
        if not kem_current_db_dict:
            context.abort(grpc.StatusCode.NOT_FOUND, f"KEM with ID '{kem_id}' not found for update.")
            return kem_pb2.KEM()

        ts_now_proto = Timestamp(); ts_now_proto.GetCurrentTime()

        qdrant_payload_update = {"kem_id_ref": kem_id}
        current_metadata = json.loads(kem_current_db_dict.get('metadata', '{}'))
        metadata_for_qdrant = current_metadata.copy()
        if kem_data_update.metadata:
            metadata_for_qdrant = dict(kem_data_update.metadata)

        for k, v in metadata_for_qdrant.items(): qdrant_payload_update[f"md_{k}"] = str(v)

        try:
            created_at_ts_q = Timestamp()
            original_created_at_iso = kem_current_db_dict['created_at']
            parse_str_q_creat = original_created_at_iso + ("Z" if not original_created_at_iso.endswith("Z") and '+' not in original_created_at_iso and '-' not in original_created_at_iso[10:] else "")
            created_at_ts_q.FromJsonString(parse_str_q_creat)
            qdrant_payload_update["created_at_ts"] = created_at_ts_q.seconds
        except Exception as e_ts_upd_q:
            logger.warning(f"UpdateKEM: Failed to parse original_created_at_iso for Qdrant payload: {e_ts_upd_q}", exc_info=True)
        qdrant_payload_update["updated_at_ts"] = ts_now_proto.seconds

        final_embeddings_for_resp = []

        if self.qdrant_repo:
            try:
                q_point_to_upsert: models.PointStruct
                if kem_data_update.embeddings:
                    new_embeds = list(kem_data_update.embeddings)
                    if len(new_embeds) != self.config.DEFAULT_VECTOR_SIZE:
                        msg = f"Invalid embedding dimension for KEM ID '{kem_id}'"
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return kem_pb2.KEM()
                    q_point_to_upsert = PointStruct(id=kem_id, vector=new_embeds, payload=qdrant_payload_update)
                    final_embeddings_for_resp = new_embeds
                    logger.info(f"UpdateKEM: Qdrant: Updating vector and payload for KEM ID '{kem_id}'.")
                else:
                    existing_q_points = self.qdrant_repo.retrieve_points_by_ids([kem_id], with_vectors=True)
                    existing_vec = None
                    if existing_q_points and existing_q_points[0].vector:
                        existing_vec = list(existing_q_points[0].vector)
                        final_embeddings_for_resp = existing_vec
                    q_point_to_upsert = PointStruct(id=kem_id, vector=existing_vec, payload=qdrant_payload_update)
                    logger.info(f"UpdateKEM: Qdrant: Updating payload for KEM ID '{kem_id}' (vector preserved: {existing_vec is not None}).")

                self.qdrant_repo.upsert_point(q_point_to_upsert)
            except Exception as e_q_upd:
                msg = f"UpdateKEM: Qdrant error for ID '{kem_id}': {e_q_upd}"
                logger.error(msg, exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, msg); return kem_pb2.KEM()

        sqlite_updated_at_iso = ts_now_proto.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
        sqlite_content_type = kem_data_update.content_type if kem_data_update.HasField("content_type") else None
        sqlite_content = kem_data_update.content.value if kem_data_update.HasField("content") else None
        sqlite_metadata_json = json.dumps(dict(kem_data_update.metadata)) if kem_data_update.metadata else None

        try:
            self.sqlite_repo.update_kem_fields(
                kem_id=kem_id, updated_at_iso=sqlite_updated_at_iso,
                content_type=sqlite_content_type, content=sqlite_content,
                metadata_json=sqlite_metadata_json
            )
        except Exception as e_sql_upd:
            msg = f"UpdateKEM: SQLite error for ID '{kem_id}': {e_sql_upd}"
            logger.error(msg, exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, msg); return kem_pb2.KEM()

        final_kem_db_dict_resp = self.sqlite_repo.get_full_kem_by_id(kem_id)
        if not final_kem_db_dict_resp:
            logger.error(f"UpdateKEM: KEM ID '{kem_id}' disappeared from DB after update for response.")
            context.abort(grpc.StatusCode.INTERNAL, "KEM disappeared post-update."); return kem_pb2.KEM()

        return self._kem_from_db_dict(final_kem_db_dict_resp, {kem_id: final_embeddings_for_resp} if final_embeddings_for_resp else None)

    def DeleteKEM(self, request: glm_service_pb2.DeleteKEMRequest, context) -> empty_pb2.Empty:
        kem_id = request.kem_id
        if not kem_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID must be specified for deletion.")
            return empty_pb2.Empty()

        if self.qdrant_repo:
            try:
                self.qdrant_repo.delete_points_by_ids([kem_id])
                logger.info(f"DeleteKEM: Qdrant processed delete for KEM ID '{kem_id}'.")
            except Exception as e_qd_del:
                msg = f"DeleteKEM: Qdrant error for ID '{kem_id}': {e_qd_del}"
                logger.error(msg, exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, msg); return empty_pb2.Empty()

        try:
            deleted_sqlite = self.sqlite_repo.delete_kem_by_id(kem_id)
            if not deleted_sqlite: logger.warning(f"DeleteKEM: KEM ID '{kem_id}' not found in SQLite (or already deleted).")
            else: logger.info(f"DeleteKEM: KEM ID '{kem_id}' deleted from SQLite.")
        except Exception as e_sql_del:
            msg = f"DeleteKEM: SQLite error for ID '{kem_id}': {e_sql_del}"
            logger.error(msg, exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, msg); return empty_pb2.Empty()

        logger.info(f"DeleteKEM: Successfully processed deletion for KEM ID '{kem_id}'.")
        return empty_pb2.Empty()

    def BatchStoreKEMs(self, request: glm_service_pb2.BatchStoreKEMsRequest, context) -> glm_service_pb2.BatchStoreKEMsResponse:
        logger.info(f"BatchStoreKEMs: Received {len(request.kems)} KEMs for storage.")

        successfully_stored_kems_final_protos: typing.List[kem_pb2.KEM] = []
        failed_kem_original_references: typing.List[str] = []

        # Pre-process and validate incoming KEMs
        valid_kems_for_processing: typing.List[typing.Dict[str,typing.Any]] = []
        for idx, kem_in_req in enumerate(request.kems):
            original_ref = kem_in_req.id if kem_in_req.id else f"req_idx_{idx}"
            kem_id = kem_in_req.id if kem_in_req.id else str(uuid.uuid4())

            if kem_in_req.embeddings:
                if not self.qdrant_repo:
                    logger.error(f"BatchStoreKEMs: Qdrant repo not available. KEM ID '{kem_id}' (ref: {original_ref}) with embeddings cannot be stored.")
                    failed_kem_original_references.append(original_ref); continue
                if len(kem_in_req.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                    logger.error(f"BatchStoreKEMs: Invalid embedding dimension for KEM ID '{kem_id}' (ref: {original_ref}). Expected {self.config.DEFAULT_VECTOR_SIZE}.")
                    failed_kem_original_references.append(original_ref); continue

            # Store a mutable copy of the proto for timestamp updates
            kem_proto_copy = kem_pb2.KEM(); kem_proto_copy.CopyFrom(kem_in_req)
            kem_proto_copy.id = kem_id # Ensure ID is set
            valid_kems_for_processing.append({"proto": kem_proto_copy, "original_ref": original_ref})

        if not valid_kems_for_processing: # All KEMs failed initial validation
            return glm_service_pb2.BatchStoreKEMsResponse(
                successfully_stored_kems=[], failed_kem_references=list(set(failed_kem_original_references)),
                overall_error_message="No valid KEMs to process after initial validation." if request.kems else "Received an empty list of KEMs to store."
            )

        # Determine created_at timestamps for SQLite batch
        ids_to_check_in_db = [item['proto'].id for item in valid_kems_for_processing if item['proto'].id]
        db_created_at_map = self.sqlite_repo.get_kems_creation_timestamps(ids_to_check_in_db) if ids_to_check_in_db else {}

        sqlite_batch_data = []
        # This map will hold protos with finalized timestamps, to be used for Qdrant payload and final response
        kems_with_final_timestamps: typing.Dict[str, kem_pb2.KEM] = {}

        for item in valid_kems_for_processing:
            kem_p = item['proto']
            current_ts = Timestamp(); current_ts.GetCurrentTime()
            final_created_ts = Timestamp()

            if kem_p.id in db_created_at_map:
                db_ts_str = db_created_at_map[kem_p.id]
                try:
                    parse_ts_str_b = db_ts_str + ("Z" if not db_ts_str.endswith("Z") and '+' not in db_ts_str and '-' not in db_ts_str[10:] else "")
                    final_created_ts.FromJsonString(parse_ts_str_b)
                except Exception: final_created_ts.CopyFrom(current_ts)
            elif kem_p.HasField("created_at") and kem_p.created_at.seconds > 0:
                final_created_at.CopyFrom(kem_p.created_at)
            else:
                final_created_at.CopyFrom(current_ts)

            kem_p.created_at.CopyFrom(final_created_at)
            kem_p.updated_at.CopyFrom(current_ts)

            sqlite_batch_data.append({
                "id": kem_p.id, "content_type": kem_p.content_type, "content": kem_p.content,
                "metadata_json": json.dumps(dict(kem_p.metadata)),
                "created_at_iso": kem_p.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                "updated_at_iso": kem_p.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
            })
            kems_with_final_timestamps[kem_p.id] = kem_p

        # Perform SQLite batch store
        sqlite_op_ok = False
        if sqlite_batch_data:
            try:
                self.sqlite_repo.batch_store_or_replace_kems(sqlite_batch_data)
                sqlite_op_ok = True
                logger.info(f"BatchStoreKEMs: SQLite batch store successful for {len(sqlite_batch_data)} KEMs.")
            except Exception as e_sql_batch:
                logger.error(f"BatchStoreKEMs: SQLite batch store operation failed: {e_sql_batch}", exc_info=True)
                # All KEMs in this batch are considered failed for SQLite
                for item in valid_kems_for_processing: # Add all original refs from this batch attempt
                    if item['original_ref'] not in failed_kem_original_references:
                        failed_kem_original_references.append(item['original_ref'])

        if not sqlite_op_ok and sqlite_batch_data: # If batch data was present but op failed
             return glm_service_pb2.BatchStoreKEMsResponse(
                successfully_stored_kems=[], failed_kem_references=list(set(failed_kem_original_references)),
                overall_error_message="SQLite batch persistence failed for all processed KEMs."
            )

        # Prepare and execute Qdrant batch operation for successfully stored SQLite KEMs
        qdrant_batch_points_to_upsert: List[PointStruct] = []
        ids_for_qdrant_rollback = [] # KEMs with embeddings that went into Qdrant batch

        if self.qdrant_repo and sqlite_op_ok: # Only if SQLite was ok and Qdrant is available
            for kem_id_q, kem_p_q in kems_with_final_timestamps.items():
                # Check if this KEM was part of the successful SQLite batch
                # This check is implicitly handled if sqlite_op_ok is True and we iterate kems_with_final_timestamps
                # (assuming kems_with_final_timestamps only contains those intended for the successful batch)
                if kem_p_q.embeddings:
                    q_payload_item = {"kem_id_ref": kem_id_q}
                    if kem_p_q.metadata:
                        for k_meta, v_meta in kem_p_q.metadata.items(): q_payload_item[f"md_{k_meta}"] = v_meta
                    if kem_p_q.HasField("created_at"): q_payload_item["created_at_ts"] = kem_p_q.created_at.seconds
                    if kem_p_q.HasField("updated_at"): q_payload_item["updated_at_ts"] = kem_p_q.updated_at.seconds
                    qdrant_batch_points_to_upsert.append(PointStruct(id=kem_id_q, vector=list(kem_p_q.embeddings), payload=q_payload_item))
                    ids_for_qdrant_rollback.append(kem_id_q)

            qdrant_batch_op_ok = True
            if qdrant_batch_points_to_upsert:
                try:
                    self.qdrant_repo.upsert_points_batch(qdrant_batch_points_to_upsert)
                except Exception as e_q_batch_up:
                    logger.error(f"BatchStoreKEMs: Qdrant batch upsert error: {e_q_batch_up}", exc_info=True)
                    qdrant_batch_op_ok = False

            if not qdrant_batch_op_ok: # Qdrant failed, rollback SQLite for these KEMs
                logger.warning("BatchStoreKEMs: Qdrant batch op failed. Rolling back SQLite for KEMs in this Qdrant batch.")
                for r_id in ids_for_qdrant_rollback:
                    original_ref_rb_q = ""
                    for item_orig_q_rb in valid_kems_for_processing:
                        if item_orig_q_rb['proto'].id == r_id: original_ref_rb_q = item_orig_q_rb['original_ref']; break
                    if original_ref_rb_q and original_ref_rb_q not in failed_kem_original_references:
                        failed_kem_original_references.append(original_ref_rb_q)

                    # Also remove from kems_with_final_timestamps to prevent it from being added to successful list
                    if r_id in kems_with_final_timestamps:
                        del kems_with_final_timestamps[r_id]
                    try:
                        self.sqlite_repo.delete_kem_by_id(r_id)
                        logger.info(f"BatchStoreKEMs: Rolled back KEM ID '{r_id}' from SQLite due to Qdrant error.")
                    except Exception as e_rb_sql_q:
                        logger.critical(f"BatchStoreKEMs: CRITICAL error during SQLite rollback for KEM '{r_id}': {e_rb_sql_q}", exc_info=True)

        # Populate final successful list from kems_with_final_timestamps (which now only contains fully successful ones)
        for kem_p_final_succ in kems_with_final_timestamps.values():
            successfully_stored_kems_protos.append(kem_p_final_succ)

        final_failed_refs_list = list(set(failed_kem_original_references))
        response = glm_service_pb2.BatchStoreKEMsResponse(
            successfully_stored_kems=successfully_stored_kems_protos,
            failed_kem_references=final_failed_refs_list
        )
        if final_failed_refs_list:
            response.overall_error_message = f"Failed to fully store {len(final_failed_refs_list)} KEMs from the batch."
        elif not request.kems and not successfully_stored_kems_protos:
            response.overall_error_message = "Received an empty list of KEMs to store."

        logger.info(f"BatchStoreKEMs: Processed. Success: {len(successfully_stored_kems_protos)}, Failures: {len(final_failed_refs_list)}.")
        return response

def serve():
    logger.info(f"GLM Configuration: Qdrant Host={config.QDRANT_HOST}, Qdrant Port={config.QDRANT_PORT}, Collection='{config.QDRANT_COLLECTION}', "
                f"SQLite DB='{os.path.join(app_dir, config.DB_FILENAME)}', gRPC Address={config.GRPC_LISTEN_ADDRESS}, LogLevel={config.LOG_LEVEL}")

    # Perform pre-flight check for Qdrant if configured
    if config.QDRANT_HOST:
        try:
            client_test = QdrantClient(host=config.QDRANT_HOST, port=config.QDRANT_PORT, timeout=2)
            client_test.get_collections()
            logger.info(f"Qdrant pre-flight check: Successfully connected to Qdrant at {config.QDRANT_HOST}:{config.QDRANT_PORT}.")
        except Exception as e_preflight:
            logger.critical(f"CRITICAL ERROR: Qdrant pre-flight check failed. Qdrant is unavailable at {config.QDRANT_HOST}:{config.QDRANT_PORT}. Details: {e_preflight}. Server NOT STARTED if Qdrant is required by config/logic.")
            # Depending on policy, we might not start if Qdrant is essential.
            # For now, GLM servicer __init__ will also try and might log errors or raise.
            # If QDRANT_HOST is set, and it fails here, it's a strong indicator of problems.
            # Consider exiting if QDRANT_HOST is set but connection fails.
            # For now, allow __init__ to make the final decision on raising or running degraded.
            pass # Let constructor handle it.

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    try:
        servicer_instance = GlobalLongTermMemoryServicerImpl()
    except Exception as e_servicer_init:
        logger.critical(f"CRITICAL ERROR initializing GlobalLongTermMemoryServicerImpl: {e_servicer_init}", exc_info=True)
        return # Do not start server if servicer init fails

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
        server.stop(grace=5)
        logger.info("GLM server stopped.")

if __name__ == '__main__':
    serve()
```
