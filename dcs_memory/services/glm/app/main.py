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

from .config import GLMConfig
from .repositories import SqliteKemRepository, QdrantKemRepository

current_script_path = os.path.abspath(__file__)
app_dir = os.path.dirname(current_script_path)

config = GLMConfig()

from pythonjsonlogger import jsonlogger # Import for JSON logging

# Centralized logging setup
def setup_logging(log_config: GLMConfig):
    handlers_list = []

    # Determine formatter based on output mode
    if log_config.LOG_OUTPUT_MODE in ["json_stdout", "json_file"]:
        # For JSON, LOG_FORMAT can define the fields. A common practice:
        # Example: "(asctime) (levelname) (name) (module) (funcName) (lineno) (message)"
        # These will become keys in the JSON log.
        # If LOG_FORMAT is the default text one, JsonFormatter will still work but might produce less structured JSON.
        # It's better if LOG_FORMAT is tailored for JSON keys when using JsonFormatter.
        # For simplicity, we can use a default JSON format string or make LOG_FORMAT more flexible.
        # Let's assume LOG_FORMAT is suitable or use a generic set of fields for JSON.
        # The `python-json-logger` adds default fields like 'asctime', 'levelname', 'message'.
        # The format string for JsonFormatter specifies *additional* fields from the LogRecord.
        json_fmt_str = getattr(log_config, 'LOG_JSON_FORMAT', log_config.LOG_FORMAT) # Could add LOG_JSON_FORMAT to config
        formatter = jsonlogger.JsonFormatter(fmt=json_fmt_str, datefmt=log_config.LOG_DATE_FORMAT)
    else: # For "stdout", "file"
        formatter = logging.Formatter(fmt=log_config.LOG_FORMAT, datefmt=log_config.LOG_DATE_FORMAT)

    if log_config.LOG_OUTPUT_MODE in ["stdout", "json_stdout"]:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        handlers_list.append(stream_handler)

    if log_config.LOG_OUTPUT_MODE in ["file", "json_file"]:
        if log_config.LOG_FILE_PATH:
            # TODO: Ensure directory for LOG_FILE_PATH exists or handle creation/permissions.
            try:
                file_handler = logging.FileHandler(log_config.LOG_FILE_PATH)
                file_handler.setFormatter(formatter)
                handlers_list.append(file_handler)
            except Exception as e:
                # Fallback to stdout if file handler fails
                print(f"Error setting up file logger at {log_config.LOG_FILE_PATH}: {e}. Falling back to stdout.", file=sys.stderr)
                if not any(isinstance(h, logging.StreamHandler) for h in handlers_list): # Avoid duplicate stdout
                    stream_handler_fallback = logging.StreamHandler(sys.stdout)
                    stream_handler_fallback.setFormatter(formatter)
                    handlers_list.append(stream_handler_fallback)
        else:
            print(f"LOG_OUTPUT_MODE is '{log_config.LOG_OUTPUT_MODE}' but LOG_FILE_PATH is not set. Defaulting to stdout if no other handler.", file=sys.stderr)
            if not handlers_list: # If no handlers configured yet (e.g. only file mode was chosen but path was missing)
                stream_handler_default = logging.StreamHandler(sys.stdout)
                stream_handler_default.setFormatter(formatter)
                handlers_list.append(stream_handler_default)

    if not handlers_list: # Ultimate fallback if somehow no handlers were added
        print("Warning: No logging handlers configured. Defaulting to basic stdout.", file=sys.stderr)
        logging.basicConfig(level=log_config.get_log_level_int())
        return

    logging.basicConfig(
        level=log_config.get_log_level_int(),
        # format and datefmt in basicConfig are not used when handlers are specified,
        # as handlers have their own formatters.
        handlers=handlers_list,
        force=True # Override any existing basicConfig
    )
    # Ensure all loggers obtained via getLogger propagate to the handlers set by basicConfig.
    # The level set on basicConfig (root logger) will be the effective level unless child loggers override it.

setup_logging(config)
logger = logging.getLogger(__name__) # Get a logger specific to this module

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2
from generated_grpc import glm_service_pb2_grpc
from google.protobuf import empty_pb2

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
                qdrant_client = QdrantClient(
                    host=self.config.QDRANT_HOST,
                    port=self.config.QDRANT_PORT,
                    timeout=self.config.QDRANT_CLIENT_TIMEOUT_S # Use configured timeout
                )
                qdrant_client.get_collections() # Test connection
                logger.info(f"Qdrant client successfully connected to {self.config.QDRANT_HOST}:{self.config.QDRANT_PORT} with timeout {self.config.QDRANT_CLIENT_TIMEOUT_S}s.")

                self.qdrant_repo = QdrantKemRepository(
                    qdrant_client=qdrant_client,
                    collection_name=self.config.QDRANT_COLLECTION,
                    default_vector_size=self.config.DEFAULT_VECTOR_SIZE,
                    default_distance_metric=self.config.QDRANT_DEFAULT_DISTANCE_METRIC # Pass new config
                )
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
        start_time = time.monotonic()
        kem = request.kem
        kem_id = kem.id if kem.id else str(uuid.uuid4())
        has_embeddings = kem.embeddings is not None and len(kem.embeddings) > 0
        logger.info(f"StoreKEM: Started. KEM_ID='{kem_id}' (client-ID='{request.kem.id}'), Embeds={has_embeddings}.")

        current_time_proto = Timestamp(); current_time_proto.GetCurrentTime()
        kem.id = kem_id

        final_created_at_proto = Timestamp()
        existing_created_at_str = self.sqlite_repo.get_kem_creation_timestamp(kem_id)

        if existing_created_at_str:
            try:
                parse_str = existing_created_at_str + ("Z" if not existing_created_at_str.endswith("Z") and '+' not in existing_created_at_str and '-' not in existing_created_at_str[10:] else "")
                final_created_at_proto.FromJsonString(parse_str)
            except Exception as e_parse:
                 logger.error(f"StoreKEM: Error parsing DB created_at '{existing_created_at_str}' for KEM_ID '{kem_id}': {e_parse}", exc_info=True)
                 final_created_at_proto.CopyFrom(current_time_proto)
        elif kem.HasField("created_at") and kem.created_at.seconds > 0:
            final_created_at_proto.CopyFrom(kem.created_at)
        else:
            final_created_at_proto.CopyFrom(current_time_proto)

        kem.created_at.CopyFrom(final_created_at_proto)
        kem.updated_at.CopyFrom(current_time_proto)

        if self.qdrant_repo and has_embeddings:
            if len(kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                msg = f"Embedding dimension ({len(kem.embeddings)}) != Qdrant config ({self.config.DEFAULT_VECTOR_SIZE})."
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return glm_service_pb2.StoreKEMResponse()

            qdrant_payload = {"kem_id_ref": kem_id}
            if kem.metadata:
                for k, v_pb_val in kem.metadata.items(): qdrant_payload[f"md_{k}"] = v_pb_val
            if kem.HasField("created_at"): qdrant_payload["created_at_ts"] = kem.created_at.seconds
            if kem.HasField("updated_at"): qdrant_payload["updated_at_ts"] = kem.updated_at.seconds

            try:
                point = PointStruct(id=kem_id, vector=list(kem.embeddings), payload=qdrant_payload)
                self.qdrant_repo.upsert_point(point)
            except Exception as e_qdrant:
                msg = f"StoreKEM: Qdrant error for ID '{kem_id}': {e_qdrant}"
                logger.error(msg, exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, msg); return glm_service_pb2.StoreKEMResponse()

        try:
            self.sqlite_repo.store_or_replace_kem(
                kem_id=kem.id, content_type=kem.content_type, content=kem.content,
                metadata_json=json.dumps(dict(kem.metadata)),
                created_at_iso=kem.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                updated_at_iso=kem.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
            )
        except Exception as e_sqlite:
            msg = f"StoreKEM: SQLite error for ID '{kem_id}' after Qdrant upsert (if performed): {e_sqlite}"
            logger.error(msg, exc_info=True)
            # Attempt to roll back Qdrant change if it happened
            if self.qdrant_repo and has_embeddings:
                try:
                    logger.warning(f"StoreKEM: Attempting to roll back Qdrant upsert for KEM ID '{kem_id}' due to SQLite error.")
                    self.qdrant_repo.delete_points_by_ids([kem_id])
                    logger.info(f"StoreKEM: Qdrant rollback successful for KEM ID '{kem_id}'.")
                except Exception as e_qdrant_rollback:
                    logger.error(f"StoreKEM: CRITICAL - Failed to roll back Qdrant upsert for KEM ID '{kem_id}': {e_qdrant_rollback}", exc_info=True)
                    # This KEM might be orphaned in Qdrant or in an inconsistent state.
            context.abort(grpc.StatusCode.INTERNAL, msg); return glm_service_pb2.StoreKEMResponse()

        duration = time.monotonic() - start_time
        logger.info(f"StoreKEM: Finished. KEM_ID='{kem_id}'. Duration: {duration:.4f}s.")
        return glm_service_pb2.StoreKEMResponse(kem=kem)

    def RetrieveKEMs(self, request: glm_service_pb2.RetrieveKEMsRequest, context) -> glm_service_pb2.RetrieveKEMsResponse:
        start_time = time.monotonic()
        query = request.query
        page_size = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
        offset = 0
        if request.page_token:
            try: offset = int(request.page_token); offset = max(0, offset)
            except ValueError: logger.warning(f"Invalid page_token: '{request.page_token}'. Using 0.")

        is_vec_q = bool(query.embedding_query)
        logger.info(f"RetrieveKEMs: Started. Type={'Vector' if is_vec_q else 'Filtered'}, "
                    f"Filters(IDs:{len(query.ids)>0}, Meta:{len(query.metadata_filters)>0}, Date:{query.HasField('created_at_start') or query.HasField('created_at_end') or query.HasField('updated_at_start') or query.HasField('updated_at_end')}), "
                    f"PageSize:{page_size}, Offset:{offset}.")

        kems_protos: typing.List[kem_pb2.KEM] = []
        next_token = ""

        if is_vec_q:
            if not self.qdrant_repo:
                context.abort(grpc.StatusCode.INTERNAL, "Qdrant not available for vector search."); return glm_service_pb2.RetrieveKEMsResponse()
            if len(query.embedding_query) != self.config.DEFAULT_VECTOR_SIZE:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid vector dim."); return glm_service_pb2.RetrieveKEMsResponse()

            try:
                q_conds = []
                if query.ids: q_conds.append(models.HasIdCondition(has_id=list(query.ids)))
                if query.metadata_filters:
                    for k, v in query.metadata_filters.items(): q_conds.append(models.FieldCondition(key=f"md_{k}", match=models.MatchValue(value=v)))
                def add_ts_q(fname, ts_s, ts_e, c):
                    gte, lte = (ts_s.seconds if ts_s.seconds > 0 or ts_s.nanos > 0 else None), (ts_e.seconds if ts_e.seconds > 0 or ts_e.nanos > 0 else None)
                    if gte is not None or lte is not None: c.append(models.FieldCondition(key=fname, range=models.Range(gte=gte, lte=lte)))
                add_ts_q("created_at_ts", query.created_at_start, query.created_at_end, q_conds)
                add_ts_q("updated_at_ts", query.updated_at_start, query.updated_at_end, q_conds)
                q_filter = models.Filter(must=q_conds) if q_conds else None

                hits = self.qdrant_repo.search_points(list(query.embedding_query), q_filter, page_size, offset, True)
                if not hits: return glm_service_pb2.RetrieveKEMsResponse()

                hit_ids = [h.id for h in hits]
                embed_map = {h.id: list(h.vector) for h in hits if h.vector}
                order_clause = f"ORDER BY INSTR('," + ",".join(hit_ids) + ",', ',' || id || ',')"
                db_dicts, _ = self.sqlite_repo.retrieve_kems_from_db([f"id IN ({','.join('?' for _ in hit_ids)})"], hit_ids, len(hit_ids), 0, order_clause)

                for db_d in db_dicts: kems_protos.append(self._kem_from_db_dict(db_d, embed_map))
                if len(hits) == page_size: next_token = str(offset + page_size)
            except Exception as e_vec:
                logger.error(f"RetrieveKEMs Vector Search Error: {e_vec}", exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"Vector search error: {e_vec}"); return glm_service_pb2.RetrieveKEMsResponse()
        else: # Filtered search
            sql_c, sql_p = [], []
            if query.ids: sql_c.append(f"id IN ({','.join('?' for _ in query.ids)})"); sql_p.extend(list(query.ids))
            for k, v in query.metadata_filters.items(): sql_c.append(f"json_extract(metadata, '$.{k}') = ?"); sql_p.append(v)
            def add_date_sql(fname, ts_s, ts_e, c, p):
                if ts_s.seconds > 0 or ts_s.nanos > 0: c.append(f"{fname} >= ?"); p.append(ts_s.ToDatetime().isoformat(timespec='seconds').replace('+00:00',''))
                if ts_e.seconds > 0 or ts_e.nanos > 0: c.append(f"{fname} <= ?"); p.append(ts_e.ToDatetime().isoformat(timespec='seconds').replace('+00:00',''))
            add_date_sql("created_at", query.created_at_start, query.created_at_end, sql_c, sql_p)
            add_date_sql("updated_at", query.updated_at_start, query.updated_at_end, sql_c, sql_p)
            try:
                db_dicts, next_token = self.sqlite_repo.retrieve_kems_from_db(sql_c, sql_p, page_size, offset)
                ids_pg = [d['id'] for d in db_dicts]
                embed_map_pg = {}
                if ids_pg and self.qdrant_repo:
                    try:
                        q_pts = self.qdrant_repo.retrieve_points_by_ids(ids_pg, True)
                        for p in q_pts:
                            if p.vector: embed_map_pg[p.id] = list(p.vector)
                    except Exception as e_q_emb_f: logger.warning(f"Failed to get embeddings for filtered search: {e_q_emb_f}", exc_info=True)
                for db_d in db_dicts: kems_protos.append(self._kem_from_db_dict(db_d, embed_map_pg))
            except Exception as e_filt:
                logger.error(f"RetrieveKEMs Filtered Search Error: {e_filt}", exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"Filtered search error: {e_filt}"); return glm_service_pb2.RetrieveKEMsResponse()

        duration = time.monotonic() - start_time
        logger.info(f"RetrieveKEMs: Finished. Found {len(kems_protos)} KEMs. NextToken: '{next_token}'. Duration: {duration:.4f}s.")
        return glm_service_pb2.RetrieveKEMsResponse(kems=kems_protos, next_page_token=next_token)

    def UpdateKEM(self, request: glm_service_pb2.UpdateKEMRequest, context) -> kem_pb2.KEM:
        start_time = time.monotonic()
        kem_id = request.kem_id
        kem_data_update = request.kem_data_update
        if not kem_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID required."); return kem_pb2.KEM()
        logger.info(f"UpdateKEM: Started. KEM_ID='{kem_id}', HasEmbeddingsInUpdate={bool(kem_data_update.embeddings)}.")

        current_db_kem = self.sqlite_repo.get_full_kem_by_id(kem_id)
        if not current_db_kem: context.abort(grpc.StatusCode.NOT_FOUND, f"KEM ID '{kem_id}' not found."); return kem_pb2.KEM()

        ts_now = Timestamp(); ts_now.GetCurrentTime()
        q_payload = {"kem_id_ref": kem_id}
        current_meta = json.loads(current_db_kem.get('metadata', '{}'))
        meta_for_q = current_meta.copy()
        if kem_data_update.metadata: meta_for_q = dict(kem_data_update.metadata)
        for k, v in meta_for_q.items(): q_payload[f"md_{k}"] = str(v)

        try:
            orig_cr_at_iso = current_db_kem['created_at']
            cr_at_ts_q = Timestamp(); cr_at_ts_q.FromJsonString(orig_cr_at_iso + ("Z" if not orig_cr_at_iso.endswith("Z") and 'T' in orig_cr_at_iso else ""))
            q_payload["created_at_ts"] = cr_at_ts_q.seconds
        except: pass # Ignore if parse fails
        q_payload["updated_at_ts"] = ts_now.seconds

        final_embeds_resp = []
        if self.qdrant_repo:
            try:
                q_point: PointStruct
                if kem_data_update.embeddings:
                    new_embeds_list = list(kem_data_update.embeddings)
                    if len(new_embeds_list) != self.config.DEFAULT_VECTOR_SIZE:
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid embedding dim."); return kem_pb2.KEM()
                    q_point = PointStruct(id=kem_id, vector=new_embeds_list, payload=q_payload)
                    final_embeds_resp = new_embeds_list
                else:
                    ex_pts = self.qdrant_repo.retrieve_points_by_ids([kem_id], True)
                    ex_vec = list(ex_pts[0].vector) if ex_pts and ex_pts[0].vector else None
                    final_embeds_resp = ex_vec if ex_vec else []
                    q_point = PointStruct(id=kem_id, vector=ex_vec, payload=q_payload)
                self.qdrant_repo.upsert_point(q_point)
            except Exception as e_q_upd:
                context.abort(grpc.StatusCode.INTERNAL, f"Qdrant update error: {e_q_upd}"); return kem_pb2.KEM()

        sql_upd_at_iso = ts_now.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
        sql_ct = kem_data_update.content_type if kem_data_update.HasField("content_type") else None
        sql_c = kem_data_update.content.value if kem_data_update.HasField("content") else None
        sql_meta_json = json.dumps(dict(kem_data_update.metadata)) if kem_data_update.metadata else None

        qdrant_updated_for_this_call = False # Flag to track if Qdrant was touched in this specific UpdateKEM call
        if self.qdrant_repo:
            # This block was already here for Qdrant update
            try:
                q_point: PointStruct
                if kem_data_update.embeddings: # Embeddings are explicitly being updated
                    new_embeds_list = list(kem_data_update.embeddings)
                    if len(new_embeds_list) != self.config.DEFAULT_VECTOR_SIZE:
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid embedding dim."); return kem_pb2.KEM()
                    q_point = PointStruct(id=kem_id, vector=new_embeds_list, payload=q_payload)
                    final_embeds_resp = new_embeds_list
                else: # Embeddings not in update, preserve existing if any, update payload
                    ex_pts = self.qdrant_repo.retrieve_points_by_ids([kem_id], True)
                    ex_vec = list(ex_pts[0].vector) if ex_pts and ex_pts[0].vector else None
                    final_embeds_resp = ex_vec if ex_vec else []
                    # Only create a point if there's a vector or if we intend to update payload even for no-vector points
                    if ex_vec or kem_data_update.metadata: # If metadata is updated, payload needs update
                         q_point = PointStruct(id=kem_id, vector=ex_vec, payload=q_payload)
                    else: # No embeddings and no metadata update, no Qdrant point op needed if point doesn't exist or has no vector
                         q_point = None

                if q_point:
                    self.qdrant_repo.upsert_point(q_point)
                    qdrant_updated_for_this_call = True
            except Exception as e_q_upd:
                # If Qdrant fails here, we haven't touched SQLite yet for this update.
                # So, state is still consistent with before this UpdateKEM call.
                context.abort(grpc.StatusCode.INTERNAL, f"Qdrant update error: {e_q_upd}"); return kem_pb2.KEM()

        try:
            update_success = self.sqlite_repo.update_kem_fields(kem_id, sql_upd_at_iso, sql_ct, sql_c, sql_meta_json)
            if not update_success and (sql_ct or sql_c or sql_meta_json): # If we intended to update fields but nothing changed
                 logger.warning(f"UpdateKEM: SQLite update_kem_fields reported no rows affected for KEM ID '{kem_id}' despite update data.")
                 # This might indicate KEM was deleted between initial fetch and update, or a concurrent update changed it.
                 # Or simply no actual changes were made by the provided data.
                 # For now, we proceed, but this could be a point for stricter checks.
        except Exception as e_sql_upd:
            logger.error(f"UpdateKEM: SQLite error for ID '{kem_id}' after Qdrant update (if performed): {e_sql_upd}", exc_info=True)
            # Attempt to roll back Qdrant change if it happened in *this specific call*
            if qdrant_updated_for_this_call and self.qdrant_repo:
                try:
                    logger.warning(f"UpdateKEM: Attempting to roll back Qdrant update for KEM ID '{kem_id}' due to SQLite error.")
                    # This is tricky: rolling back Qdrant means restoring its *previous state*.
                    # The current q_payload might be based on the *intended new metadata*.
                    # A true rollback would require fetching the KEM's Qdrant point *before* this update.
                    # For simplicity now, if an update involved embeddings, we might delete the point.
                    # If only payload, restoring previous payload is complex.
                    # A safer, simpler rollback for now if Qdrant was touched: re-fetch from SQLite (which failed to update)
                    # and try to re-sync Qdrant to that older SQLite state.
                    # Or, if the update was primarily payload, and embeddings were just preserved,
                    # try to re-upsert with the *old* metadata from current_db_kem.
                    # This is getting complex. Simplest rollback: delete if new embeddings were pushed.
                    # If only payload was updated, a Qdrant delete might be too destructive if the point should exist.
                    #
                    # Let's consider the case: if `kem_data_update.embeddings` were provided, then Qdrant point was definitely changed with new vector.
                    # If only metadata changed, Qdrant payload changed.
                    # A simple strategy: if Qdrant was touched and SQLite failed, we log a CRITICAL inconsistency.
                    # A true rollback of Qdrant to its exact previous state is hard without storing that state.
                    logger.critical(f"UpdateKEM: CRITICAL INCONSISTENCY - SQLite update failed for KEM ID '{kem_id}' after Qdrant was updated. Manual reconciliation may be needed.")
                    # For now, we won't attempt complex Qdrant rollback here to avoid further errors.
                except Exception as e_qdrant_rollback:
                    logger.error(f"UpdateKEM: Error during Qdrant rollback attempt for KEM ID '{kem_id}': {e_qdrant_rollback}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"SQLite update error: {e_sql_upd}"); return kem_pb2.KEM()

        final_db_kem = self.sqlite_repo.get_full_kem_by_id(kem_id)
        if not final_db_kem: # Should not happen if update_kem_fields didn't delete
            logger.error(f"UpdateKEM: KEM ID '{kem_id}' disappeared after SQLite update attempt. This should not happen.")
            context.abort(grpc.StatusCode.INTERNAL, "KEM disappeared post-update."); return kem_pb2.KEM()

        # Ensure final_embeds_resp reflects the latest state (especially if no new embeddings were provided in update)
        if not kem_data_update.embeddings and self.qdrant_repo: # If embeddings were not part of the update payload
            retrieved_pts = self.qdrant_repo.retrieve_points_by_ids([kem_id], with_vectors=True)
            if retrieved_pts and retrieved_pts[0].vector:
                final_embeds_resp = list(retrieved_pts[0].vector)
            else:
                final_embeds_resp = [] # Ensure it's an empty list if no vector found

        resp_kem = self._kem_from_db_dict(final_db_kem, {kem_id: final_embeds_resp} if final_embeds_resp else None)
        duration = time.monotonic() - start_time
        logger.info(f"UpdateKEM: Finished. KEM_ID='{kem_id}'. Duration: {duration:.4f}s.")
        return resp_kem

    def DeleteKEM(self, request: glm_service_pb2.DeleteKEMRequest, context) -> empty_pb2.Empty:
        start_time = time.monotonic()
        kem_id = request.kem_id
        if not kem_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID required."); return empty_pb2.Empty()
        logger.info(f"DeleteKEM: Started. KEM_ID='{kem_id}'.")

        if self.qdrant_repo:
            try: self.qdrant_repo.delete_points_by_ids([kem_id])
            except Exception as e_qd_del: context.abort(grpc.StatusCode.INTERNAL, f"Qdrant delete error: {e_qd_del}"); return empty_pb2.Empty()
        try:
            self.sqlite_repo.delete_kem_by_id(kem_id)
        except Exception as e_sql_del: context.abort(grpc.StatusCode.INTERNAL, f"SQLite delete error: {e_sql_del}"); return empty_pb2.Empty()

        duration = time.monotonic() - start_time
        logger.info(f"DeleteKEM: Finished. KEM_ID='{kem_id}'. Duration: {duration:.4f}s.")
        return empty_pb2.Empty()

    def BatchStoreKEMs(self, request: glm_service_pb2.BatchStoreKEMsRequest, context) -> glm_service_pb2.BatchStoreKEMsResponse:
        start_time = time.monotonic()
        num_req_kems = len(request.kems)
        logger.info(f"BatchStoreKEMs: Started. Received {num_req_kems} KEMs.")

        success_protos: typing.List[kem_pb2.KEM] = []
        failed_refs: typing.List[str] = []

        valid_for_processing: typing.List[typing.Dict[str,typing.Any]] = []
        for idx, req_kem in enumerate(request.kems):
            orig_ref = req_kem.id if req_kem.id else f"req_idx_{idx}"
            kem_ident = req_kem.id if req_kem.id else str(uuid.uuid4())
            if req_kem.embeddings and self.qdrant_repo:
                if len(req_kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                    logger.error(f"Batch: Invalid embed dim for KEM ID '{kem_ident}' (ref: {orig_ref}).")
                    failed_refs.append(orig_ref); continue
            elif req_kem.embeddings and not self.qdrant_repo:
                 logger.error(f"Batch: Qdrant repo N/A. KEM ID '{kem_ident}' (ref: {orig_ref}) with embeds skipped.")
                 failed_refs.append(orig_ref); continue

            kem_p_copy = kem_pb2.KEM(); kem_p_copy.CopyFrom(req_kem); kem_p_copy.id = kem_ident
            valid_for_processing.append({"proto": kem_p_copy, "original_ref": orig_ref})

        if not valid_for_processing:
            return glm_service_pb2.BatchStoreKEMsResponse(failed_kem_references=list(set(failed_refs)),
                                                        overall_error_message="No valid KEMs after pre-validation." if request.kems else "Empty KEM list.")

        ids_in_batch = [item['proto'].id for item in valid_for_processing if item['proto'].id]
        db_cr_at_map = self.sqlite_repo.get_kems_creation_timestamps(ids_in_batch) if ids_in_batch else {}

        sqlite_data_batch = []
        final_kem_protos_map: typing.Dict[str, kem_pb2.KEM] = {}

        for item in valid_for_processing:
            kem_p = item['proto']; curr_ts = Timestamp(); curr_ts.GetCurrentTime(); final_cr_at = Timestamp()
            if kem_p.id in db_cr_at_map:
                db_ts_str = db_cr_at_map[kem_p.id]
                try:
                    parse_ts_b = db_ts_str + ("Z" if not db_ts_str.endswith("Z") and 'T' in db_ts_str else "")
                    final_cr_at.FromJsonString(parse_ts_b)
                except: final_cr_at.CopyFrom(curr_ts)
            elif kem_p.HasField("created_at") and kem_p.created_at.seconds > 0: final_cr_at.CopyFrom(kem_p.created_at)
            else: final_cr_at.CopyFrom(curr_ts)
            kem_p.created_at.CopyFrom(final_cr_at); kem_p.updated_at.CopyFrom(curr_ts)

            sqlite_data_batch.append({
                "id": kem_p.id, "content_type": kem_p.content_type, "content": kem_p.content,
                "metadata_json": json.dumps(dict(kem_p.metadata)),
                "created_at_iso": kem_p.created_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', ''),
                "updated_at_iso": kem_p.updated_at.ToDatetime().isoformat(timespec='seconds').replace('+00:00', '')
            })
            final_kem_protos_map[kem_p.id] = kem_p

        sqlite_ok = False
        if sqlite_data_batch:
            try: self.sqlite_repo.batch_store_or_replace_kems(sqlite_data_batch); sqlite_ok = True
            except Exception as e_sql_b:
                logger.error(f"BatchStoreKEMs: SQLite batch store failed: {e_sql_b}", exc_info=True)
                for item_f in valid_for_processing: # All in this batch attempt failed SQLite
                    if item_f['original_ref'] not in failed_refs: failed_refs.append(item_f['original_ref'])

        if not sqlite_ok and sqlite_data_batch: # SQLite batch failed for all
            dur_fail = time.monotonic() - start_time
            logger.info(f"BatchStoreKEMs: Finished (SQLite failure). Duration: {dur_fail:.4f}s. Success: 0, Failures: {len(list(set(failed_refs)))}.")
            return glm_service_pb2.BatchStoreKEMsResponse(failed_kem_references=list(set(failed_refs)), overall_error_message="SQLite batch persistence failed.")

        q_batch_pts: List[PointStruct] = []
        ids_for_q_rb_check = []
        if self.qdrant_repo and sqlite_ok:
            for kem_id_q, kem_p_q in final_kem_protos_map.items():
                # Only process KEMs that were part of the successful SQLite batch (implicitly all if sqlite_ok=True)
                if kem_p_q.embeddings:
                    q_pl = {"kem_id_ref": kem_id_q}
                    if kem_p_q.metadata:
                        for k,v in kem_p_q.metadata.items(): q_pl[f"md_{k}"]=v
                    if kem_p_q.HasField("created_at"): q_pl["created_at_ts"]=kem_p_q.created_at.seconds
                    if kem_p_q.HasField("updated_at"): q_pl["updated_at_ts"]=kem_p_q.updated_at.seconds
                    q_batch_pts.append(PointStruct(id=kem_id_q, vector=list(kem_p_q.embeddings), payload=q_pl))
                    ids_for_q_rb_check.append(kem_id_q)

            q_batch_ok = True
            if q_batch_pts:
                try: self.qdrant_repo.upsert_points_batch(q_batch_pts)
                except Exception as e_q_b_up: logger.error(f"Batch: Qdrant batch upsert error: {e_q_b_up}", exc_info=True); q_batch_ok = False

            if not q_batch_ok:
                logger.warning("Batch: Qdrant op failed. Rolling back SQLite for KEMs in Qdrant batch.")
                for r_id_q in ids_for_q_rb_check:
                    orig_ref_q_rb = next((item['original_ref'] for item in valid_for_processing if item['proto'].id == r_id_q), r_id_q)
                    if orig_ref_q_rb not in failed_refs: failed_refs.append(orig_ref_q_rb)
                    if r_id_q in final_kem_protos_map: del final_kem_protos_map[r_id_q] # Remove from success
                    try: self.sqlite_repo.delete_kem_by_id(r_id_q)
                    except Exception as e_rb_sql_q_b: logger.critical(f"Batch: CRIT SQLite rollback error KEM '{r_id_q}': {e_rb_sql_q_b}", exc_info=True)

        for kem_p_final_s in final_kem_protos_map.values(): success_protos.append(kem_p_final_s)

        final_failed_refs_list_unique = list(set(failed_refs))
        resp = glm_service_pb2.BatchStoreKEMsResponse(successfully_stored_kems=success_protos, failed_kem_references=final_failed_refs_list_unique)
        if final_failed_refs_list_unique: resp.overall_error_message = f"Failed to store {len(final_failed_refs_list_unique)} KEMs."
        elif not request.kems and not success_protos: resp.overall_error_message = "Empty KEM list received."

        duration = time.monotonic() - start_time
        logger.info(f"BatchStoreKEMs: Finished. Duration: {duration:.4f}s. Success: {len(success_protos)}, Failures: {len(final_failed_refs_list_unique)}.")
        return resp

def serve():
    logger.info(f"GLM Config: Qdrant={config.QDRANT_HOST}:{config.QDRANT_PORT} ('{config.QDRANT_COLLECTION}'), "
                f"SQLite='{os.path.join(app_dir, config.DB_FILENAME)}', gRPC={config.GRPC_LISTEN_ADDRESS}, LogLvl={config.LOG_LEVEL}")

    if config.QDRANT_HOST: # Pre-flight check only if Qdrant is configured
        try:
            client_test = QdrantClient(
                host=config.QDRANT_HOST,
                port=config.QDRANT_PORT,
                timeout=config.QDRANT_PREFLIGHT_CHECK_TIMEOUT_S # Use configured timeout
            )
            client_test.get_collections()
            logger.info(f"Qdrant pre-flight check OK: {config.QDRANT_HOST}:{config.QDRANT_PORT} (timeout: {config.QDRANT_PREFLIGHT_CHECK_TIMEOUT_S}s).")
        except Exception as e_preflight:
            logger.critical(f"CRITICAL: Qdrant unavailable at {config.QDRANT_HOST}:{config.QDRANT_PORT}. {e_preflight}. Server NOT STARTED if Qdrant is essential.")
            return # Exit if Qdrant is configured but unavailable

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.GRPC_SERVER_MAX_WORKERS)) # Use configured max_workers
    try:
        servicer_instance = GlobalLongTermMemoryServicerImpl()
    except Exception as e_servicer_init:
        logger.critical(f"CRITICAL: Error initializing ServicerImpl: {e_servicer_init}", exc_info=True)
        return

    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(servicer_instance, server)

    # Add Health Servicer
    from grpc_health.v1 import health, health_pb2_grpc
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    # For now, set overall health to SERVING.
    # TODO: Implement more detailed health checks for dependencies (SQLite, Qdrant).
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    # Set specific service health if needed, though "" usually covers the whole server.
    # health_servicer.set(glm_service_pb2.DESCRIPTOR.services_by_name['GlobalLongTermMemory'].full_name, health_pb2.HealthCheckResponse.SERVING)


    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS)
    logger.info(f"Starting GLM server on {config.GRPC_LISTEN_ADDRESS} with health checks enabled...")
    server.start()
    logger.info(f"GLM server started and listening on {config.GRPC_LISTEN_ADDRESS}.")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("GLM server stopping via KeyboardInterrupt...")
    finally:
        server.stop(grace=config.GRPC_SERVER_SHUTDOWN_GRACE_S) # Use configured grace period
        logger.info("GLM server stopped.")

if __name__ == '__main__':
    serve()

```
