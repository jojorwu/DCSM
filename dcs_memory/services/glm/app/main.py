import grpc
from concurrent import futures
import time
import sys
import os
import uuid
import json
import sqlite3
from qdrant_client import QdrantClient, models # Используем models для VectorParams и Distance
from qdrant_client.http.models import PointStruct # PointStruct напрямую
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict # MessageToDict не используется напрямую в текущей версии
import typing
import logging # Добавлен модуль логирования

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout) # Вывод логов в stdout
    ]
)
logger = logging.getLogger(__name__)
# --- Конец настройки логирования ---

# --- Начало блока для корректного импорта сгенерированного кода ---
current_script_path = os.path.abspath(__file__)
app_dir = os.path.dirname(current_script_path)
service_root_dir = os.path.dirname(app_dir)

if service_root_dir not in sys.path:
    sys.path.insert(0, service_root_dir)

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2
from generated_grpc import glm_service_pb2_grpc
from google.protobuf import empty_pb2
# --- Конец блока для корректного импорта ---

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

# --- Конфигурация ---
SQLITE_DB_PATH = os.path.join(app_dir, os.getenv("SQLITE_DB_FILENAME", "glm_metadata.sqlite3"))
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "glm_kems_demo_collection")
DEFAULT_VECTOR_SIZE = int(os.getenv("DEFAULT_VECTOR_SIZE", 25))
DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", 10))
GRPC_LISTEN_ADDRESS = os.getenv("GRPC_LISTEN_ADDRESS", "[::]:50051")
# --- Конец конфигурации ---


class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self):
        logger.info("Инициализация GlobalLongTermMemoryServicerImpl...")
        self.qdrant_client = None
        try:
            self.qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=10)
            # A light operation to check connectivity. get_collections() is more reliable than a custom health_check()
            self.qdrant_client.get_collections()
            logger.info(f"Qdrant клиент успешно подключен к {QDRANT_HOST}:{QDRANT_PORT}")
            self._ensure_qdrant_collection()
        except Exception as e:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при инициализации Qdrant клиента: {e}. Сервис может работать некорректно.")
            self.qdrant_client = None

        self._init_sqlite()
        logger.info("Сервисер GLM инициализирован.")

    def _get_sqlite_conn(self):
        return sqlite3.connect(SQLITE_DB_PATH, timeout=10)

    def _init_sqlite(self):
        logger.info(f"Инициализация SQLite БД по пути: {SQLITE_DB_PATH}")
        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor()
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
                conn.commit()
            logger.info("Таблица 'kems' в SQLite успешно инициализирована.")
        except Exception as e:
            logger.error(f"Ошибка инициализации SQLite: {e}")

    def _ensure_qdrant_collection(self):
        if not self.qdrant_client:
            logger.warning("Qdrant клиент не инициализирован, пропуск создания коллекции.")
            return
        try:
            try:
                collection_info = self.qdrant_client.get_collection(QDRANT_COLLECTION)
                logger.info(f"Коллекция '{QDRANT_COLLECTION}' уже существует.")
                current_vector_size = collection_info.config.params.vectors.size
                current_distance = collection_info.config.params.vectors.distance
                if current_vector_size != DEFAULT_VECTOR_SIZE or current_distance != models.Distance.COSINE:
                     logger.warning(f"Конфигурация существующей коллекции '{QDRANT_COLLECTION}' не совпадает с ожидаемой.")
                else:
                    logger.info(f"Конфигурация коллекции '{QDRANT_COLLECTION}' соответствует.")
            except Exception:
                logger.info(f"Коллекция '{QDRANT_COLLECTION}' не найдена или ошибка доступа. Создание новой коллекции...")
                self.qdrant_client.recreate_collection(
                    collection_name=QDRANT_COLLECTION,
                    vectors_config=models.VectorParams(size=DEFAULT_VECTOR_SIZE, distance=models.Distance.COSINE)
                )
                logger.info(f"Коллекция '{QDRANT_COLLECTION}' успешно создана.")
        except Exception as e:
            logger.error(f"Ошибка при проверке/создании коллекции Qdrant '{QDRANT_COLLECTION}': {e}")

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
        if kem_dict.get('created_at'):
            created_at_str = kem_dict['created_at']
            if not created_at_str.endswith("Z"): created_at_str += "Z"
            created_at_ts.FromJsonString(created_at_str)
        kem_dict['created_at'] = created_at_ts

        updated_at_ts = Timestamp()
        if kem_dict.get('updated_at'):
            updated_at_str = kem_dict['updated_at']
            if not updated_at_str.endswith("Z"): updated_at_str += "Z"
            updated_at_ts.FromJsonString(updated_at_str)
        kem_dict['updated_at'] = updated_at_ts

        if embeddings_map and kem_dict['id'] in embeddings_map:
            kem_dict['embeddings'] = embeddings_map[kem_dict['id']]

        return self._kem_dict_to_proto(kem_dict)

    def StoreKEM(self, request, context):
        kem = request.kem
        kem_id = kem.id if kem.id else str(uuid.uuid4())
        logger.info("StoreKEM: ID='{}' (клиентский ID='{}')".format(kem_id, request.kem.id))

        current_time_proto = Timestamp()
        current_time_proto.GetCurrentTime()

        final_created_at_proto = Timestamp()
        is_new_kem = True

        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT created_at FROM kems WHERE id = ?", (kem_id,))
                existing_row = cursor.fetchone()
                if existing_row:
                    is_new_kem = False
                    final_created_at_proto.FromJsonString(existing_row[0] + "Z")

                if is_new_kem:
                    if kem.HasField("created_at"):
                        final_created_at_proto.CopyFrom(kem.created_at)
                    else:
                        final_created_at_proto.CopyFrom(current_time_proto)

                kem.id = kem_id
                kem.created_at.CopyFrom(final_created_at_proto)
                kem.updated_at.CopyFrom(current_time_proto)

                cursor.execute('''
                INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (kem.id, kem.content_type, kem.content,
                      json.dumps(dict(kem.metadata)),
                      kem.created_at.ToDatetime().isoformat(),
                      kem.updated_at.ToDatetime().isoformat()))
                conn.commit()
            logger.info("Метаданные/контент для КЕП ID '{}' сохранены/обновлены в SQLite.".format(kem_id))
        except Exception as e:
            msg = "Ошибка SQLite (StoreKEM) для ID '{}': {}".format(kem_id, e)
            logger.error(msg, exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, msg); return glm_service_pb2.StoreKEMResponse()

        if self.qdrant_client and kem.embeddings:
            if len(kem.embeddings) != DEFAULT_VECTOR_SIZE:
                msg = "Размерность эмбеддингов ({}) не совпадает с конфигурацией ({}).".format(len(kem.embeddings), DEFAULT_VECTOR_SIZE)
                logger.error(msg)
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return glm_service_pb2.StoreKEMResponse()
            try:
                self.qdrant_client.upsert(
                    collection_name=QDRANT_COLLECTION,
                    points=[PointStruct(id=kem_id, vector=list(kem.embeddings), payload={"kem_id_ref": kem_id})]
                )
                logger.info("Эмбеддинги для КЕП ID '{}' сохранены/обновлены в Qdrant.".format(kem_id))
            except Exception as e:
                msg = "Ошибка Qdrant (StoreKEM) для ID '{}': {}".format(kem_id, e)
                logger.error(msg, exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, msg); return glm_service_pb2.StoreKEMResponse()

        logger.info("КЕП ID '{}' успешно сохранена/обновлена.".format(kem_id))
        return glm_service_pb2.StoreKEMResponse(kem=kem)

    def RetrieveKEMs(self, request, context):
        query = request.query
        page_size = request.page_size if request.page_size > 0 else DEFAULT_PAGE_SIZE
        offset = 0
        if request.page_token:
            try: offset = int(request.page_token)
            except ValueError:
                logger.warning("Неверный формат page_token: '{}'. Используется offset=0.".format(request.page_token))

        logger.info("RetrieveKEMs: query={}, page_size={}, offset={}".format(query, page_size, offset))

        found_kems_proto = []
        next_offset_str = ""
        qdrant_ids_to_filter = None
        embeddings_from_qdrant = {}

        if query.embedding_query:
            if not self.qdrant_client:
                logger.error("Qdrant сервис недоступен для векторного поиска.")
                context.abort(grpc.StatusCode.INTERNAL, "Qdrant сервис недоступен."); return glm_service_pb2.RetrieveKEMsResponse()
            if len(query.embedding_query) != DEFAULT_VECTOR_SIZE:
                msg = "Неверная размерность вектора запроса: {} (ожидается {})".format(len(query.embedding_query), DEFAULT_VECTOR_SIZE)
                logger.error(msg)
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return glm_service_pb2.RetrieveKEMsResponse()
            try:
                qdrant_filter = None # TODO: Implement Qdrant filter from query.metadata_filters, etc.

                search_result = self.qdrant_client.search(
                    collection_name=QDRANT_COLLECTION,
                    query_vector=list(query.embedding_query),
                    query_filter=qdrant_filter,
                    limit=page_size, # Qdrant handles pagination if offset is used
                    offset=offset,
                    with_vectors=True
                )
                qdrant_ids_to_filter = [hit.id for hit in search_result]
                for hit in search_result: embeddings_from_qdrant[hit.id] = list(hit.vector) if hit.vector else []

                if not qdrant_ids_to_filter:
                    logger.info("Qdrant не нашел кандидатов по вектору.")
                    return glm_service_pb2.RetrieveKEMsResponse(kems=[], next_page_token="")
                logger.info("Qdrant нашел {} кандидатов по вектору.".format(len(qdrant_ids_to_filter)))

                # If Qdrant returned results, fetch full data from SQLite using these IDs
                placeholders = ','.join('?' for _ in qdrant_ids_to_filter)
                sql_query_base = f"SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id IN ({placeholders})"
                # Preserve Qdrant's relevance order
                sql_query_final = sql_query_base + f" ORDER BY INSTR(',' || ({placeholders}) || ',', ',' || id || ',')"
                sql_params = qdrant_ids_to_filter + qdrant_ids_to_filter # IDs for IN clause and for INSTR ordering

                with self._get_sqlite_conn() as conn:
                    conn.row_factory = sqlite3.Row; cursor = conn.cursor()
                    cursor.execute(sql_query_final, sql_params)
                    rows = cursor.fetchall()
                    for row_dict in rows:
                        kem_proto = self._kem_from_db_row(row_dict, embeddings_map=embeddings_from_qdrant)
                        found_kems_proto.append(kem_proto)

                if len(search_result) == page_size : # If Qdrant returned a full page, there might be more
                    next_offset_str = str(offset + page_size)

            except Exception as e:
                logger.error("Ошибка Qdrant (RetrieveKEMs - vector search): {}".format(e), exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"Ошибка Qdrant (RetrieveKEMs): {e}"); return glm_service_pb2.RetrieveKEMsResponse()

        else: # No embedding query, proceed with SQLite filtering and pagination
            sql_conditions = []; sql_params = []
            if query.ids:
                placeholders = ','.join('?' for _ in query.ids); sql_conditions.append(f"id IN ({placeholders})"); sql_params.extend(query.ids)
            for key, value in query.metadata_filters.items():
                sql_conditions.append(f"json_extract(metadata, '$.{key}') = ?"); sql_params.append(value)

            def add_date_condition(field_name, proto_timestamp, operator):
                if proto_timestamp.seconds > 0 or proto_timestamp.nanos > 0:
                    sql_conditions.append(f"{field_name} {operator} ?");
                    sql_params.append(proto_timestamp.ToDatetime().isoformat())

            add_date_condition("created_at", query.created_at_start, ">=")
            add_date_condition("created_at", query.created_at_end, "<=")
            add_date_condition("updated_at", query.updated_at_start, ">=")
            add_date_condition("updated_at", query.updated_at_end, "<=")

            sql_where_clause = " WHERE " + " AND ".join(sql_conditions) if sql_conditions else ""
            sql_query_base = "SELECT id, content_type, content, metadata, created_at, updated_at FROM kems" + sql_where_clause
            sql_query_ordered = sql_query_base + " ORDER BY updated_at DESC"
            sql_query_paginated = sql_query_ordered + " LIMIT ? OFFSET ?"
            sql_params_paginated = sql_params + [page_size, offset]

            logger.info("SQLite запрос (RetrieveKEMs non-vector): {}, Параметры: {}".format(sql_query_paginated, sql_params_paginated))

            try:
                with self._get_sqlite_conn() as conn:
                    conn.row_factory = sqlite3.Row; cursor = conn.cursor()
                    cursor.execute(sql_query_paginated, sql_params_paginated)
                    rows = cursor.fetchall()

                    ids_from_sqlite = [row['id'] for row in rows]
                    sqlite_embeddings_map = {}
                    if ids_from_sqlite and self.qdrant_client:
                        try:
                            q_points = self.qdrant_client.retrieve(QDRANT_COLLECTION, ids=ids_from_sqlite, with_vectors=True)
                            sqlite_embeddings_map = {p.id: list(p.vector) for p in q_points if p.vector}
                        except Exception as e: logger.warning(f"Не удалось получить эмбеддинги из Qdrant для SQLite результатов: {e}", exc_info=True)

                    for row_dict in rows:
                        kem_proto = self._kem_from_db_row(row_dict, sqlite_embeddings_map)
                        found_kems_proto.append(kem_proto)

                    if len(rows) == page_size:
                        cursor_count = conn.cursor()
                        cursor_count.execute("SELECT COUNT(1) FROM (" + sql_query_ordered + " LIMIT 1 OFFSET ?)", sql_params + [offset + page_size])
                        if cursor_count.fetchone()[0] > 0: next_offset_str = str(offset + page_size)
            except Exception as e:
                logger.error("Ошибка SQLite (RetrieveKEMs non-vector): {}".format(e), exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"Ошибка SQLite (RetrieveKEMs non-vector): {e}")
                return glm_service_pb2.RetrieveKEMsResponse()

        logger.info("RetrieveKEMs: возвращено {} КЕП, next_page_token='{}'".format(len(found_kems_proto), next_offset_str))
        return glm_service_pb2.RetrieveKEMsResponse(kems=found_kems_proto, next_page_token=next_offset_str)

    def UpdateKEM(self, request, context):
        kem_id = request.kem_id
        kem_data_update = request.kem_data_update
        logger.info("UpdateKEM: ID='{}'".format(kem_id))

        if not kem_id:
            msg = "KEM ID должен быть указан для обновления."
            logger.error(msg); context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return kem_pb2.KEM()

        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id = ?", (kem_id,))
                row = cursor.fetchone()
                if not row:
                    msg = "КЕП с ID '{}' не найдена для обновления.".format(kem_id)
                    logger.warning(msg); context.abort(grpc.StatusCode.NOT_FOUND, msg); return kem_pb2.KEM()

                current_kem_dict = dict(row)
                current_kem_dict['metadata'] = json.loads(current_kem_dict.get('metadata', '{}'))
                original_created_at_iso = current_kem_dict['created_at'] # Сохраняем оригинальный created_at

                if kem_data_update.HasField("content_type"): current_kem_dict['content_type'] = kem_data_update.content_type
                if kem_data_update.HasField("content"): current_kem_dict['content'] = kem_data_update.content.value # content is BytesValue
                if kem_data_update.metadata:
                    current_kem_dict['metadata'] = dict(kem_data_update.metadata)

                current_time_proto = Timestamp(); current_time_proto.GetCurrentTime()
                current_kem_dict['updated_at'] = current_time_proto.ToDatetime().isoformat()

                cursor.execute('''
                UPDATE kems SET content_type = ?, content = ?, metadata = ?, updated_at = ?
                WHERE id = ?
                ''', (current_kem_dict['content_type'], current_kem_dict['content'],
                      json.dumps(current_kem_dict['metadata']), current_kem_dict['updated_at'],
                      kem_id))
                conn.commit()
                logger.info("КЕП ID '{}' обновлена в SQLite.".format(kem_id))

                final_embeddings = list(kem_data_update.embeddings)
                if self.qdrant_client and final_embeddings:
                    if len(final_embeddings) != DEFAULT_VECTOR_SIZE:
                        msg = "Неверная размерность эмбеддингов: {}".format(len(final_embeddings))
                        logger.error(msg); context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return kem_pb2.KEM()
                    self.qdrant_client.upsert(
                        collection_name=QDRANT_COLLECTION,
                        points=[PointStruct(id=kem_id, vector=final_embeddings)]
                    )
                    logger.info("Эмбеддинги для КЕП ID '{}' обновлены в Qdrant.".format(kem_id))
                elif not final_embeddings and self.qdrant_client:
                    points_response = self.qdrant_client.get_points(QDRANT_COLLECTION, ids=[kem_id], with_vectors=True)
                    if points_response.points and points_response.points[0].vector:
                        final_embeddings = list(points_response.points[0].vector)

                current_kem_dict['created_at'] = original_created_at_iso # Восстанавливаем для ответа
                current_kem_dict['embeddings'] = final_embeddings

                return self._kem_dict_to_proto(current_kem_dict)

        except grpc.RpcError: raise
        except Exception as e:
            msg = "Ошибка при обновлении КЕП ID '{}': {}".format(kem_id, e)
            logger.error(msg, exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, msg); return kem_pb2.KEM()


    def DeleteKEM(self, request, context):
        kem_id = request.kem_id
        logger.info("DeleteKEM: ID='{}'".format(kem_id))

        if not kem_id:
            msg = "KEM ID должен быть указан для удаления."
            logger.error(msg); context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return empty_pb2.Empty()
        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor(); cursor.execute("DELETE FROM kems WHERE id = ?", (kem_id,))
                conn.commit()
                if cursor.rowcount == 0: logger.warning("КЕП ID '{}' не найдена в SQLite.".format(kem_id))
                else: logger.info("КЕП ID '{}' удалена из SQLite.".format(kem_id))
            if self.qdrant_client:
                try:
                    self.qdrant_client.delete_points(
                        collection_name=QDRANT_COLLECTION,
                        points_selector=models.PointIdsList(points=[kem_id])
                    )
                    logger.info("Точка для КЕП ID '{}' удалена из Qdrant.".format(kem_id))
                except Exception as e:
                    logger.warning("Ошибка при удалении из Qdrant ID '{}': {}".format(kem_id, e))
            return empty_pb2.Empty()
        except Exception as e:
            msg = "Ошибка DeleteKEM для ID '{}': {}".format(kem_id, e)
            logger.error(msg, exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, msg); return empty_pb2.Empty()

def serve():
    logger.info(f"Конфигурация GLM: Qdrant={QDRANT_HOST}:{QDRANT_PORT} ({QDRANT_COLLECTION}), SQLite={SQLITE_DB_PATH}, gRPC Адрес={GRPC_LISTEN_ADDRESS}")
    try:
        # More robust Qdrant check
        client_test = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=2)
        client_test.get_collections() # Throws error if not reachable
        logger.info(f"Qdrant доступен на {QDRANT_HOST}:{QDRANT_PORT}.")
    except Exception as e:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Qdrant недоступен. {e}. Сервер НЕ ЗАПУЩЕН.")
        return

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    try:
        servicer_instance = GlobalLongTermMemoryServicerImpl()
    except Exception as e:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА при инициализации GlobalLongTermMemoryServicerImpl: {e}")
        return

    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(servicer_instance, server)
    server.add_insecure_port(GRPC_LISTEN_ADDRESS)
    logger.info(f"Запуск GLM сервера на {GRPC_LISTEN_ADDRESS}...")
    server.start(); logger.info(f"GLM сервер запущен."); server.wait_for_termination()
    logger.info("GLM сервер остановлен.")

if __name__ == '__main__':
    serve()
