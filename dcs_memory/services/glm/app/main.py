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

# Импортируем конфигурацию
from .config import GLMConfig

# --- Начало блока для корректного импорта сгенерированного кода ---
# app_dir определяется здесь для SQLITE_DB_PATH
current_script_path = os.path.abspath(__file__)
app_dir = os.path.dirname(current_script_path) # /app/dcs_memory/services/glm/app
# --- Конец определения app_dir ---

# Глобальный экземпляр конфигурации
# Он будет инициализирован один раз при загрузке модуля main.py
# Pydantic автоматически прочитает переменные окружения (с префиксом GLM_) и .env файлы
config = GLMConfig()

# --- Настройка логирования ---
# Используем уровень логирования из конфигурации
logging.basicConfig(
    level=config.get_log_level_int(), # Используем метод из BaseServiceConfig
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
# --- Конец настройки логирования ---


service_root_dir = os.path.dirname(app_dir) # /app/dcs_memory/services/glm
if service_root_dir not in sys.path:
    sys.path.insert(0, service_root_dir)

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2
from generated_grpc import glm_service_pb2_grpc
from google.protobuf import empty_pb2
# --- Конец блока для корректного импорта ---

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

# --- Старые переменные конфигурации удалены, теперь используется объект config ---
# SQLITE_DB_PATH будет config.DB_FILENAME (но нужно учесть app_dir)
# QDRANT_HOST будет config.QDRANT_HOST
# QDRANT_PORT будет config.QDRANT_PORT
# QDRANT_COLLECTION будет config.QDRANT_COLLECTION
# DEFAULT_VECTOR_SIZE будет config.DEFAULT_VECTOR_SIZE
# DEFAULT_PAGE_SIZE будет config.DEFAULT_PAGE_SIZE
# GRPC_LISTEN_ADDRESS будет config.GRPC_LISTEN_ADDRESS


class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self):
        logger.info("Инициализация GlobalLongTermMemoryServicerImpl...")
        self.qdrant_client = None
        self.config = config # Сохраняем ссылку на глобальный config или передаем его

        # Формируем полный путь к БД SQLite
        self.sqlite_db_path = os.path.join(app_dir, self.config.DB_FILENAME)

        try:
            self.qdrant_client = QdrantClient(host=self.config.QDRANT_HOST, port=self.config.QDRANT_PORT, timeout=10)
            self.qdrant_client.get_collections()
            logger.info(f"Qdrant клиент успешно подключен к {self.config.QDRANT_HOST}:{self.config.QDRANT_PORT}")
            self._ensure_qdrant_collection()
        except Exception as e:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при инициализации Qdrant клиента: {e}. Сервис может работать некорректно.")
            self.qdrant_client = None

        self._init_sqlite()
        logger.info("Сервисер GLM инициализирован.")

    def _get_sqlite_conn(self):
        return sqlite3.connect(self.sqlite_db_path, timeout=10) # Используем self.sqlite_db_path

    def _init_sqlite(self):
        logger.info(f"Инициализация SQLite БД по пути: {self.sqlite_db_path}")
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
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_created_at ON kems (created_at);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_kems_updated_at ON kems (updated_at);")
                conn.commit()
            logger.info("Таблица 'kems' и индексы в SQLite успешно инициализированы.")
        except Exception as e:
            logger.error(f"Ошибка инициализации SQLite: {e}")

    def _ensure_qdrant_collection(self):
        if not self.qdrant_client:
            logger.warning("Qdrant клиент не инициализирован, пропуск создания коллекции.")
            return
        try:
            try:
                collection_info = self.qdrant_client.get_collection(self.config.QDRANT_COLLECTION)
                logger.info(f"Коллекция '{self.config.QDRANT_COLLECTION}' уже существует.")
                # Проверяем существующую конфигурацию, если коллекция есть
                if hasattr(collection_info.config.params.vectors, 'size'): # Для стандартных векторов
                    current_vector_size = collection_info.config.params.vectors.size
                    current_distance = collection_info.config.params.vectors.distance
                elif isinstance(collection_info.config.params.vectors, dict): # Для именованных векторов
                    # Предполагаем, что есть вектор по умолчанию или единственный вектор
                    # Это потребует более сложной логики, если имен много.
                    # Пока что, если это dict, ищем ключ 'size' и 'distance' или пропускаем проверку.
                    # Для простоты, если это dict, предполагаем, что конфигурация проверяется вручную.
                    logger.info("Проверка конфигурации для именованных векторов пока не реализована детально.")
                    current_vector_size = self.config.DEFAULT_VECTOR_SIZE # Предполагаем совпадение
                    current_distance = models.Distance.COSINE        # Предполагаем совпадение
                else: # Неизвестный формат конфигурации векторов
                     logger.warning(f"Не удалось определить конфигурацию векторов для коллекции '{self.config.QDRANT_COLLECTION}'. Пропуск проверки.")
                     current_vector_size = self.config.DEFAULT_VECTOR_SIZE
                     current_distance = models.Distance.COSINE

                if current_vector_size != self.config.DEFAULT_VECTOR_SIZE or current_distance != models.Distance.COSINE:
                     logger.warning(f"Конфигурация существующей коллекции '{self.config.QDRANT_COLLECTION}' не совпадает с ожидаемой (размер: {current_vector_size} vs {self.config.DEFAULT_VECTOR_SIZE}, дистанция: {current_distance}).")
                else:
                    logger.info(f"Конфигурация коллекции '{self.config.QDRANT_COLLECTION}' соответствует.")
            except Exception as e_get_collection: # Явно ловим ошибку получения коллекции (например, если ее нет)
                if "not found" in str(e_get_collection).lower() or (hasattr(e_get_collection, 'status_code') and e_get_collection.status_code == 404):
                    logger.info(f"Коллекция '{self.config.QDRANT_COLLECTION}' не найдена. Создание новой коллекции...")
                    self.qdrant_client.recreate_collection(
                        collection_name=self.config.QDRANT_COLLECTION,
                        vectors_config=models.VectorParams(size=self.config.DEFAULT_VECTOR_SIZE, distance=models.Distance.COSINE)
                    )
                    logger.info(f"Коллекция '{self.config.QDRANT_COLLECTION}' успешно создана.")
                else:
                    # Другая ошибка при get_collection
                    logger.error(f"Ошибка при получении информации о коллекции Qdrant '{self.config.QDRANT_COLLECTION}': {e_get_collection}")
        except Exception as e: # Общая ошибка на случай проблем с recreate_collection и т.д.
            logger.error(f"Ошибка при проверке/создании коллекции Qdrant '{self.config.QDRANT_COLLECTION}': {e}")

    def _kem_dict_to_proto(self, kem_data: dict) -> kem_pb2.KEM:
        # ... (остается без изменений) ...
        kem_data_copy = kem_data.copy()
        if 'created_at' in kem_data_copy and not isinstance(kem_data_copy['created_at'], str):
            del kem_data_copy['created_at']
        if 'updated_at' in kem_data_copy and not isinstance(kem_data_copy['updated_at'], str):
            del kem_data_copy['updated_at']
        if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
             kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')
        return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)


    def _kem_from_db_row(self, row: sqlite3.Row, embeddings_map: typing.Optional[dict] = None) -> kem_pb2.KEM:
        # ... (остается без изменений) ...
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
        # ... (остается без изменений) ...
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
            if len(kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE: # Используем self.config
                msg = "Размерность эмбеддингов ({}) не совпадает с конфигурацией ({}).".format(len(kem.embeddings), self.config.DEFAULT_VECTOR_SIZE)
                logger.error(msg)
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg); return glm_service_pb2.StoreKEMResponse()
            try:
                self.qdrant_client.upsert(
                    collection_name=self.config.QDRANT_COLLECTION, # Используем self.config
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
        page_size = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE # Используем self.config
        offset = 0
        if request.page_token:
            try: offset = int(request.page_token)
            except ValueError: logger.warning(f"Неверный формат page_token: '{request.page_token}'. Используется offset=0.")
        logger.info(f"RetrieveKEMs: query={query}, page_size={page_size}, offset={offset}")
        found_kems_proto = []
        next_offset_str = ""
        embeddings_from_qdrant = {}
        if query.embedding_query:
            if not self.qdrant_client:
                context.abort(grpc.StatusCode.INTERNAL, "Qdrant сервис недоступен."); return glm_service_pb2.RetrieveKEMsResponse()
            if len(query.embedding_query) != self.config.DEFAULT_VECTOR_SIZE: # Используем self.config
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Неверная размерность вектора: {len(query.embedding_query)}, ожидалось {self.config.DEFAULT_VECTOR_SIZE}"); return glm_service_pb2.RetrieveKEMsResponse()
            try:
                qdrant_filter_obj = None # TODO: Implement Qdrant filter
                search_result = self.qdrant_client.search(collection_name=self.config.QDRANT_COLLECTION, query_vector=list(query.embedding_query), query_filter=qdrant_filter_obj, limit=page_size, offset=offset, with_vectors=True) # Используем self.config
                qdrant_ids_to_filter = [hit.id for hit in search_result]
                for hit in search_result: embeddings_from_qdrant[hit.id] = list(hit.vector) if hit.vector else []
                if not qdrant_ids_to_filter: return glm_service_pb2.RetrieveKEMsResponse(kems=[], next_page_token="")
                placeholders = ','.join('?' for _ in qdrant_ids_to_filter)
                sql_query_base = f"SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id IN ({placeholders})"
                sql_query_final = sql_query_base + f" ORDER BY INSTR(',' || ({placeholders}) || ',', ',' || id || ',')"
                sql_params = qdrant_ids_to_filter * 2
                with self._get_sqlite_conn() as conn:
                    conn.row_factory = sqlite3.Row; cursor = conn.cursor()
                    cursor.execute(sql_query_final, sql_params)
                    for row_dict in cursor.fetchall(): found_kems_proto.append(self._kem_from_db_row(row_dict, embeddings_from_qdrant))
                if len(search_result) == page_size: next_offset_str = str(offset + page_size)
            except Exception as e:
                logger.error(f"Ошибка Qdrant (RetrieveKEMs - vector search): {e}", exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"Ошибка Qdrant: {e}"); return glm_service_pb2.RetrieveKEMsResponse()
        else:
            sql_conditions = []; sql_params = []
            if query.ids:
                placeholders = ','.join('?' for _ in query.ids); sql_conditions.append(f"id IN ({placeholders})"); sql_params.extend(query.ids)
            for key, value in query.metadata_filters.items():
                sql_conditions.append(f"json_extract(metadata, '$.{key}') = ?"); sql_params.append(value)
            def add_date_condition(field_name, proto_ts, op):
                if proto_ts.seconds > 0 or proto_ts.nanos > 0:
                    sql_conditions.append(f"{field_name} {op} ?"); sql_params.append(proto_ts.ToDatetime().isoformat())
            add_date_condition("created_at", query.created_at_start, ">="); add_date_condition("created_at", query.created_at_end, "<=")
            add_date_condition("updated_at", query.updated_at_start, ">="); add_date_condition("updated_at", query.updated_at_end, "<=")
            sql_where_clause = " WHERE " + " AND ".join(sql_conditions) if sql_conditions else ""
            sql_query_ordered = f"SELECT id, content_type, content, metadata, created_at, updated_at FROM kems{sql_where_clause} ORDER BY updated_at DESC"
            sql_query_paginated = f"{sql_query_ordered} LIMIT ? OFFSET ?"
            try:
                with self._get_sqlite_conn() as conn:
                    conn.row_factory = sqlite3.Row; cursor = conn.cursor()
                    cursor.execute(sql_query_paginated, sql_params + [page_size, offset])
                    rows = cursor.fetchall()
                    ids_from_sqlite = [row['id'] for row in rows]
                    if ids_from_sqlite and self.qdrant_client:
                        try:
                            q_points = self.qdrant_client.retrieve(self.config.QDRANT_COLLECTION, ids=ids_from_sqlite, with_vectors=True) # Используем self.config
                            for p in q_points:
                                if p.vector: embeddings_from_qdrant[p.id] = list(p.vector)
                        except Exception as e_qd_retrieve: logger.warning(f"Не удалось получить эмбеддинги: {e_qd_retrieve}")
                    for row_dict in rows: found_kems_proto.append(self._kem_from_db_row(row_dict, embeddings_from_qdrant))
                    if len(rows) == page_size:
                        cursor_count = conn.cursor()
                        cursor_count.execute(f"SELECT COUNT(1) FROM ({sql_query_ordered} LIMIT 1 OFFSET ?)", sql_params + [offset + page_size])
                        if cursor_count.fetchone()[0] > 0: next_offset_str = str(offset + page_size)
            except Exception as e:
                logger.error(f"Ошибка SQLite (RetrieveKEMs non-vector): {e}", exc_info=True)
                context.abort(grpc.StatusCode.INTERNAL, f"Ошибка SQLite: {e}"); return glm_service_pb2.RetrieveKEMsResponse()
        return glm_service_pb2.RetrieveKEMsResponse(kems=found_kems_proto, next_page_token=next_offset_str)

    def UpdateKEM(self, request, context):
        # ... (остается без изменений) ...
        kem_id = request.kem_id; kem_data_update = request.kem_data_update
        if not kem_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID должен быть указан"); return kem_pb2.KEM()
        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, content_type, content, metadata, created_at, updated_at FROM kems WHERE id = ?", (kem_id,))
                row = cursor.fetchone()
                if not row: context.abort(grpc.StatusCode.NOT_FOUND, f"КЕП с ID '{kem_id}' не найдена"); return kem_pb2.KEM()
                current_kem_dict = dict(row)
                current_kem_dict['metadata'] = json.loads(current_kem_dict.get('metadata', '{}'))
                original_created_at_iso = current_kem_dict['created_at']
                if kem_data_update.HasField("content_type"): current_kem_dict['content_type'] = kem_data_update.content_type
                if kem_data_update.HasField("content"): current_kem_dict['content'] = kem_data_update.content.value
                if kem_data_update.metadata: current_kem_dict['metadata'] = dict(kem_data_update.metadata)
                ts_now = Timestamp(); ts_now.GetCurrentTime()
                current_kem_dict['updated_at'] = ts_now.ToDatetime().isoformat()
                cursor.execute("UPDATE kems SET content_type = ?, content = ?, metadata = ?, updated_at = ? WHERE id = ?",
                               (current_kem_dict['content_type'], current_kem_dict['content'], json.dumps(current_kem_dict['metadata']), current_kem_dict['updated_at'], kem_id))
                conn.commit()
                final_embeddings = list(kem_data_update.embeddings)
                if self.qdrant_client and final_embeddings:
                    if len(final_embeddings) != self.config.DEFAULT_VECTOR_SIZE: # Используем self.config
                        context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Неверная размерность эмбеддингов: {len(final_embeddings)}, ожидалось {self.config.DEFAULT_VECTOR_SIZE}"); return kem_pb2.KEM()
                    self.qdrant_client.upsert(collection_name=self.config.QDRANT_COLLECTION, points=[PointStruct(id=kem_id, vector=final_embeddings)]) # Используем self.config
                elif not final_embeddings and self.qdrant_client:
                    points_resp = self.qdrant_client.get_points(self.config.QDRANT_COLLECTION, ids=[kem_id], with_vectors=True) # Используем self.config
                    if points_resp.points and points_resp.points[0].vector: final_embeddings = list(points_resp.points[0].vector)
                current_kem_dict['created_at'] = original_created_at_iso
                current_kem_dict['embeddings'] = final_embeddings
                return self._kem_dict_to_proto(current_kem_dict)
        except grpc.RpcError: raise
        except Exception as e:
            logger.error(f"Ошибка при обновлении КЕП ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Ошибка обновления: {e}"); return kem_pb2.KEM()

    def DeleteKEM(self, request, context):
        # ... (остается без изменений) ...
        kem_id = request.kem_id
        if not kem_id: context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID должен быть указан"); return empty_pb2.Empty()
        try:
            with self._get_sqlite_conn() as conn:
                cursor = conn.cursor(); cursor.execute("DELETE FROM kems WHERE id = ?", (kem_id,))
                conn.commit()
                if cursor.rowcount == 0: logger.warning(f"КЕП ID '{kem_id}' не найдена в SQLite для удаления.")
                else: logger.info(f"КЕП ID '{kem_id}' удалена из SQLite.")
            if self.qdrant_client:
                try: self.qdrant_client.delete_points(self.config.QDRANT_COLLECTION, points_selector=models.PointIdsList(points=[kem_id])) # Используем self.config
                except Exception as e_qd_del: logger.warning(f"Ошибка при удалении из Qdrant ID '{kem_id}': {e_qd_del}")
            return empty_pb2.Empty()
        except Exception as e:
            logger.error(f"Ошибка DeleteKEM для ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Ошибка удаления: {e}"); return empty_pb2.Empty()

    def BatchStoreKEMs(self, request: glm_service_pb2.BatchStoreKEMsRequest, context):
        logger.info(f"BatchStoreKEMs: Получено {len(request.kems)} КЕП для сохранения.")
        successfully_stored_kems_list = []
        failed_kem_references_list = []

        for idx, kem_in in enumerate(request.kems):
            current_kem_processed = kem_pb2.KEM()
            current_kem_processed.CopyFrom(kem_in)
            kem_id = current_kem_processed.id if current_kem_processed.id else str(uuid.uuid4())
            current_kem_processed.id = kem_id

            current_time_proto = Timestamp()
            current_time_proto.GetCurrentTime()
            final_created_at_proto = Timestamp()
            is_new_kem = True

            sqlite_persisted_this_kem = False
            qdrant_persisted_this_kem = True # True if no embeddings or no qdrant client

            # Шаг 1: Сохранение/Обновление в SQLite
            try:
                with self._get_sqlite_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT created_at FROM kems WHERE id = ?", (kem_id,))
                    existing_row = cursor.fetchone()
                    if existing_row:
                        is_new_kem = False
                        final_created_at_proto.FromJsonString(existing_row[0] + "Z")

                    if is_new_kem:
                        final_created_at_proto.CopyFrom(current_kem_processed.created_at if current_kem_processed.HasField("created_at") else current_time_proto)

                    current_kem_processed.created_at.CopyFrom(final_created_at_proto)
                    current_kem_processed.updated_at.CopyFrom(current_time_proto)

                    cursor.execute('''
                    INSERT OR REPLACE INTO kems (id, content_type, content, metadata, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (current_kem_processed.id, current_kem_processed.content_type, current_kem_processed.content,
                          json.dumps(dict(current_kem_processed.metadata)),
                          current_kem_processed.created_at.ToDatetime().isoformat(),
                          current_kem_processed.updated_at.ToDatetime().isoformat()))
                    conn.commit()
                sqlite_persisted_this_kem = True
            except Exception as e_sqlite:
                logger.error(f"BatchStoreKEMs: Ошибка SQLite для КЕП ID '{kem_id}' (индекс {idx}): {e_sqlite}", exc_info=True)
                failed_kem_references_list.append(kem_in.id if kem_in.id else f"req_idx_{idx}")
                continue # Переходим к следующей КЕП, эта не удалась на этапе SQLite

            # Шаг 2: Сохранение/Обновление эмбеддингов в Qdrant (только если SQLite был успешен)
            has_embeddings = current_kem_processed.embeddings and len(current_kem_processed.embeddings) > 0

            if has_embeddings:
                if not self.qdrant_client:
                    logger.error(f"BatchStoreKEMs: Qdrant клиент не доступен, не удается сохранить эмбеддинги для КЕП ID '{kem_id}'.")
                    qdrant_persisted_this_kem = False
                elif len(current_kem_processed.embeddings) != self.config.DEFAULT_VECTOR_SIZE: # Используем self.config
                    logger.error(f"BatchStoreKEMs: Неверная размерность эмбеддингов ({len(current_kem_processed.embeddings)}) для КЕП ID '{kem_id}'. Ожидалось {self.config.DEFAULT_VECTOR_SIZE}.")
                    qdrant_persisted_this_kem = False
                else:
                    try:
                        self.qdrant_client.upsert(
                            collection_name=self.config.QDRANT_COLLECTION, # Используем self.config
                            points=[PointStruct(id=kem_id, vector=list(current_kem_processed.embeddings), payload={"kem_id_ref": kem_id})]
                        )
                    except Exception as e_qdrant:
                        logger.error(f"BatchStoreKEMs: Ошибка Qdrant для КЕП ID '{kem_id}': {e_qdrant}", exc_info=True)
                        qdrant_persisted_this_kem = False
            # Если эмбеддингов нет, qdrant_persisted_this_kem остается True

            # Шаг 3: Проверка консистентности и откат SQLite при необходимости
            if sqlite_persisted_this_kem and not qdrant_persisted_this_kem:
                # Если эмбеддинги были, но не сохранились в Qdrant, или Qdrant был недоступен.
                if kem_id not in failed_kem_references_list: # Добавляем в ошибки, если еще не там
                    failed_kem_references_list.append(kem_id)
                try:
                    with self._get_sqlite_conn() as conn_cleanup:
                        cursor_cleanup = conn_cleanup.cursor()
                        cursor_cleanup.execute("DELETE FROM kems WHERE id = ?", (kem_id,))
                        conn_cleanup.commit()
                        logger.info(f"BatchStoreKEMs: Запись для КЕП ID '{kem_id}' удалена из SQLite из-за ошибки/отсутствия Qdrant при наличии эмбеддингов.")
                except Exception as e_cleanup:
                    logger.critical(f"BatchStoreKEMs: КРИТИЧЕСКАЯ ОШИБКА при откате SQLite для КЕП ID '{kem_id}': {e_cleanup}")
            elif sqlite_persisted_this_kem and qdrant_persisted_this_kem:
                successfully_stored_kems_list.append(current_kem_processed)
            # else: если sqlite_persisted_this_kem == False, КЕП уже в failed_kem_references_list

        logger.info(f"BatchStoreKEMs: Успешно обработано {len(successfully_stored_kems_list)} КЕП, не удалось обработать {len(set(failed_kem_references_list))} КЕП.")

        response = glm_service_pb2.BatchStoreKEMsResponse(
            successfully_stored_kems=successfully_stored_kems_list,
            failed_kem_references=list(set(failed_kem_references_list))
        )

        if response.failed_kem_references:
            response.overall_error_message = f"Не удалось полностью сохранить {len(response.failed_kem_references)} КЕП из пакета."
        elif not request.kems and not successfully_stored_kems_list :
            response.overall_error_message = "Получен пустой список КЕП для сохранения."

        return response

def serve():
    # Используем глобальный объект config
    logger.info(f"Конфигурация GLM: Qdrant={config.QDRANT_HOST}:{config.QDRANT_PORT} ({config.QDRANT_COLLECTION}), SQLite={os.path.join(app_dir, config.DB_FILENAME)}, gRPC Адрес={config.GRPC_LISTEN_ADDRESS}, LogLevel={config.LOG_LEVEL}")
    try:
        client_test = QdrantClient(host=config.QDRANT_HOST, port=config.QDRANT_PORT, timeout=2)
        client_test.get_collections()
        logger.info(f"Qdrant доступен на {config.QDRANT_HOST}:{config.QDRANT_PORT}.")
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
    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS) # Используем config
    logger.info(f"Запуск GLM сервера на {config.GRPC_LISTEN_ADDRESS}...")
    server.start(); logger.info(f"GLM сервер запущен."); server.wait_for_termination()
    logger.info("GLM сервер остановлен.")

if __name__ == '__main__':
    serve()
