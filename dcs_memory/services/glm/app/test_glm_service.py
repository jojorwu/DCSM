import unittest
from unittest.mock import MagicMock, patch, call
import uuid
import json

# Добавляем service_root_dir в sys.path, как это сделано в main.py GLM
import sys
import os
current_script_path = os.path.abspath(__file__)
app_dir_test = os.path.dirname(current_script_path) # /app/dcs_memory/services/glm/app
service_root_dir_test = os.path.dirname(app_dir_test) # /app/dcs_memory/services/glm

if service_root_dir_test not in sys.path:
    sys.path.insert(0, service_root_dir_test)

from app.main import GlobalLongTermMemoryServicerImpl
from generated_grpc import kem_pb2, glm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp

# Нужно определить DEFAULT_VECTOR_SIZE, который используется в main.py, для тестов
# Возьмем его из main.py или зададим здесь константой.
# В main.py: DEFAULT_VECTOR_SIZE = int(os.getenv("DEFAULT_VECTOR_SIZE", 25))
# Для тестов установим его явно, чтобы не зависеть от env.
TEST_DEFAULT_VECTOR_SIZE = 3 # Уменьшим для простоты тестов эмбеддингов

def create_proto_kem(id: str = None, metadata: dict = None, embeddings: list[float] = None, content: str = "content") -> kem_pb2.KEM:
    kem = kem_pb2.KEM()
    if id:
        kem.id = id
    kem.content_type = "text/plain"
    kem.content = content.encode('utf-8')
    if metadata:
        for k, v in metadata.items():
            kem.metadata[k] = str(v) # Protobuf map<string, string>
    if embeddings:
        kem.embeddings.extend(embeddings)

    # created_at и updated_at обычно устанавливаются сервером
    return kem

class TestGLMBatchStoreKEMs(unittest.TestCase):

    def setUp(self):
        # Мокаем QdrantClient и его методы
        self.mock_qdrant_client_patch = patch('app.main.QdrantClient')
        self.MockQdrantClientClass = self.mock_qdrant_client_patch.start()
        self.mock_qdrant_instance = self.MockQdrantClientClass.return_value
        self.mock_qdrant_instance.get_collection.return_value = MagicMock() # Для _ensure_qdrant_collection
        self.mock_qdrant_instance.recreate_collection.return_value = None
        self.mock_qdrant_instance.upsert.return_value = None # По умолчанию успешный апсерт

        # Мокаем SQLite соединение и курсор
        self.mock_sqlite_conn = MagicMock()
        self.mock_sqlite_cursor = MagicMock()
        self.mock_sqlite_conn.cursor.return_value = self.mock_sqlite_cursor
        self.mock_sqlite_cursor.fetchone.return_value = None # По умолчанию КЕП не существует (для is_new_kem)
        self.mock_sqlite_cursor.execute.return_value = None

        # Патчим _get_sqlite_conn в экземпляре сервисера
        # self.patch_get_sqlite_conn = patch.object(GlobalLongTermMemoryServicerImpl, '_get_sqlite_conn', return_value=self.mock_sqlite_conn)
        # self.mock_get_sqlite_conn = self.patch_get_sqlite_conn.start()
        # Вместо патчинга метода класса, мы можем передать mock_conn через __init__ или мокнуть sqlite3.connect
        # Проще всего мокнуть sqlite3.connect глобально для этих тестов
        self.mock_sqlite3_connect_patch = patch('app.main.sqlite3.connect')
        self.mock_sqlite3_connect = self.mock_sqlite3_connect_patch.start()
        self.mock_sqlite3_connect.return_value = self.mock_sqlite_conn


        # Переменные окружения для тестов (если нужны, но лучше мокать зависимости)
        # os.environ["DEFAULT_VECTOR_SIZE"] = str(TEST_DEFAULT_VECTOR_SIZE)
        # Создаем экземпляр сервисера ПОСЛЕ установки всех моков
        # Также нужно передать или мокнуть DEFAULT_VECTOR_SIZE из main.py
        with patch('app.main.DEFAULT_VECTOR_SIZE', TEST_DEFAULT_VECTOR_SIZE):
             self.servicer = GlobalLongTermMemoryServicerImpl()
             # Если QdrantClient создается в __init__, нужно убедиться, что мок уже активен
             # self.servicer.qdrant_client = self.mock_qdrant_instance # Можно присвоить явно, если __init__ сложный

    def tearDown(self):
        self.mock_qdrant_client_patch.stop()
        self.mock_sqlite3_connect_patch.stop()
        # self.patch_get_sqlite_conn.stop()


    def test_batch_store_all_successful_no_embeddings(self):
        kems_in = [create_proto_kem(id=f"kem{i}") for i in range(2)]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)

        response = self.servicer.BatchStoreKEMs(request, None)

        self.assertEqual(len(response.successfully_stored_kems), 2)
        self.assertEqual(len(response.failed_kem_references), 0)
        self.assertEqual(response.overall_error_message, "")
        self.mock_qdrant_instance.upsert.assert_not_called() # Нет эмбеддингов
        # Проверить вызовы SQLite (2 раза INSERT/REPLACE)
        self.assertEqual(self.mock_sqlite_cursor.execute.call_count, 2 * 2 + 2) # 2x (SELECT + INSERT) + 2x индексы в init

    def test_batch_store_all_successful_with_embeddings(self):
        kems_in = [create_proto_kem(id=f"kem{i}", embeddings=[0.1]*TEST_DEFAULT_VECTOR_SIZE) for i in range(2)]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)

        response = self.servicer.BatchStoreKEMs(request, None)

        self.assertEqual(len(response.successfully_stored_kems), 2)
        self.assertEqual(len(response.failed_kem_references), 0)
        self.mock_qdrant_instance.upsert.assert_called()
        self.assertEqual(self.mock_qdrant_instance.upsert.call_count, 2)

    def test_batch_store_sqlite_error_for_one_kem(self):
        kems_in = [
            create_proto_kem(id="kem1", embeddings=[0.1]*TEST_DEFAULT_VECTOR_SIZE),
            create_proto_kem(id="kem2", embeddings=[0.2]*TEST_DEFAULT_VECTOR_SIZE)
        ]
        # Ошибка SQLite для второй КЕП
        def sqlite_execute_side_effect(*args, **kwargs):
            sql = args[0]
            # Ошибка на второй INSERT (после SELECT для kem1, INSERT для kem1, SELECT для kem2)
            if "INSERT OR REPLACE INTO kems" in sql and self.mock_sqlite_cursor.execute.call_count >= 4: # Приблизительно
                 # Подсчет вызовов может быть неточным, лучше проверять по ID КЕП
                 # Для простоты, если это INSERT и kem_id == "kem2" (нужно передать kem_id в side_effect)
                 # Проще всего просто упасть на 3-й или 4-й execute, если он INSERT
                if 'kem2' in str(args[1]): # Если ID 'kem2' в параметрах INSERT
                    raise sqlite3.Error("Simulated SQLite error for kem2")
            return MagicMock()

        # Сброс mock_calls для execute перед этим тестом, чтобы call_count был точным для этого теста
        self.mock_sqlite_cursor.execute.reset_mock()
        self.mock_sqlite_cursor.execute.side_effect = sqlite_execute_side_effect
        #fetchone должен вернуть None для is_new_kem = True
        self.mock_sqlite_cursor.fetchone.side_effect = [None, None]


        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)
        response = self.servicer.BatchStoreKEMs(request, None)

        self.assertEqual(len(response.successfully_stored_kems), 1)
        self.assertEqual(response.successfully_stored_kems[0].id, "kem1")
        self.assertEqual(len(response.failed_kem_references), 1)
        self.assertIn("kem2", response.failed_kem_references)
        # Qdrant должен был вызваться только для kem1
        self.mock_qdrant_instance.upsert.assert_called_once()
        # Проверить, что у вызванного upsert ID был kem1
        args, _ = self.mock_qdrant_instance.upsert.call_args
        self.assertEqual(args[0][0].id, "kem1")


    def test_batch_store_qdrant_error_for_one_kem(self):
        kems_in = [
            create_proto_kem(id="ok_kem", embeddings=[0.1]*TEST_DEFAULT_VECTOR_SIZE),
            create_proto_kem(id="qdrant_fail_kem", embeddings=[0.2]*TEST_DEFAULT_VECTOR_SIZE)
        ]

        self.mock_qdrant_instance.upsert.side_effect = lambda collection_name, points, **kwargs: \
            (None if points[0].id == "qdrant_fail_kem" else (_ for _ in ()).throw(RuntimeError("Simulated Qdrant Error"))) \
            if points[0].id == "qdrant_fail_kem" else None


        # Сброс execute перед этим тестом
        self.mock_sqlite_cursor.execute.reset_mock()
        # Настроим execute так, чтобы он отслеживал DELETE
        delete_calls = []
        def track_delete_sqlite(*args, **kwargs):
            sql = args[0]
            if "DELETE FROM kems" in sql:
                delete_calls.append(args[1]) # (kem_id,)
            return MagicMock()
        self.mock_sqlite_cursor.execute.side_effect = track_delete_sqlite
        self.mock_sqlite_cursor.fetchone.side_effect = [None, None] # Обе КЕП новые


        request = glm_service_pb2.BatchStoreKEMsRequest(kems=kems_in)
        response = self.servicer.BatchStoreKEMs(request, None)

        self.assertEqual(len(response.successfully_stored_kems), 1)
        self.assertEqual(response.successfully_stored_kems[0].id, "ok_kem")
        self.assertEqual(len(response.failed_kem_references), 1)
        self.assertIn("qdrant_fail_kem", response.failed_kem_references)

        # Проверить, что DELETE был вызван для qdrant_fail_kem
        self.assertTrue(any(call_args[0] == "qdrant_fail_kem" for call_args in delete_calls))


if __name__ == '__main__':
    unittest.main()
