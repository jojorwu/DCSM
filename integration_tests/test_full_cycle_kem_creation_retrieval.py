import unittest
import uuid
import time

# Добавляем корень проекта в sys.path для импортов
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from integration_tests.kps_client_simple import process_data_via_kps
from dcsm_agent_sdk_python.sdk import AgentSDK
# DEFAULT_VECTOR_SIZE из GLM, чтобы проверить наличие эмбеддингов правильной длины
# Это значение должно быть согласовано с тем, что используется в KPS и GLM при запуске
# В docker-compose.yml я установил DEFAULT_VECTOR_SIZE: "384" для GLM и KPS.
# Здесь мы его используем для проверки.
# Если KPS генерирует эмбеддинги другой длины, тест упадет.
# Это значение берется из конфигурации GLM по умолчанию, если не переопределено.
# Чтобы тест был более надежным, он должен либо знать это значение, либо не проверять длину эмбеддинга.
# Пока что захардкодим ожидаемый размер, соответствующий all-MiniLM-L6-v2
EXPECTED_EMBEDDING_SIZE = 384


KPS_SERVICE_URL = os.getenv("KPS_SERVICE_URL", "localhost:50052")
GLM_SERVICE_URL = os.getenv("GLM_SERVICE_URL", "localhost:50051")


class TestFullCycleKemCreationRetrieval(unittest.TestCase):

    def test_kem_creation_through_kps_and_retrieval_through_sdk(self):
        print(f"--- Запуск Интеграционного Теста: KPS -> GLM -> AgentSDK ---")
        print(f"KPS URL: {KPS_SERVICE_URL}, GLM URL: {GLM_SERVICE_URL}")

        test_data_id = f"it_kem_{uuid.uuid4()}"
        test_content = "Это интеграционный тест для полного цикла создания и получения КЕП."
        test_metadata = {"source": "integration_test", "test_run_id": str(uuid.uuid4())}

        # 1. Создание КЕП через KPS
        print(f"\nШаг 1: Отправка данных в KPS (data_id: {test_data_id})...")
        kps_response = process_data_via_kps(
            target_address=KPS_SERVICE_URL,
            data_id=test_data_id,
            content_type="text/plain",
            raw_content_bytes=test_content.encode('utf-8'),
            initial_metadata=test_metadata
        )

        self.assertIsNotNone(kps_response, "Ответ от KPS не должен быть None")
        self.assertTrue(kps_response.success, f"KPS не смог обработать данные: {kps_response.status_message}")
        self.assertIsNotNone(kps_response.kem_id, "KPS должен вернуть kem_id")
        kem_id_from_kps = kps_response.kem_id
        print(f"KPS успешно обработал данные. Получен KEM ID: {kem_id_from_kps}")

        # Даем небольшую паузу, чтобы GLM точно успел обработать (особенно если есть очередь или асинхронность)
        time.sleep(2) # 2 секунды должно быть достаточно для локального docker-compose

        # 2. Получение КЕП через AgentSDK (напрямую из GLM)
        print(f"\nШаг 2: Получение КЕП ID '{kem_id_from_kps}' через AgentSDK из GLM...")
        sdk = None
        retrieved_kem = None
        try:
            # Используем lpa_max_size=0, чтобы гарантированно не использовать ЛПА для этого get
            # и всегда идти на сервер. Или force_remote=True.
            sdk = AgentSDK(glm_server_address=GLM_SERVICE_URL, lpa_max_size=0)
            retrieved_kem = sdk.get_kem(kem_id_from_kps, force_remote=True)
        except Exception as e:
            self.fail(f"AgentSDK не смог подключиться или получить КЕП: {e}")
        finally:
            if sdk:
                sdk.close()

        self.assertIsNotNone(retrieved_kem, f"КЕП ID '{kem_id_from_kps}' не найдена в GLM через SDK.")
        print(f"AgentSDK успешно получил КЕП: ID='{retrieved_kem.get('id')}'")

        # 3. Проверка полей полученной КЕП
        print("\nШаг 3: Проверка полей полученной КЕП...")
        self.assertEqual(retrieved_kem.get('id'), kem_id_from_kps)
        self.assertEqual(retrieved_kem.get('content_type'), "text/plain")

        # Содержимое может быть возвращено как base64 строка, если MessageToDict так делает,
        # или как bytes, если _kem_proto_to_dict в GLMClient его не декодирует.
        # GLMClient._kem_proto_to_dict пытается декодировать в utf-8.
        retrieved_content = retrieved_kem.get('content')
        if isinstance(retrieved_content, bytes):
            retrieved_content = retrieved_content.decode('utf-8')
        self.assertEqual(retrieved_content, test_content)

        # Проверка метаданных
        self.assertIn("source", retrieved_kem.get('metadata', {}))
        self.assertEqual(retrieved_kem.get('metadata', {}).get('source'), test_metadata["source"])
        self.assertIn("test_run_id", retrieved_kem.get('metadata', {}))
        self.assertEqual(retrieved_kem.get('metadata', {}).get('test_run_id'), test_metadata["test_run_id"])
        # KPS добавляет 'source_data_id' в метаданные
        self.assertIn("source_data_id", retrieved_kem.get('metadata', {}))
        self.assertEqual(retrieved_kem.get('metadata', {}).get('source_data_id'), test_data_id)

        # Проверка наличия и размерности эмбеддингов
        # (предполагаем, что KPS генерирует их для text/plain)
        self.assertIn('embeddings', retrieved_kem, "Поле embeddings отсутствует в полученной КЕП")
        embeddings = retrieved_kem.get('embeddings')
        self.assertIsNotNone(embeddings, "Эмбеддинги не должны быть None")
        if isinstance(embeddings, list): # embeddings это repeated float
            self.assertEqual(len(embeddings), EXPECTED_EMBEDDING_SIZE,
                             f"Размерность эмбеддингов ({len(embeddings)}) не совпадает с ожидаемой ({EXPECTED_EMBEDDING_SIZE}).")
            print(f"Эмбеддинги найдены, размерность: {len(embeddings)}")
        else:
            self.fail(f"Поле embeddings не является списком: {type(embeddings)}")

        # Проверка временных меток (что они установлены)
        self.assertTrue(retrieved_kem.get('created_at'), "Поле created_at отсутствует или пустое")
        self.assertTrue(retrieved_kem.get('updated_at'), "Поле updated_at отсутствует или пустое")
        # Можно добавить более сложную проверку формата Timestamp строк, если необходимо

        print(f"--- Интеграционный Тест KPS -> GLM -> AgentSDK УСПЕШНО ЗАВЕРШЕН ---")

if __name__ == '__main__':
    # Этот тест нужно запускать, когда все сервисы (KPS, GLM, Qdrant) запущены
    # например, через docker-compose.
    print("Запуск интеграционного теста test_full_cycle_kem_creation_retrieval.py")
    print(f"Ожидается, что KPS слушает на {KPS_SERVICE_URL}")
    print(f"Ожидается, что GLM слушает на {GLM_SERVICE_URL}")

    # Для запуска из командной строки: python -m unittest integration_tests.test_full_cycle_kem_creation_retrieval
    unittest.main()
