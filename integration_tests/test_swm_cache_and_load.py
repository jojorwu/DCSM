import unittest
import uuid
import time

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import grpc
from integration_tests.kps_client_simple import process_data_via_kps
# Импорты для SWM
from dcs_memory.services.swm.generated_grpc import swm_service_pb2
from dcs_memory.services.swm.generated_grpc import swm_service_pb2_grpc
# Импорты для KEMQuery (используется в SWM и GLM)
# from dcs_memory.common.grpc_protos.glm_service_pb2 import KEMQuery # Используем напрямую из общего proto
# Используем из temp_generated_grpc_code, если PYTHONPATH настроен на корень проекта
from temp_generated_grpc_code.glm_service_pb2 import KEMQuery

KPS_SERVICE_URL = os.getenv("KPS_SERVICE_URL", "localhost:50052")
SWM_SERVICE_URL = os.getenv("SWM_SERVICE_URL", "localhost:50053")

class TestSWMCacheAndLoad(unittest.TestCase):

    def _query_swm(self, swm_stub, kem_id_to_query: str) -> list:
        """Хелпер для вызова QuerySWM по ID."""
        kem_query = KEMQuery(ids=[kem_id_to_query])
        query_request = swm_service_pb2.QuerySWMRequest(query=kem_query, page_size=1)
        print(f"SWM Client: Отправка QuerySWM для ID '{kem_id_to_query}': {query_request}")
        query_response = swm_stub.QuerySWM(query_request, timeout=10)
        print(f"SWM Client: Получен ответ QuerySWM: {query_response}")
        return list(query_response.kems)

    def test_load_from_glm_and_query_swm(self):
        print(f"--- Запуск Интеграционного Теста: SWM LoadKEMsFromGLM и QuerySWM ---")
        print(f"KPS URL: {KPS_SERVICE_URL}, SWM URL: {SWM_SERVICE_URL}")

        test_data_id = f"it_swm_load_{uuid.uuid4()}"
        test_content = "КЕП для проверки загрузки SWM из GLM."
        test_metadata = {"source": "integration_test_swm", "test_run_id": str(uuid.uuid4())}

        # 1. Создаем КЕП через KPS, чтобы она была в GLM
        print(f"\nШаг 1: Создание тестовой КЕП через KPS (data_id: {test_data_id})...")
        kps_response = process_data_via_kps(
            target_address=KPS_SERVICE_URL,
            data_id=test_data_id,
            content_type="text/plain",
            raw_content_bytes=test_content.encode('utf-8'),
            initial_metadata=test_metadata
        )
        self.assertIsNotNone(kps_response, "Ответ от KPS не должен быть None")
        self.assertTrue(kps_response.success, f"KPS не смог обработать данные: {kps_response.status_message}")
        kem_id_in_glm = kps_response.kem_id
        print(f"KPS успешно создал КЕП. KEM ID: {kem_id_in_glm}")

        time.sleep(1) # Небольшая пауза для гарантии сохранения в GLM

        # 2. Подключаемся к SWM
        with grpc.insecure_channel(SWM_SERVICE_URL) as channel:
            swm_stub = swm_service_pb2_grpc.SharedWorkingMemoryServiceStub(channel)

            # 3. Проверяем, что КЕП сначала нет в SWM (или ее кэш пуст для этого теста)
            # Для чистоты теста, хорошо бы иметь способ очистить SWM кэш, но пока просто проверяем.
            # Если SWM уже содержит эту КЕП от предыдущих запусков, тест может быть неточным.
            # Однако, ID уникален для каждого запуска теста.
            print(f"\nШаг 2: Проверка отсутствия КЕП ID '{kem_id_in_glm}' в SWM кэше...")
            kems_from_swm_before_load = self._query_swm(swm_stub, kem_id_in_glm)
            # Мы не можем гарантировать, что ее там нет, если SWM не был перезапущен.
            # Но если она есть, это не провал теста, а состояние среды.
            if kems_from_swm_before_load:
                print(f"Предупреждение: КЕП ID '{kem_id_in_glm}' уже была в SWM перед LoadKEMsFromGLM.")

            # 4. Загружаем КЕП из GLM в SWM
            print(f"\nШаг 3: Загрузка КЕП ID '{kem_id_in_glm}' из GLM в SWM...")
            load_query = KEMQuery(ids=[kem_id_in_glm])
            load_request = swm_service_pb2.LoadKEMsFromGLMRequest(query_for_glm=load_query)
            load_response = swm_stub.LoadKEMsFromGLM(load_request, timeout=10)

            print(f"SWM Client: Получен ответ LoadKEMsFromGLM: {load_response}")
            self.assertTrue(load_response.kems_loaded_to_swm_count >= 0) # Может быть 0, если уже была или ошибка
            if kem_id_in_glm not in load_response.loaded_kem_ids and not kems_from_swm_before_load :
                 # Если ее не было до, и она не загрузилась - это проблема
                 self.fail(f"КЕП ID '{kem_id_in_glm}' не была загружена в SWM, хотя должна была.")
            elif kem_id_in_glm in load_response.loaded_kem_ids:
                 print("КЕП успешно загружена в SWM по запросу LoadKEMsFromGLM.")


            time.sleep(0.5) # Пауза, чтобы SWM обработал загрузку

            # 5. Проверяем, что КЕП теперь есть в SWM
            print(f"\nШаг 4: Повторная проверка наличия КЕП ID '{kem_id_in_glm}' в SWM кэше...")
            kems_from_swm_after_load = self._query_swm(swm_stub, kem_id_in_glm)

            self.assertGreaterEqual(len(kems_from_swm_after_load), 1, f"КЕП ID '{kem_id_in_glm}' не найдена в SWM после LoadKEMsFromGLM.")
            if kems_from_swm_after_load:
                retrieved_kem_swm = kems_from_swm_after_load[0]
                self.assertEqual(retrieved_kem_swm.id, kem_id_in_glm)
                self.assertEqual(retrieved_kem_swm.content.decode('utf-8'), test_content)
                self.assertEqual(retrieved_kem_swm.metadata["source"], test_metadata["source"])
                print(f"КЕП ID '{kem_id_in_glm}' успешно найдена в SWM и ее содержимое корректно.")

        print(f"--- Интеграционный Тест SWM Load & Query УСПЕШНО ЗАВЕРШЕН ---")

if __name__ == '__main__':
    print("Запуск интеграционного теста test_swm_cache_and_load.py")
    print(f"Ожидается, что KPS слушает на {KPS_SERVICE_URL}")
    print(f"Ожидается, что SWM слушает на {SWM_SERVICE_URL}")
    print("Ожидается, что GLM и Qdrant также запущены.")
    unittest.main()
