import grpc
import sys
import os

# Добавляем корень проекта в sys.path, чтобы найти dcs_memory и generated_grpc
# Это нужно, если скрипт запускается напрямую из integration_tests
# При запуске через `python -m integration_tests.имя_файла` из корня, это может быть не нужно.
# Но для прямого запуска `python integration_tests/kps_client_simple.py` - нужно.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    # Попытка импорта сгенерированного кода KPS
    # Путь к generated_grpc зависит от того, как структурированы импорты в KPS
    # Если KPS имеет свой generated_grpc, то from dcs_memory.services.kps.generated_grpc import ...
    # Предполагаем, что PYTHONPATH настроен так, что generated_grpc доступен.
    # Для простоты, предположим, что мы можем импортировать так, если тесты запускаются из корня:
    from dcs_memory.generated_grpc import kps_service_pb2
    from dcs_memory.generated_grpc import kps_service_pb2_grpc
except ModuleNotFoundError:
    print("Ошибка: Не удалось импортировать сгенерированный gRPC код для KPS.")
    print("Убедитесь, что gRPC код сгенерирован в dcs_memory/generated_grpc")
    print("и что PYTHONPATH настроен правильно, если запускаете этот скрипт не из корня проекта.")
    sys.exit(1)

def process_data_via_kps(target_address: str, data_id: str, content_type: str, raw_content_bytes: bytes, initial_metadata: dict):
    """
    Отправляет данные в KPS для обработки и возвращает ответ.
    """
    try:
        with grpc.insecure_channel(target_address) as channel:
            stub = kps_service_pb2_grpc.KnowledgeProcessorServiceStub(channel)
            request = kps_service_pb2.ProcessRawDataRequest(
                data_id=data_id,
                content_type=content_type,
                raw_content=raw_content_bytes,
                initial_metadata=initial_metadata
            )
            print(f"KPS Client: Отправка запроса ProcessRawData: {request}")
            response = stub.ProcessRawData(request, timeout=30) # Увеличим таймаут для генерации эмбеддингов
            print(f"KPS Client: Получен ответ: {response}")
            return response
    except grpc.RpcError as e:
        print(f"KPS Client: gRPC ошибка: {e.code()} - {e.details()}")
        return None
    except Exception as e:
        print(f"KPS Client: Общая ошибка: {e}")
        return None

if __name__ == '__main__':
    # Пример использования (не для автоматического запуска)
    # KPS_SERVICE_URL = "localhost:50052" # Должен быть доступен, если docker-compose запущен
    # metadata = {"source": "kps_client_simple_test", "language": "python"}
    # text_content = "Это тестовый текст для проверки KPS и генерации эмбеддингов."

    # print(f"Отправка данных в KPS по адресу {KPS_SERVICE_URL}...")
    # kps_response = process_data_via_kps(
    #     KPS_SERVICE_URL,
    #     data_id="test_data_001",
    #     content_type="text/plain",
    #     raw_content_bytes=text_content.encode('utf-8'),
    #     initial_metadata=metadata
    # )

    # if kps_response and kps_response.success:
    #     print(f"KPS успешно обработал данные. KEM ID: {kps_response.kem_id}")
    # else:
    #     print("Не удалось обработать данные через KPS.")
    pass
