import grpc
from concurrent import futures
import time
import sys
import os

# --- Начало блока для корректного импорта сгенерированного кода ---
# main.py находится в dcs_memory/services/glm/app/
# Сгенерированный код находится в dcs_memory/services/glm/generated_grpc/
# Чтобы импортировать из generated_grpc, нам нужно, чтобы
# директория dcs_memory/services/glm/ была в sys.path,
# чтобы Python мог найти пакет generated_grpc.

# Добавляем родительскую директорию от 'app' (т.е. dcs_memory/services/glm) в sys.path
# Это позволит писать 'from generated_grpc import ...'
current_script_path = os.path.abspath(__file__)
app_dir = os.path.dirname(current_script_path) # dcs_memory/services/glm/app
service_root_dir = os.path.dirname(app_dir) # dcs_memory/services/glm

if service_root_dir not in sys.path:
    sys.path.insert(0, service_root_dir)

# Теперь импортируем сгенерированные модули
from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2
from generated_grpc import glm_service_pb2_grpc
from google.protobuf import empty_pb2
# --- Конец блока для корректного импорта ---

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def StoreKEMs(self, request, context):
        print("Обработчик StoreKEMs в dcs_memory/services/glm: Получено {} КЕП.".format(len(request.kems)))
        for kem in request.kems:
            print("  КЕП ID: {}, Тип: {}, Метаданные: {}".format(kem.id, kem.content_type, kem.metadata))
        stored_ids = [kem.id for kem in request.kems]
        print("Имитация сохранения КЕП с ID: {}".format(stored_ids))
        return glm_service_pb2.StoreKEMsResponse(
            stored_kem_ids=stored_ids,
            success_count=len(request.kems)
        )

    def RetrieveKEMs(self, request, context):
        print("Обработчик RetrieveKEMs в dcs_memory/services/glm: Запрос: Текст='{}', Фильтры='{}', Лимит='{}'".format(
            request.query.text_query, request.query.metadata_filters, request.limit))
        print("Имитация извлечения: возвращен пустой список КЕП.")
        return glm_service_pb2.RetrieveKEMsResponse(kems=[])

    def UpdateKEM(self, request, context):
        print("Обработчик UpdateKEM в dcs_memory/services/glm: Запрос для КЕП ID: {}".format(request.kem_id))
        print("Имитация обновления для КЕП ID: {}. Данные для обновления: {}".format(request.kem_id, request.kem_data_update))

        updated_kem = kem_pb2.KEM()
        updated_kem.id = request.kem_id
        if request.kem_data_update.IsInitialized():
            updated_kem.content_type = request.kem_data_update.content_type
            updated_kem.content = request.kem_data_update.content
            updated_kem.embeddings.extend(request.kem_data_update.embeddings)
            for key, value in request.kem_data_update.metadata.items():
                updated_kem.metadata[key] = value

        print("Возвращаем 'обновленную' КЕП: {}".format(updated_kem))
        return updated_kem

    def DeleteKEM(self, request, context):
        print("Обработчик DeleteKEM в dcs_memory/services/glm: Запрос для КЕП ID: {}".format(request.kem_id))
        print("Имитация удаления КЕП ID: {}".format(request.kem_id))
        return empty_pb2.Empty()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(
        GlobalLongTermMemoryServicerImpl(), server
    )
    server.add_insecure_port('[::]:50051') # Стандартный порт для GLM
    print("Запуск GLM сервера (из dcs_memory/services/glm) на порту 50051...")
    server.start()
    print("GLM сервер (dcs_memory/services/glm) запущен и ожидает соединений.")
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        print("Остановка GLM сервера (dcs_memory/services/glm)...")
        server.stop(0)
        print("GLM сервер (dcs_memory/services/glm) остановлен.")

if __name__ == '__main__':
    serve()
