import grpc
from concurrent import futures
import time
import sys
import os

# --- BEGIN sys.path MODIFICATION ---
# Add the directory where main.py is located (app/) to sys.path
# This allows `from generated_grpc_code import ...`
app_dir = os.path.dirname(os.path.abspath(__file__))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

# Add the generated_grpc_code directory itself to sys.path
# This allows files within generated_grpc_code (like glm_service_pb2.py)
# to import other files in the same directory (like kem_pb2.py) directly.
generated_grpc_code_dir = os.path.join(app_dir, "generated_grpc_code")
if generated_grpc_code_dir not in sys.path:
    sys.path.insert(0, generated_grpc_code_dir)

# print("--- sys.path (at global scope) ---") # Debugging print, can be removed
# for p in sys.path:
#     print(p)
# print("----------------------------------")
# --- END sys.path MODIFICATION ---

# Импорты сгенерированного кода.
# ... (rest of the comments remain the same)
from generated_grpc_code import kem_pb2
from generated_grpc_code import glm_service_pb2
from generated_grpc_code import glm_service_pb2_grpc
from google.protobuf import empty_pb2 # google.protobuf обычно доступен глобально после установки grpcio-tools

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def StoreKEMs(self, request, context):
        print(f"Обработчик StoreKEMs: Получено {len(request.kems)} КЕП.")
        for kem in request.kems:
            print(f"  КЕП ID: {kem.id}, Тип: {kem.content_type}, Метаданные: {kem.metadata}")
        # Здесь будет логика сохранения КЕП
        stored_ids = [kem.id for kem in request.kems]
        print(f"Имитация сохранения КЕП с ID: {stored_ids}")
        return glm_service_pb2.StoreKEMsResponse(
            stored_kem_ids=stored_ids,
            success_count=len(request.kems)
        )

    def RetrieveKEMs(self, request, context):
        print(f"Обработчик RetrieveKEMs: Запрос: Текст='{request.query.text_query}', Фильтры='{request.query.metadata_filters}', Лимит='{request.limit}'")
        # Здесь будет логика извлечения КЕП
        # Пока возвращаем пустой список
        print("Имитация извлечения: возвращен пустой список КЕП.")
        return glm_service_pb2.RetrieveKEMsResponse(kems=[])

    def UpdateKEM(self, request, context):
        print(f"Обработчик UpdateKEM: Запрос для КЕП ID: {request.kem_id}")
        # Здесь будет логика обновления КЕП
        print(f"Имитация обновления для КЕП ID: {request.kem_id}. Данные для обновления: {request.kem_data_update}")
        # Возвращаем kem_data_update с установленным ID, как будто обновление прошло успешно
        # и копируем основные поля из kem_data_update
        updated_kem = kem_pb2.KEM()
        updated_kem.id = request.kem_id
        if request.kem_data_update.IsInitialized(): # Проверяем, есть ли данные для обновления
            updated_kem.content_type = request.kem_data_update.content_type
            updated_kem.content = request.kem_data_update.content
            updated_kem.embeddings.extend(request.kem_data_update.embeddings)
            for key, value in request.kem_data_update.metadata.items():
                updated_kem.metadata[key] = value
            # Временные метки должны обновляться сервером
            # updated_kem.updated_at.GetCurrentTime() # Пример

        print(f"Возвращаем 'обновленную' КЕП: {updated_kem}")
        return updated_kem


    def DeleteKEM(self, request, context):
        print(f"Обработчик DeleteKEM: Запрос для КЕП ID: {request.kem_id}")
        # Здесь будет логика удаления КЕП
        print(f"Имитация удаления КЕП ID: {request.kem_id}")
        return empty_pb2.Empty()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(
        GlobalLongTermMemoryServicerImpl(), server
    )
    server.add_insecure_port('[::]:50051')
    print("Запуск GLM сервера на порту 50051...")
    server.start()
    print("GLM сервер запущен и ожидает соединений.")
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        print("Остановка GLM сервера...")
        server.stop(0)
        print("GLM сервер остановлен.")

if __name__ == '__main__':
    import sys
    import os

    # Add the directory where main.py is located (app/) to sys.path
    # This allows `from generated_grpc_code import ...`
    app_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(app_dir)

    # Add the generated_grpc_code directory itself to sys.path
    # This allows files within generated_grpc_code (like glm_service_pb2.py)
    # to import other files in the same directory (like kem_pb2.py) directly.
    # Test imports directly here (moved to global for earlier check)
    # try:
    #     print("Attempting imports in main...")
    #     # from generated_grpc_code import kem_pb2 # Already imported globally
    #     print("Successfully imported kem_pb2 in main (global)")
    #     # from generated_grpc_code import glm_service_pb2 # Already imported globally
    #     print("Successfully imported glm_service_pb2 in main (global)")
    #     print("Global imports successful. If an error occurred, it was during those global imports.")
    # except Exception as e:
    #     print(f"Error during test imports in main (after global): {e}")
    #     import traceback
    #     traceback.print_exc()

    # sys.exit("Exiting for debug.") # Remove this to allow server to run
    serve()
