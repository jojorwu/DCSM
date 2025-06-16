import grpc
# Используем относительные импорты, если glm_client.py находится в том же пакете,
# что и generated_grpc_code. Если generated_grpc_code - это подпакет,
# то импорты должны быть from .generated_grpc_code import ...
# Предполагаем, что dcsm_agent_sdk_python будет добавлен в PYTHONPATH
# или используется структура пакетов, где generated_grpc_code доступен.
# Для простоты, если generated_grpc_code - это директория на том же уровне, что и glm_client.py,
# и оба они являются частью пакета dcsm_agent_sdk_python, то:
from .generated_grpc_code import kem_pb2
from .generated_grpc_code import glm_service_pb2
from .generated_grpc_code import glm_service_pb2_grpc
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf import empty_pb2 # Для DeleteKEM

class GLMClient:
    def __init__(self, server_address='localhost:50051'):
        self.server_address = server_address
        self.channel = None
        self.stub = None
        # Отложенное создание канала и заглушки до первого вызова или явного connect()
        # self.connect() # Можно вызвать здесь или сделать отдельный метод connect()

    def connect(self):
        if not self.channel:
            self.channel = grpc.insecure_channel(self.server_address)
            self.stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.channel)
            print(f"GLMClient подключен к: {self.server_address}")

    def _ensure_connected(self):
        if not self.stub:
            self.connect()

    def _kem_dict_to_proto(self, kem_data: dict) -> kem_pb2.KEM:
        kem_data_copy = kem_data.copy()
        # ParseDict не очень хорошо работает с вложенными Timestamp, если они не строки RFC 3339.
        # Очистим их, если они не строки, чтобы избежать ошибок. Сервер должен управлять ими.
        if 'created_at' in kem_data_copy and not isinstance(kem_data_copy['created_at'], str):
            del kem_data_copy['created_at']
        if 'updated_at' in kem_data_copy and not isinstance(kem_data_copy['updated_at'], str):
            del kem_data_copy['updated_at']

        # content должен быть bytes
        if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
            kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')

        return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)

    def _kem_proto_to_dict(self, kem_proto: kem_pb2.KEM) -> dict:
        # Конвертируем content bytes в строку, если это возможно (предполагаем UTF-8)
        # Это для удобства, но может быть не всегда желаемым поведением.
        kem_dict = MessageToDict(kem_proto, preserving_proto_field_name=True, including_default_value_fields=False)
        if 'content' in kem_dict and isinstance(kem_dict['content'], bytes):
            try:
                kem_dict['content'] = kem_dict['content'].decode('utf-8')
            except UnicodeDecodeError:
                # Если не UTF-8, оставляем как есть (base64 строка от MessageToDict) или можно придумать другую логику
                pass
        return kem_dict

    def store_kems(self, kems_data: list[dict]) -> tuple[list[str] | None, int | None, list[str] | None]:
        self._ensure_connected()
        try:
            proto_kems = [self._kem_dict_to_proto(data) for data in kems_data]
            request = glm_service_pb2.StoreKEMsRequest(kems=proto_kems)
            response = self.stub.StoreKEMs(request, timeout=10) # Добавлен таймаут
            return list(response.stored_kem_ids), response.success_count, list(response.error_messages)
        except grpc.RpcError as e:
            print(f"gRPC ошибка при вызове StoreKEMs: {e.code()} - {e.details()}")
            return None, 0, [e.details()]
        except Exception as e:
            print(f"Ошибка при подготовке запроса StoreKEMs: {e}")
            import traceback
            traceback.print_exc()
            return None, 0, [str(e)]

    def retrieve_kems(self, text_query: str = None, embedding_query: list[float] = None,
                      metadata_filters: dict = None, limit: int = 10) -> list[dict] | None:
        self._ensure_connected()
        try:
            query_proto = glm_service_pb2.KEMQuery()
            if text_query:
                query_proto.text_query = text_query
            if embedding_query:
                query_proto.embedding_query.extend(embedding_query)
            if metadata_filters:
                for key, value in metadata_filters.items():
                    query_proto.metadata_filters[key] = str(value)

            request = glm_service_pb2.RetrieveKEMsRequest(query=query_proto, limit=limit)
            response = self.stub.RetrieveKEMs(request, timeout=10) # Добавлен таймаут
            return [self._kem_proto_to_dict(kem) for kem in response.kems]
        except grpc.RpcError as e:
            print(f"gRPC ошибка при вызове RetrieveKEMs: {e.code()} - {e.details()}")
            return None
        except Exception as e:
            print(f"Ошибка при подготовке запроса RetrieveKEMs: {e}")
            return None

    def update_kem(self, kem_id: str, kem_data_update: dict) -> dict | None:
        self._ensure_connected()
        try:
            kem_proto_update = self._kem_dict_to_proto(kem_data_update)
            request = glm_service_pb2.UpdateKEMRequest(kem_id=kem_id, kem_data_update=kem_proto_update)
            response_kem_proto = self.stub.UpdateKEM(request, timeout=10) # Добавлен таймаут
            return self._kem_proto_to_dict(response_kem_proto)
        except grpc.RpcError as e:
            print(f"gRPC ошибка при вызове UpdateKEM: {e.code()} - {e.details()}")
            return None
        except Exception as e:
            print(f"Ошибка при подготовке запроса UpdateKEM: {e}")
            return None

    def delete_kem(self, kem_id: str) -> bool:
        self._ensure_connected()
        try:
            request = glm_service_pb2.DeleteKEMRequest(kem_id=kem_id)
            self.stub.DeleteKEM(request, timeout=10) # Добавлен таймаут
            return True
        except grpc.RpcError as e:
            print(f"gRPC ошибка при вызове DeleteKEM: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            print(f"Ошибка при подготовке запроса DeleteKEM: {e}")
            return False

    def close(self):
        if self.channel:
            self.channel.close()
            self.channel = None
            self.stub = None
            print("Канал GLMClient закрыт.")

    def __enter__(self):
        self.connect() # Подключаемся при входе в контекстный менеджер
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == '__main__':
    # Этот блок кода выполняется только при запуске файла напрямую (python glm_client.py)
    # Он не будет выполняться при импорте GLMClient из другого модуля.
    # Для запуска этого примера требуется, чтобы GLM сервер был запущен на localhost:50051

    # Добавляем родительскую директорию в sys.path, чтобы можно было запустить этот файл напрямую
    # для демонстрации, и чтобы импорты generated_grpc_code работали.
    import os
    import sys
    # sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    # Это не совсем правильно, если generated_grpc_code не является устанавливаемым пакетом.
    # Проще всего запускать пример из корневой директории SDK, добавив ее в PYTHONPATH,
    # или убедившись, что generated_grpc_code устанавливается как часть пакета.

    print("Запуск примера использования GLMClient (требуется запущенный GLM сервер на localhost:50051)")
    print("Этот пример не будет выполнен автоматически при выполнении subtask.")

    if os.getenv("RUN_GLM_CLIENT_EXAMPLE"): # Запускать пример только если установлена переменная окружения
        try:
            with GLMClient() as client:
                # 1. Store KEMs
                print("\n--- Тест StoreKEMs ---")
                kems_to_store = [
                    {"id": "kem_sdk_001", "content_type": "text/plain", "content": "KEM 1 from SDK.", "metadata": {"sdk_source": "python_glm_client", "topic": "sdk_test"}},
                    {"id": "kem_sdk_002", "content_type": "application/json", "content": '{"data_key": "data_value"}', "metadata": {"sdk_source": "python_glm_client", "status": "draft"}, "embeddings": [0.4, 0.5, 0.6]}
                ]
                stored_ids, success_count, errors = client.store_kems(kems_to_store)
                if stored_ids is not None:
                    print(f"Успешно сохранены КЕП с ID: {stored_ids} (Всего: {success_count})")
                if errors and any(e for e in errors if e): # Проверка, что ошибки не пустые строки или None
                    print(f"Ошибки при сохранении: {errors}")

                # 2. Retrieve KEMs
                print("\n--- Тест RetrieveKEMs (по метаданным) ---")
                retrieved_kems_meta = client.retrieve_kems(metadata_filters={"sdk_source": "python_glm_client"}, limit=5)
                if retrieved_kems_meta is not None:
                    print(f"Извлечено {len(retrieved_kems_meta)} КЕП по метаданным:")
                    for k in retrieved_kems_meta:
                        content_preview = k.get('content', '')[:30] if isinstance(k.get('content'), str) else 'N/A'
                        print(f"  ID: {k.get('id')}, Content: {content_preview}...")
                else:
                    print("Не удалось извлечь КЕП по метаданным.")

                # 3. Update KEM (предполагаем, что kem_sdk_001 существует)
                print("\n--- Тест UpdateKEM ---")
                if stored_ids and "kem_sdk_001" in stored_ids:
                    updated_data = {"metadata": {"sdk_source": "python_glm_client_v2", "status": "final"}}
                    updated_kem = client.update_kem("kem_sdk_001", updated_data)
                    if updated_kem:
                        print(f"КЕП kem_sdk_001 обновлена: {updated_kem.get('metadata')}")
                    else:
                        print("Не удалось обновить КЕП kem_sdk_001")
                else:
                    print("Пропуск теста UpdateKEM, т.к. kem_sdk_001 не был сохранен или ID не получен.")

                # 4. Delete KEM (предполагаем, что kem_sdk_002 существует)
                print("\n--- Тест DeleteKEM ---")
                if stored_ids and "kem_sdk_002" in stored_ids:
                    delete_success = client.delete_kem("kem_sdk_002")
                    if delete_success:
                        print("КЕП kem_sdk_002 успешно удалена.")
                    else:
                        print("Не удалось удалить КЕП kem_sdk_002.")
                else:
                    print("Пропуск теста DeleteKEM, т.к. kem_sdk_002 не был сохранен или ID не получен.")
        except Exception as e:
            print(f"Произошла ошибка во время выполнения примера: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("Переменная окружения RUN_GLM_CLIENT_EXAMPLE не установлена, пример не выполняется.")
