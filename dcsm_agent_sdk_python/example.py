# dcsm_agent_sdk_python/example.py
import sys
import os
import grpc # Импортируем grpc для обработки RpcError
import time

# --- Начало блока для корректного импорта sdk ---
# Добавляем родительскую директорию dcsm_agent_sdk_python в sys.path,
# если example.py запускается напрямую изнутри этой директории.
# Это позволяет импортировать 'sdk' как модуль.
# В реальном использовании, пакет dcsm_agent_sdk_python должен быть установлен
# или его родительская директория должна быть в PYTHONPATH.

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
# Если dcsm_agent_sdk_python - это пакет, и мы хотим импортировать из него,
# то родительская директория dcsm_agent_sdk_python должна быть в sys.path.
# Для запуска example.py из dcsm_agent_sdk_python, мы можем сделать так:
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
     sys.path.insert(0, project_root) # Добавляет родителя dcsm_agent_sdk_python

# Теперь пробуем импортировать AgentSDK
try:
    from dcsm_agent_sdk_python.sdk import AgentSDK
except ImportError:
    # Если предыдущий импорт не удался (например, запускаем example.py напрямую из его директории)
    # пробуем импортировать sdk как локальный модуль (если sdk.py в той же директории)
    print("Не удалось импортировать 'from dcsm_agent_sdk_python.sdk import AgentSDK', пробую 'from sdk import AgentSDK'")
    from sdk import AgentSDK
# --- Конец блока для корректного импорта sdk ---


def run_example():
    print("Запуск расширенного примера AgentSDK...")
    print("Убедитесь, что GLM сервер запущен на localhost:50051 для этого примера.")
    print("Для реального выполнения установите переменную окружения RUN_SDK_EXAMPLE=true")

    sdk_instance = None # Для использования в блоке finally
    glm_server_address = 'localhost:50051'

    if os.getenv("RUN_SDK_EXAMPLE") != "true":
        print("Переменная окружения RUN_SDK_EXAMPLE не установлена в 'true'. Пример не будет подключаться к серверу.")
        print("Для демонстрации будут вызваны только локальные части SDK, если это возможно.")
        # Можно добавить сюда вызовы sdk.local_memory для демонстрации без сервера
        sdk_instance = AgentSDK(glm_server_address=glm_server_address, lpa_max_size=5)
        sdk_instance.local_memory.put("local_test_001", {"id":"local_test_001", "data":"test"})
        print("Локальная память содержит local_test_001: {}".format(sdk_instance.local_memory.get("local_test_001")))
        sdk_instance.close() # Закрываем, даже если не подключались, для консистентности
        return

    try:
        # Используем контекстный менеджер для SDK
        with AgentSDK(glm_server_address=glm_server_address, lpa_max_size=5) as sdk:
            sdk_instance = sdk # Сохраняем для блока finally

            # 1. Очистим ЛПА для чистоты эксперимента
            sdk.local_memory.clear()
            print("\\n--- ЛПА очищена ---")

            # 2. Данные для КЕП
            kems_data = [
                {"id": "ex_sdk_001", "content_type": "text/plain", "content": "Первая КЕП для примера SDK.", "metadata": {"tag": "example", "version": 1}},
                {"id": "ex_sdk_002", "content_type": "application/json", "content": '{"message": "Вторая КЕП"}', "metadata": {"tag": "example", "format": "json"}, "embeddings": [0.1,0.1]},
                {"id": "ex_sdk_003", "content_type": "text/plain", "content": "Третья КЕП, будет удалена.", "metadata": {"tag": "temp"}},
            ]

            # 3. Сохранение нескольких КЕП
            print("\\n--- Сохранение нескольких КЕП ---")
            stored_ids, count, errors = sdk.store_kems(kems_data)
            if count and count > 0 and stored_ids:
                print("Успешно сохранено {} КЕП. ID: {}".format(count, stored_ids))
                for kem_id in stored_ids:
                    assert sdk.local_memory.contains(kem_id), "КЕП {} должна быть в ЛПА после сохранения!".format(kem_id)
            else:
                print("Ошибка при сохранении КЕП: {}".format(errors if errors else "Нет информации"))
                # Если сохранение не удалось, нет смысла продолжать тесты, зависящие от этих данных
                if not (count and count > 0 and stored_ids) : return


            # 4. Получение одной из КЕП (должна быть взята из ЛПА)
            print("\\n--- Получение ex_sdk_001 (из ЛПА) ---")
            kem1_lpa = sdk.get_kem("ex_sdk_001")
            if kem1_lpa:
                print("Получена (ЛПА) ex_sdk_001: {}".format(kem1_lpa.get("content")))
            else:
                print("КЕП ex_sdk_001 не найдена в ЛПА, хотя должна была быть!")

            # 5. Принудительное получение той же КЕП с сервера
            print("\\n--- Принудительное получение ex_sdk_001 (с сервера) ---")
            kem1_remote = sdk.get_kem("ex_sdk_001", force_remote=True)
            if kem1_remote:
                print("Получена (сервер) ex_sdk_001: {}".format(kem1_remote.get("content")))
            else:
                print("КЕП ex_sdk_001 не найдена на сервере!")

            # 6. Обновление КЕП ex_sdk_002
            print("\\n--- Обновление ex_sdk_002 ---")
            update_payload = {"metadata": {"tag": "example_updated", "format": "json_v2"}, "content": '{"message": "Обновленная вторая КЕП"}'}
            updated_kem2 = sdk.update_kem("ex_sdk_002", update_payload)
            if updated_kem2:
                print("КЕП ex_sdk_002 обновлена. Новое содержимое: '{}', новые метаданные: {}".format(updated_kem2.get("content"), updated_kem2.get("metadata")))
                kem2_lpa_after_update = sdk.local_memory.get("ex_sdk_002")
                assert kem2_lpa_after_update and kem2_lpa_after_update.get("metadata", {}).get("format") == "json_v2", "Обновление ex_sdk_002 не отразилось в ЛПА!"
                print("Проверка ЛПА для ex_sdk_002 после обновления прошла успешно.")
            else:
                print("Не удалось обновить ex_sdk_002.")

            # 7. Удаление КЕП ex_sdk_003
            print("\\n--- Удаление ex_sdk_003 ---")
            deleted = sdk.delete_kem("ex_sdk_003")
            if deleted:
                print("КЕП ex_sdk_003 успешно удалена.")
                assert not sdk.local_memory.contains("ex_sdk_003"), "КЕП ex_sdk_003 все еще в ЛПА!"
                assert sdk.get_kem("ex_sdk_003") is None, "Удаленная КЕП ex_sdk_003 все еще получается через get_kem!"
                print("Проверки удаления для ex_sdk_003 прошли успешно.")
            else:
                print("Не удалось удалить ex_sdk_003.")

            # 8. Демонстрация LRU в ЛПА (размер ЛПА = 5)
            print("\\n--- Демонстрация LRU в ЛПА (max_size=5) ---")
            for i in range(1, 7):
                kem_id = "kem_lru_{}".format(i)
                sdk.local_memory.put(kem_id, {"id": kem_id, "content": "LRU test {}".format(i)})

            print("Размер ЛПА после добавления 6 элементов (max 5): {}".format(sdk.local_memory.current_size))
            assert sdk.local_memory.current_size == 5, "Размер ЛПА не соответствует max_size."
            assert not sdk.local_memory.contains("kem_lru_1"), "kem_lru_1 (самая старая) должна была быть вытеснена."
            assert sdk.local_memory.contains("kem_lru_2"), "kem_lru_2 должна быть в ЛПА."
            assert sdk.local_memory.contains("kem_lru_6"), "kem_lru_6 (самая новая) должна быть в ЛПА."
            print("Проверки LRU в ЛПА прошли успешно.")

            print("\\n--- Пример завершен успешно! ---")

    except grpc.RpcError as e:
        print("!!!!!!!!!! gRPC ОШИБКА !!!!!!!!!")
        print("Не удалось подключиться к GLM серверу или произошла другая ошибка gRPC.")
        if sdk_instance and sdk_instance.glm_client:
             print("Адрес сервера: {}".format(sdk_instance.glm_client.server_address))
        else:
             print("Адрес сервера: {}".format(glm_server_address))
        print("Пожалуйста, убедитесь, что GLM сервер запущен.")
        print("Код ошибки: {}".format(e.code()))
        print("Детали: {}".format(e.details()))
    except Exception as e:
        print("Произошла непредвиденная ошибка в примере: {}".format(e))
        import traceback
        traceback.print_exc()
    # Блок finally не нужен, так как используется контекстный менеджер 'with AgentSDK(...)'

if __name__ == '__main__':
    run_example()
