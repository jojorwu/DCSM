# dcsm_agent_sdk_python/sdk.py
from .glm_client import GLMClient
from .local_memory import LocalAgentMemory
import typing

class AgentSDK:
    def __init__(self, glm_server_address: str = 'localhost:50051', lpa_max_size: int = 100):
        print("AgentSDK: Инициализация...")
        self.glm_client = GLMClient(server_address=glm_server_address)
        self.local_memory = LocalAgentMemory(max_size=lpa_max_size)
        # self.swm_client = SWMClient(swm_server_address) # Для будущего расширения
        print("AgentSDK: GLMClient и LocalAgentMemory инициализированы.")

    def get_kem(self, kem_id: str, force_remote: bool = False) -> typing.Optional[dict]:
        print("AgentSDK: Запрос get_kem для ID '{}', force_remote={}".format(kem_id, force_remote))
        if not force_remote:
            cached_kem = self.local_memory.get(kem_id)
            if cached_kem:
                print("AgentSDK: КЕП ID '{}' найдена в ЛПА.".format(kem_id))
                return cached_kem

        print("AgentSDK: КЕП ID '{}' не найдена в ЛПА или запрошен принудительный удаленный вызов. Обращение к GLM...".format(kem_id))
        # GLMClient.retrieve_kems ожидает фильтры, для получения по ID можно использовать metadata_filters
        # или добавить специальный метод get_kem_by_id в GLMClient, если GLM сервис его поддерживает.
        # Пока используем retrieve_kems с фильтром по ID.
        # Предполагаем, что ID КЕП уникален и retrieve_kems вернет список из 0 или 1 элемента.
        remote_kem_list = self.glm_client.retrieve_kems(metadata_filters={'id': kem_id}, limit=1)

        if remote_kem_list and len(remote_kem_list) > 0:
            remote_kem = remote_kem_list[0]
            # Дополнительная проверка, что ID действительно совпадает (на случай неточной фильтрации на сервере)
            if remote_kem.get('id') == kem_id:
                print("AgentSDK: КЕП ID '{}' получена от GLM. Кэширование в ЛПА.".format(kem_id))
                self.local_memory.put(kem_id, remote_kem)
                return remote_kem
            else:
                # Это маловероятно, если сервер правильно обрабатывает фильтр по 'id'
                print("AgentSDK: Получена КЕП от GLM, но ID не совпадает ({} != {}). КЕП не возвращена и не кэширована.".format(remote_kem.get('id'), kem_id))
                return None
        else:
            print("AgentSDK: КЕП ID '{}' не найдена в GLM.".format(kem_id))
            return None

    def store_kems(self, kems_data: list[dict]) -> tuple[typing.Optional[list[str]], typing.Optional[int], typing.Optional[list[str]]]:
        print("AgentSDK: Запрос store_kems для {} КЕП.".format(len(kems_data)))
        stored_ids, success_count, errors = self.glm_client.store_kems(kems_data)

        if success_count and success_count > 0 and stored_ids:
            # Создадим карту исходных КЕП по их ID для легкого доступа
            # Убедимся, что у всех КЕП в kems_data есть 'id', или они не смогут быть замаплены
            sent_kems_map = {}
            for kem_d in kems_data:
                kem_d_id = kem_d.get('id')
                if kem_d_id:
                    sent_kems_map[kem_d_id] = kem_d
                else:
                    # Если ID не было, сервер должен был его сгенерировать.
                    # В этом случае мы не можем просто замапить по ID из kems_data.
                    # Для простоты, текущая реализация store_kems в GLMClient предполагает,
                    # что ID предоставляются клиентом или сервер их как-то возвращает/связывает.
                    # Ответ StoreKEMsResponse содержит stored_kem_ids.
                    # Если ID генерируются сервером, нам нужен способ сопоставить их с исходными данными.
                    # Пока предполагаем, что ID были в kems_data.
                    print("AgentSDK: Предупреждение - КЕП без ID в исходных данных store_kems: {}".format(kem_d))


            for kem_id in stored_ids:
                if kem_id in sent_kems_map:
                    self.local_memory.put(kem_id, sent_kems_map[kem_id])
                    print("AgentSDK: КЕП ID '{}' (сохраненная в GLM) обновлена в ЛПА.".format(kem_id))
                else:
                    # Это может случиться, если ID был сгенерирован сервером и не совпадает с предложенным клиентом,
                    # или если ID не было в исходных данных.
                    print("AgentSDK: Предупреждение - КЕП ID '{}' сохранена в GLM, но не найдена в исходных данных для обновления ЛПА по этому ID.".format(kem_id))

        return stored_ids, success_count, errors

    def update_kem(self, kem_id: str, kem_data_update: dict) -> typing.Optional[dict]:
        print("AgentSDK: Запрос update_kem для ID '{}'.".format(kem_id))
        updated_kem_on_server = self.glm_client.update_kem(kem_id, kem_data_update)
        if updated_kem_on_server:
            print("AgentSDK: КЕП ID '{}' успешно обновлена в GLM. Обновление ЛПА.".format(kem_id))
            self.local_memory.put(kem_id, updated_kem_on_server) # Кэшируем то, что вернул сервер
        else:
            print("AgentSDK: Не удалось обновить КЕП ID '{}' в GLM.".format(kem_id))
        return updated_kem_on_server

    def delete_kem(self, kem_id: str) -> bool:
        print("AgentSDK: Запрос delete_kem для ID '{}'.".format(kem_id))
        success = self.glm_client.delete_kem(kem_id)
        if success:
            print("AgentSDK: КЕП ID '{}' успешно удалена из GLM. Удаление из ЛПА.".format(kem_id))
            self.local_memory.delete(kem_id)
        else:
            print("AgentSDK: Не удалось удалить КЕП ID '{}' из GLM.".format(kem_id))
        return success

    def close(self):
        print("AgentSDK: Закрытие соединений...")
        if self.glm_client:
            self.glm_client.close()
        print("AgentSDK: Соединения закрыты.")

    def __enter__(self):
        if self.glm_client:
            self.glm_client.connect() # Убедимся, что клиент подключен при входе в контекст
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == '__main__':
    print("Запуск примера использования AgentSDK (требуется запущенный GLM сервер на localhost:50051)")
    print("Этот пример не будет выполнен автоматически при выполнении subtask.")
    print("Для локального теста установите переменную окружения RUN_AGENT_SDK_EXAMPLE=true")

    import os
    if os.getenv("RUN_AGENT_SDK_EXAMPLE") == "true":
        try:
            # Используем контекстный менеджер для SDK
            with AgentSDK() as sdk:
                # Пример: Сохранение КЕП
                kem_to_store1 = {"id": "sdk_kem_main_001", "content_type": "text/plain", "content": "Первая КЕП через SDK (main)", "metadata": {"source":"sdk_example_main", "version":"1"}}
                kem_to_store2 = {"id": "sdk_kem_main_002", "content_type": "text/plain", "content": "Вторая КЕП через SDK (main)", "metadata": {"source":"sdk_example_main", "version":"1"}}

                print("\\n--- Сохранение КЕП ---")
                ids, count, errors = sdk.store_kems([kem_to_store1, kem_to_store2])
                if count and count > 0:
                    print("Успешно сохранено {} КЕП с ID: {}".format(count, ids))
                else:
                    print("Не удалось сохранить КЕП. Ошибки: {}".format(errors))

                # Пример: Получение КЕП
                print("\\n--- Получение sdk_kem_main_001 (сначала из ЛПА, если есть) ---")
                kem = sdk.get_kem("sdk_kem_main_001")
                if kem:
                    print("Получена КЕП sdk_kem_main_001: '{}'".format(kem.get("content")))
                else:
                    print("КЕП sdk_kem_main_001 не найдена.")

                print("\\n--- Принудительное получение sdk_kem_main_001 с сервера ---")
                kem_forced = sdk.get_kem("sdk_kem_main_001", force_remote=True)
                if kem_forced:
                    print("Принудительно получена КЕП sdk_kem_main_001: '{}'".format(kem_forced.get("content")))
                else:
                    print("КЕП sdk_kem_main_001 не найдена на сервере (принудительно).")

                print("\\n--- Получение несуществующей КЕП ---")
                non_existent_kem = sdk.get_kem("non_existent_id_123")
                if non_existent_kem:
                    print("Это не должно было произойти! Получена несуществующая КЕП.")
                else:
                    print("Несуществующая КЕП non_existent_id_123 не найдена, как и ожидалось.")

                print("\\n--- Обновление sdk_kem_main_001 ---")
                if ids and "sdk_kem_main_001" in ids: # Проверяем, что КЕП была сохранена
                    update_data = {"metadata": {"source":"sdk_example_main", "version":"2", "status": "updated"}}
                    updated = sdk.update_kem("sdk_kem_main_001", update_data)
                    if updated:
                        print("КЕП sdk_kem_main_001 обновлена. Новые метаданные: {}".format(updated.get("metadata")))
                        lpa_kem = sdk.local_memory.get("sdk_kem_main_001")
                        print("КЕП sdk_kem_main_001 в ЛПА (метаданные): {}".format(lpa_kem.get("metadata") if lpa_kem else "Не найдена"))
                    else:
                        print("Не удалось обновить sdk_kem_main_001.")
                else:
                    print("Пропуск обновления sdk_kem_main_001, так как она не была успешно сохранена.")


                print("\\n--- Удаление sdk_kem_main_002 ---")
                if ids and "sdk_kem_main_002" in ids: # Проверяем, что КЕП была сохранена
                    deleted = sdk.delete_kem("sdk_kem_main_002")
                    if deleted:
                        print("КЕП sdk_kem_main_002 удалена.")
                        assert sdk.local_memory.get("sdk_kem_main_002") is None, "КЕП sdk_kem_main_002 все еще в ЛПА!"
                        assert sdk.get_kem("sdk_kem_main_002") is None, "КЕП sdk_kem_main_002 все еще доступна через get_kem!"
                    else:
                        print("Не удалось удалить sdk_kem_main_002.")
                else:
                    print("Пропуск удаления sdk_kem_main_002, так как она не была успешно сохранена.")

            print("\nAgentSDK пример завершен.")
        except Exception as e:
            print("Произошла ошибка во время выполнения примера AgentSDK: {}".format(e))
            import traceback
            traceback.print_exc()
    else:
        print("Переменная окружения RUN_AGENT_SDK_EXAMPLE не установлена в 'true', пример не выполняется.")
