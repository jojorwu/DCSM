# dcsm_agent_sdk_python/sdk.py
from .glm_client import GLMClient
from .local_memory import LocalAgentMemory
import typing

class AgentSDK:
    def __init__(self,
                 glm_server_address: str = 'localhost:50051',
                 lpa_max_size: int = 100,
                 lpa_indexed_keys: typing.List[str] = None): # Новый параметр
        print("AgentSDK: Инициализация...")
        self.glm_client = GLMClient(server_address=glm_server_address)
        # Передаем indexed_keys в LocalAgentMemory
        self.local_memory = LocalAgentMemory(max_size=lpa_max_size, indexed_keys=lpa_indexed_keys if lpa_indexed_keys else [])
        # self.swm_client = SWMClient(swm_server_address) # Для будущего расширения
        print(f"AgentSDK: GLMClient и LocalAgentMemory (indexed_keys: {lpa_indexed_keys if lpa_indexed_keys else []}) инициализированы.")

    def query_local_memory(self, metadata_filters: typing.Optional[dict] = None, ids: typing.Optional[list[str]] = None) -> list[dict]:
        """ Запрашивает КЕП из локальной памяти (ЛПА) с возможностью фильтрации. """
        # print(f"AgentSDK: Запрос query_local_memory с metadata_filters={metadata_filters}, ids={ids}")
        # Предполагаем, что LocalAgentMemory теперь имеет метод query
        return self.local_memory.query(metadata_filters=metadata_filters, ids=ids)

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
        # Используем ids_filter для получения КЕП по ID.
        # GLMClient.retrieve_kems теперь возвращает (list[dict] | None, str | None)
        remote_kems_tuple = self.glm_client.retrieve_kems(ids_filter=[kem_id], page_size=1)

        kems_list_candidate = None
        if remote_kems_tuple and remote_kems_tuple[0] is not None:
            kems_list_candidate = remote_kems_tuple[0]

        if isinstance(kems_list_candidate, list) and len(kems_list_candidate) > 0:
            remote_kem = kems_list_candidate[0]
            if isinstance(remote_kem, dict) and remote_kem.get('id') == kem_id:
                self.local_memory.put(kem_id, remote_kem)
                return remote_kem

        # Если мы здесь, значит, КЕП не найдена или ответ некорректен
        # print(f"AgentSDK: КЕП ID '{kem_id}' не найдена в GLM или получен некорректный ответ от GLM (ожидался список КЕП).") # Этот print уже был, но закомментирован
        return None

    def store_kems(self, kems_data: list[dict]) -> tuple[list[dict] | None, list[str] | None, str | None]:
        """Сохраняет пакет КЕП в GLM и обновляет ЛПА актуальными данными с сервера."""
        print("AgentSDK: Запрос store_kems для {} КЕП.".format(len(kems_data)))
        # glm_client.batch_store_kems возвращает (successfully_stored_kems_as_dicts, failed_kem_references, overall_error_message)
        stored_kems_dicts, failed_refs, error_msg = self.glm_client.batch_store_kems(kems_data)

        if stored_kems_dicts:
            print(f"AgentSDK: Успешно сохранено/обновлено {len(stored_kems_dicts)} КЕП в GLM. Обновление ЛПА.")
            for kem_dict in stored_kems_dicts:
                if kem_dict and 'id' in kem_dict: # Убедимся, что есть ID
                    self.local_memory.put(kem_dict['id'], kem_dict)
            # Возвращаем список сохраненных КЕП (dict), список ссылок на неудавшиеся и общее сообщение об ошибке
            return stored_kems_dicts, failed_refs, error_msg
        else:
            print(f"AgentSDK: Не удалось сохранить КЕПы в GLM. Ошибка: {error_msg}")
            return None, failed_refs, error_msg


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
