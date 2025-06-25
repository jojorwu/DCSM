import unittest
import uuid
import time
import threading
import queue

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import grpc
from integration_tests.kps_client_simple import process_data_via_kps # Для создания КЕП в GLM
from dcs_memory.services.swm.generated_grpc import swm_service_pb2
from dcs_memory.services.swm.generated_grpc import swm_service_pb2_grpc
from dcs_memory.common.grpc_protos.kem_pb2 import KEM # Используем напрямую из общего proto
from dcs_memory.common.grpc_protos.glm_service_pb2 import KEMQuery
from google.protobuf.timestamp_pb2 import Timestamp


KPS_SERVICE_URL = os.getenv("KPS_SERVICE_URL", "localhost:50052")
SWM_SERVICE_URL = os.getenv("SWM_SERVICE_URL", "localhost:50053")

def create_kem_proto_for_swm_publish(id_str: str, metadata: dict = None, content_str: str = "content") -> KEM:
    kem = KEM(id=id_str, content_type="text/plain", content=content_str.encode('utf-8'))
    if metadata:
        for k, v in metadata.items():
            kem.metadata[k] = str(v)
    # Timestamps будут установлены сервером SWM или GLM
    return kem

class TestSWMPubSubIntegration(unittest.TestCase):

    def _subscribe_and_collect_in_thread(self, agent_id: str, topics_filter_criteria: list[str], event_queue: queue.Queue, stop_event: threading.Event, context_active_flag: list):
        """Запускает подписчика в отдельном потоке."""

        # context_active_flag - это список с одним булевым значением, чтобы его можно было менять из основного потока
        mock_context = MagicMock()

        def is_active_side_effect():
            return context_active_flag[0]
        mock_context.is_active.side_effect = is_active_side_effect

        sub_topics = [swm_service_pb2.SubscriptionTopic(filter_criteria=crit) for crit in topics_filter_criteria]
        request = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id=agent_id, topics=sub_topics)

        try:
            with grpc.insecure_channel(SWM_SERVICE_URL) as channel:
                stub = swm_service_pb2_grpc.SharedWorkingMemoryServiceStub(channel)
                print(f"Thread {agent_id}: Подписка на события...")
                for event in stub.SubscribeToSWMEvents(request, timeout=None): # Таймаут управляется context.is_active
                    print(f"Thread {agent_id}: Получено событие: {event.event_type} для KEM ID {event.kem_payload.id}")
                    event_queue.put(event)
                    if stop_event.is_set(): # Проверяем флаг остановки после каждого события
                        context_active_flag[0] = False # Говорим gRPC stream завершиться
                        break
                print(f"Thread {agent_id}: Цикл событий завершен (контекст неактивен или stop_event).")
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                print(f"Thread {agent_id}: Стрим был отменен (ожидаемо при остановке).")
            else:
                print(f"Thread {agent_id}: gRPC ошибка в подписчике: {e.code()} - {e.details()}")
        except Exception as e:
            print(f"Thread {agent_id}: Неожиданная ошибка в подписчике: {e}")
        finally:
            print(f"Thread {agent_id}: Завершение работы.")


    def test_publish_and_receive_event(self):
        print(f"--- Запуск Интеграционного Теста: SWM Pub/Sub - Publish Event ---")
        agent_id = "pubsub_agent_1"
        event_q = queue.Queue()
        stop_subscriber_event = threading.Event()
        context_active = [True] # Используем список для передачи изменяемого флага

        subscriber_thread = threading.Thread(
            target=self._subscribe_and_collect_in_thread,
            args=(agent_id, [], event_q, stop_subscriber_event, context_active) # Пустые фильтры - получаем все
        )
        subscriber_thread.start()
        time.sleep(0.5) # Даем время на установку подписки

        # Публикуем КЕП через SWM
        kem_to_publish = create_kem_proto_for_swm_publish("pubsub_kem1", {"test_meta": "value1"})
        publish_request = swm_service_pb2.PublishKEMToSWMRequest(
            kem_to_publish=kem_to_publish,
            persist_to_glm_if_new_or_updated=False # Не будем сейчас тестировать GLM persistency
        )

        received_event = None
        with grpc.insecure_channel(SWM_SERVICE_URL) as channel:
            stub = swm_service_pb2_grpc.SharedWorkingMemoryServiceStub(channel)
            print("Main: Публикация KEM 'pubsub_kem1'...")
            publish_response = stub.PublishKEMToSWM(publish_request, timeout=10)
            self.assertTrue(publish_response.published_to_swm)
            print(f"Main: KEM '{publish_response.kem_id}' опубликована.")

            # Пытаемся получить событие из очереди
            try:
                received_event = event_q.get(timeout=2.0) # Ожидаем событие
            except queue.Empty:
                print("Main: Событие не было получено от подписчика вовремя.")

        stop_subscriber_event.set() # Сигнализируем потоку подписчика о завершении
        context_active[0] = False   # Также деактивируем контекст
        subscriber_thread.join(timeout=3.0)
        self.assertFalse(subscriber_thread.is_alive(), "Поток подписчика не завершился.")

        self.assertIsNotNone(received_event, "Подписчик не получил событие.")
        if received_event:
            self.assertEqual(received_event.event_type, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
            self.assertEqual(received_event.kem_payload.id, "pubsub_kem1")
            self.assertEqual(received_event.kem_payload.metadata["test_meta"], "value1")

        print(f"--- Тест SWM Pub/Sub - Publish Event УСПЕШНО ЗАВЕРШЕН ---")

    # TODO: Добавить тесты для KEM_UPDATED, KEM_EVICTED с использованием Pub/Sub
    # TODO: Добавить тесты на фильтрацию событий (по kem_id, по metadata)

if __name__ == '__main__':
    print("Запуск интеграционного теста test_swm_pubsub_integration.py")
    print(f"Ожидается, что SWM слушает на {SWM_SERVICE_URL}")
    print("Ожидается, что GLM и Qdrant также запущены (хотя не все тесты их напрямую используют).")
    unittest.main()
