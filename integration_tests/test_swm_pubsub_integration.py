import unittest
from unittest.mock import MagicMock # For mocking context in subscriber thread
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
# from dcs_memory.common.grpc_protos.kem_pb2 import KEM
# from dcs_memory.common.grpc_protos.glm_service_pb2 import KEMQuery
# Using AgentSDK's generated code as a consistent source for shared proto messages in tests
from dcsm_agent_sdk_python.generated_grpc_code.kem_pb2 import KEM
from dcsm_agent_sdk_python.generated_grpc_code.glm_service_pb2 import KEMQuery
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

    def _subscribe_and_collect_in_thread(self, agent_id: str, topics_filter_criteria: list[str],
                                         event_queue: queue.Queue, stop_event: threading.Event,
                                         ready_event: threading.Event): # Added ready_event
        """Запускает подписчика в отдельном потоке и сигнализирует о готовности."""

        sub_topics = [swm_service_pb2.SubscriptionTopic(filter_criteria=crit) for crit in topics_filter_criteria]
        request = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id=agent_id, topics=sub_topics)
        stream = None
        try:
            with grpc.insecure_channel(SWM_SERVICE_URL) as channel:
                stub = swm_service_pb2_grpc.SharedWorkingMemoryServiceStub(channel)
                print(f"Thread {agent_id}: Подписка на события...")
                stream = stub.SubscribeToSWMEvents(request) # No method-level timeout for server streams typically
                ready_event.set() # Signal that subscription is initiated
                print(f"Thread {agent_id}: Подписка установлена, готов к приему событий.")
                for event in stream:
                    print(f"Thread {agent_id}: Получено событие: {event.event_type} для KEM ID {event.kem_payload.id}")
                    event_queue.put(event)
                    if stop_event.is_set():
                        if hasattr(stream, 'cancel') and callable(stream.cancel):
                            stream.cancel()
                        break
                print(f"Thread {agent_id}: Цикл событий завершен.")
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.CANCELLED:
                print(f"Thread {agent_id}: Стрим был отменен.")
            else:
                print(f"Thread {agent_id}: gRPC ошибка в подписчике: {e.code()} - {e.details()}")
                event_queue.put(e) # Put error onto queue so main thread can see it
        except Exception as e:
            print(f"Thread {agent_id}: Неожиданная ошибка в подписчике: {e}")
        finally:
            print(f"Thread {agent_id}: Завершение работы.")


    def test_publish_and_receive_event(self):
        print(f"--- Запуск Интеграционного Теста: SWM Pub/Sub - Publish Event ---")
        agent_id = "pubsub_agent_1"
        event_q = queue.Queue()
        stop_subscriber_event = threading.Event()
        subscriber_ready_event = threading.Event() # New event for readiness

        subscriber_thread = threading.Thread(
            target=self._subscribe_and_collect_in_thread,
            args=(agent_id, [], event_q, stop_subscriber_event, subscriber_ready_event)
        )
        subscriber_thread.daemon = True # Ensure thread doesn't block exit
        subscriber_thread.start()

        # Wait for subscriber to be ready
        print("Main: Ожидание готовности подписчика...")
        ready_flag = subscriber_ready_event.wait(timeout=5.0) # Wait up to 5s
        self.assertTrue(ready_flag, "Подписчик не стал готов вовремя.")
        print("Main: Подписчик готов.")

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
            elif isinstance(received_event, grpc.RpcError):
                self.fail(f"Подписчик получил ошибку вместо события: {received_event.code()} - {received_event.details()}")


        print("Main: Остановка подписчика...")
        stop_subscriber_event.set()
        # context_active_flag[0] = False # No longer needed
        subscriber_thread.join(timeout=5.0) # Increased join timeout slightly
        if subscriber_thread.is_alive():
            print("Main Warning: Поток подписчика не завершился вовремя после join().")
            # Attempt to cancel the stream if possible (if stream object was accessible)
            # This might be complex if the stream object is not shared from the thread.
            # For now, rely on channel closure when 'with' block exits if thread is stuck.

        self.assertFalse(subscriber_thread.is_alive(), "Поток подписчика должен был завершиться.")
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
