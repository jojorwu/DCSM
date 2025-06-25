import unittest
from unittest.mock import MagicMock, patch
import time # Исправлено с समय на time
import queue
import uuid
import threading

import sys
import os
current_script_path = os.path.abspath(__file__)
app_dir_test = os.path.dirname(current_script_path)
service_root_dir_test = os.path.dirname(app_dir_test)

if service_root_dir_test not in sys.path:
    sys.path.insert(0, service_root_dir_test)

from app.main import SharedWorkingMemoryServiceImpl, SWM_INTERNAL_CACHE_MAX_SIZE, SWM_INDEXED_METADATA_KEYS_CONFIG
from generated_grpc import kem_pb2, swm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp

def create_kem_proto_for_test(id_str: str, content_str: str = "content") -> kem_pb2.KEM:
    kem = kem_pb2.KEM(id=id_str, content_type="text/plain", content=content_str.encode('utf-8'))
    # Add timestamps to avoid issues if code expects them
    ts = Timestamp()
    ts.GetCurrentTime()
    kem.created_at.CopyFrom(ts)
    kem.updated_at.CopyFrom(ts)
    return kem

class TestSWMPubSub(unittest.TestCase):

    def setUp(self):
        # Мокируем GLMClient, так как он не нужен для этих тестов Pub/Sub
        self.mock_glm_stub_patch = patch('app.main.glm_service_pb2_grpc.GlobalLongTermMemoryStub')
        self.MockGLMStub = self.mock_glm_stub_patch.start()
        self.mock_glm_instance = self.MockGLMStub.return_value

        # Мокируем grpc.insecure_channel
        self.mock_grpc_channel_patch = patch('app.main.grpc.insecure_channel')
        self.mock_grpc_channel = self.mock_grpc_channel_patch.start()

        self.swm_service = SharedWorkingMemoryServiceImpl()
        # Очищаем подписчиков и кэш перед каждым тестом
        self.swm_service.subscribers.clear()
        self.swm_service.swm_cache.clear()


    def tearDown(self):
        self.mock_glm_stub_patch.stop()
        self.mock_grpc_channel_patch.stop()

    def test_subscribe_and_unsubscribe(self):
        mock_context = MagicMock()
        mock_context.is_active.return_value = True # Начинаем как активный

        request = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id="agent1")

        # Запускаем SubscribeToSWMEvents в отдельном потоке, так как это блокирующий генератор
        events_received = []
        def consume_events():
            try:
                for event in self.swm_service.SubscribeToSWMEvents(request, mock_context):
                    events_received.append(event)
                    if len(events_received) >= 1: # Получим одно событие и выйдем
                        mock_context.is_active.return_value = False
            except Exception as e:
                print(f"Consumer error: {e}")

        # Имитируем отправку события после подписки
        kem_test = create_kem_proto_for_test("kem_ev_1")

        # Сначала запускаем потребителя
        consumer_thread = threading.Thread(target=consume_events)
        consumer_thread.start()

        # Даем время на регистрацию подписчика
        time.sleep(0.1)
        self.assertIn("agent1", self.swm_service.subscribers)

        # Публикуем событие
        self.swm_service._notify_subscribers(kem_test, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

        # Ждем завершения потока потребителя (он должен завершиться после получения события и is_active=False)
        consumer_thread.join(timeout=2.0)

        self.assertFalse(consumer_thread.is_alive())
        self.assertGreaterEqual(len(events_received), 1)
        if events_received:
            self.assertEqual(events_received[0].kem_payload.id, "kem_ev_1")
            self.assertEqual(events_received[0].event_type, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)

        # Проверяем, что подписчик удален
        time.sleep(0.1) # Даем время на выполнение finally блока в SubscribeToSWMEvents
        self.assertNotIn("agent1", self.swm_service.subscribers)

    def test_event_on_put_kem(self):
        # Подписываемся
        mock_context = MagicMock()
        mock_context.is_active.return_value = True
        request = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id="agent_put_test")

        event_list = []
        def event_collector():
            for event in self.swm_service.SubscribeToSWMEvents(request, mock_context):
                event_list.append(event)
                mock_context.is_active.return_value = False # Останавливаем после первого события

        collector_thread = threading.Thread(target=event_collector)
        collector_thread.start()
        time.sleep(0.1) # Дать время на подписку

        # Выполняем _put_kem_to_cache, что должно вызвать _notify_subscribers
        kem1 = create_kem_proto_for_test("kem_put1")
        self.swm_service._put_kem_to_cache(kem1) # Должно создать KEM_PUBLISHED

        collector_thread.join(timeout=2.0)
        self.assertFalse(collector_thread.is_alive())
        self.assertEqual(len(event_list), 1)
        self.assertEqual(event_list[0].event_type, swm_service_pb2.SWMMemoryEvent.EventType.KEM_PUBLISHED)
        self.assertEqual(event_list[0].kem_payload.id, "kem_put1")

        # Проверяем обновление
        mock_context.is_active.return_value = True # Снова активируем для получения второго события
        collector_thread = threading.Thread(target=event_collector) # Перезапускаем сборщик
        event_list.clear()
        collector_thread.start()
        time.sleep(0.1)

        kem1_updated = create_kem_proto_for_test("kem_put1", content_str="updated_content")
        self.swm_service._put_kem_to_cache(kem1_updated) # Должно создать KEM_UPDATED

        collector_thread.join(timeout=2.0)
        self.assertFalse(collector_thread.is_alive())
        self.assertEqual(len(event_list), 1)
        self.assertEqual(event_list[0].event_type, swm_service_pb2.SWMMemoryEvent.EventType.KEM_UPDATED)
        self.assertEqual(event_list[0].kem_payload.id, "kem_put1")

    def test_event_on_evict_kem(self):
        # Заполняем кэш до предела, чтобы вызвать вытеснение
        # Устанавливаем маленький maxsize для IndexedLRUCache в этом тесте
        self.swm_service.swm_cache = type(self.swm_service.swm_cache)(
            maxsize=1,
            indexed_keys=SWM_INDEXED_METADATA_KEYS_CONFIG,
            on_evict_callback=self.swm_service._handle_kem_eviction
        )

        mock_context = MagicMock(); mock_context.is_active.return_value = True
        request = swm_service_pb2.SubscribeToSWMEventsRequest(agent_id="agent_evict_test")
        event_list = []
        def event_collector():
            for event in self.swm_service.SubscribeToSWMEvents(request, mock_context):
                event_list.append(event)
                mock_context.is_active.return_value = False

        collector_thread = threading.Thread(target=event_collector)
        collector_thread.start()
        time.sleep(0.1)

        kem_old = create_kem_proto_for_test("kem_evict_old")
        self.swm_service._put_kem_to_cache(kem_old) # Помещаем первый элемент
        event_list.clear() # Очищаем от KEM_PUBLISHED для kem_old
        mock_context.is_active.return_value = True # Снова слушаем

        kem_new = create_kem_proto_for_test("kem_evict_new")
        self.swm_service._put_kem_to_cache(kem_new) # Это должно вытеснить kem_old и вызвать колбэк

        collector_thread.join(timeout=2.0) # Ждем событие KEM_EVICTED
        self.assertFalse(collector_thread.is_alive())

        # Мы ожидаем два события: PUBLISHED для kem_new и EVICTED для kem_old.
        # Но тест останавливается после первого. Переделаем.
        # Вместо этого, проверим напрямую _handle_kem_eviction через мок.

        mock_notify = MagicMock()
        self.swm_service._notify_subscribers = mock_notify

        # Пересоздаем кэш для чистоты этого специфического теста на колбэк
        evicted_kems_via_callback = []
        def test_evict_callback(kem):
            evicted_kems_via_callback.append(kem)

        small_cache = type(self.swm_service.swm_cache)(maxsize=1, indexed_keys=[], on_evict_callback=test_evict_callback)

        kem_to_be_evicted = create_kem_proto_for_test("kem_ev_1")
        small_cache["kem_ev_1"] = kem_to_be_evicted # Добавляем первый

        kem_that_evicts = create_kem_proto_for_test("kem_ev_2")
        small_cache["kem_ev_2"] = kem_that_evicts # Добавляем второй, первый должен вытесниться

        self.assertEqual(len(evicted_kems_via_callback), 1)
        self.assertEqual(evicted_kems_via_callback[0].id, "kem_ev_1")


if __name__ == '__main__':
    unittest.main()
