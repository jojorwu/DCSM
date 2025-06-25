import grpc
from concurrent import futures
import time
import sys
import os
import uuid
import logging
from sentence_transformers import SentenceTransformer

# Импортируем конфигурацию
from .config import KPSConfig

# Глобальный экземпляр конфигурации
config = KPSConfig()

# --- Настройка логирования ---
logging.basicConfig(
    level=config.get_log_level_int(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
# --- Конец настройки логирования ---

# --- Начало блока для корректного импорта сгенерированного кода ---
current_script_path = os.path.abspath(__file__)
app_dir_kps = os.path.dirname(current_script_path)
service_root_dir_kps = os.path.dirname(app_dir_kps)

if service_root_dir_kps not in sys.path:
    sys.path.insert(0, service_root_dir_kps)

# Ensure generated_grpc itself is in sys.path for internal imports in _pb2.py files
# This is typically handled by an __init__.py in generated_grpc, but can be done here too if needed.
# For KPS, generate_grpc_code.sh creates generated_grpc/__init__.py.
# We will assume that __init__.py in dcs_memory/services/kps/generated_grpc/ handles its own internal paths
# (e.g., by adding itself to sys.path, as done for GLM's generated_grpc).

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2 # For GLM client
from generated_grpc import glm_service_pb2_grpc # For GLM client
from generated_grpc import kps_service_pb2 # For KPS server
from generated_grpc import kps_service_pb2_grpc # For KPS server
from generated_grpc import kps_service_pb2 # For KPS server
from generated_grpc import kps_service_pb2_grpc # For KPS server
# Импортируем retry декоратор
from dcs_memory.common.grpc_utils import retry_grpc_call # <--- Новый импорт
# --- Конец блока для корректного импорта ---

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

# --- Конфигурация ---
GLM_SERVICE_ADDRESS_CONFIG = os.getenv("GLM_SERVICE_ADDRESS", "localhost:50051")
# Параметры Retry для GLM клиента внутри KPS
KPS_GLM_RETRY_MAX_ATTEMPTS = int(os.getenv("KPS_GLM_RETRY_MAX_ATTEMPTS", 3))
KPS_GLM_RETRY_INITIAL_DELAY_S = float(os.getenv("KPS_GLM_RETRY_INITIAL_DELAY_S", 1.0))
KPS_GLM_RETRY_BACKOFF_FACTOR = float(os.getenv("KPS_GLM_RETRY_BACKOFF_FACTOR", 2.0))

KPS_GRPC_LISTEN_ADDRESS_CONFIG = os.getenv("KPS_GRPC_LISTEN_ADDRESS", "[::]:50052")
SENTENCE_TRANSFORMER_MODEL_CONFIG = os.getenv("SENTENCE_TRANSFORMER_MODEL", "all-MiniLM-L6-v2")
KPS_DEFAULT_VECTOR_SIZE = int(os.getenv("DEFAULT_VECTOR_SIZE", 384))
# --- Конец конфигурации ---

class KnowledgeProcessorServiceImpl(kps_service_pb2_grpc.KnowledgeProcessorServiceServicer):
    def __init__(self):
        logger.info("Инициализация KnowledgeProcessorServiceImpl...")
        self.glm_channel = None
        self.glm_stub = None
        self.embedding_model = None

        # Параметры Retry для вызовов к GLM
        self.retry_max_attempts = KPS_GLM_RETRY_MAX_ATTEMPTS
        self.retry_initial_delay_s = KPS_GLM_RETRY_INITIAL_DELAY_S
        self.retry_backoff_factor = KPS_GLM_RETRY_BACKOFF_FACTOR

        try:
            logger.info(f"Загрузка модели sentence-transformer: {SENTENCE_TRANSFORMER_MODEL_CONFIG}...")
            # Specify cache folder to avoid issues in restricted environments if possible
            # model_cache_dir = os.path.join(app_dir_kps, ".model_cache") # Or a shared cache location
            # os.makedirs(model_cache_dir, exist_ok=True)
            # self.embedding_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL_CONFIG, cache_folder=model_cache_dir)
            self.embedding_model = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL_CONFIG)

            test_embedding = self.embedding_model.encode(["test"])[0]
            model_vector_size = len(test_embedding)
            logger.info(f"Модель {SENTENCE_TRANSFORMER_MODEL_CONFIG} загружена. Размерность векторов: {model_vector_size}")
            if model_vector_size != KPS_DEFAULT_VECTOR_SIZE:
                logger.warning(
                    f"ВНИМАНИЕ: Размерность векторов модели ({model_vector_size}) "
                    f"не совпадает с KPS_DEFAULT_VECTOR_SIZE ({KPS_DEFAULT_VECTOR_SIZE}). "
                )
        except Exception as e:
            logger.error(f"Ошибка при загрузке модели sentence-transformer '{SENTENCE_TRANSFORMER_MODEL_CONFIG}': {e}", exc_info=True)

        try:
            self.glm_channel = grpc.insecure_channel(GLM_SERVICE_ADDRESS_CONFIG)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.glm_channel)
            logger.info(f"GLM клиент для KPS инициализирован, целевой адрес: {GLM_SERVICE_ADDRESS_CONFIG}")
        except Exception as e:
            logger.error(f"Ошибка при инициализации GLM клиента в KPS: {e}")

    @retry_grpc_call
    def _glm_store_kem_with_retry(self, request: glm_service_pb2.StoreKEMRequest, timeout: int = 10) -> glm_service_pb2.StoreKEMResponse:
        if not self.glm_stub:
            logger.error("KPS._glm_store_kem_with_retry: GLM stub не инициализирован.")
            # Это должно быть обработано как фатальная ошибка конфигурации
            raise grpc.RpcError("GLM stub not available in KPS")
        return self.glm_stub.StoreKEM(request, timeout=timeout)

    def ProcessRawData(self, request: kps_service_pb2.ProcessRawDataRequest, context):
        logger.info("KPS: ProcessRawData вызван для data_id='{}', content_type='{}'".format(request.data_id, request.content_type))

        if not self.glm_stub:
            msg = "GLM сервис недоступен для KPS (клиент не инициализирован)."
            logger.error(msg)
            context.abort(grpc.StatusCode.INTERNAL, msg)
            return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)

        try:
            content_to_embed = ""
            is_text_content = False
            if request.content_type.startswith("text/"):
                try:
                    content_to_embed = request.raw_content.decode('utf-8')
                    is_text_content = True
                except UnicodeDecodeError:
                    msg = "Ошибка декодирования raw_content как UTF-8 для content_type='{}'".format(request.content_type)
                    logger.error(msg)
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
                    return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)
            else:
                logger.info("Тип контента '{}' не является текстовым, эмбеддинги не будут сгенерированы этой моделью.".format(request.content_type))

            embeddings = []
            if is_text_content and self.embedding_model:
                try:
                    logger.info("Генерация эмбеддингов для текста (длина: {})...".format(len(content_to_embed)))
                    embeddings_np = self.embedding_model.encode([content_to_embed])
                    embeddings = embeddings_np[0].tolist()
                    logger.info("Эмбеддинги сгенерированы моделью (размерность: {}).".format(len(embeddings)))

                    if len(embeddings) != self.config.DEFAULT_VECTOR_SIZE: # Используем self.config
                        msg = (f"Критическая ошибка: Модель '{self.config.SENTENCE_TRANSFORMER_MODEL}' вернула эмбеддинг размерности {len(embeddings)}, "
                               f"но DEFAULT_VECTOR_SIZE из конфигурации равна {self.config.DEFAULT_VECTOR_SIZE}.")
                        logger.error(msg)
                        context.abort(grpc.StatusCode.INTERNAL, "Ошибка конфигурации размерности эмбеддингов.")
                        return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)
                except Exception as e:
                    msg = "Ошибка при генерации эмбеддингов моделью: {}".format(e)
                    logger.error(msg, exc_info=True)
                    context.abort(grpc.StatusCode.INTERNAL, msg)
                    return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)
            elif is_text_content and not self.embedding_model:
                logger.warning("Модель эмбеддингов не загружена, эмбеддинги не будут сгенерированы для текстового контента.")

            kem_to_store = kem_pb2.KEM(
                content_type=request.content_type,
                content=request.raw_content,
                embeddings=embeddings,
                metadata=request.initial_metadata
            )
            if request.data_id:
                kem_to_store.metadata["source_data_id"] = request.data_id

            logger.info("KPS: Вызов GLM.StoreKEM (с retry)...")
            store_kem_request = glm_service_pb2.StoreKEMRequest(kem=kem_to_store)

            try:
                store_kem_response = self._glm_store_kem_with_retry(store_kem_request, timeout=10)
                if store_kem_response and store_kem_response.kem and store_kem_response.kem.id:
                    kem_id_from_glm = store_kem_response.kem.id
                    logger.info("KPS: КЕП успешно сохранена в GLM с ID: {}".format(kem_id_from_glm))
                    return kps_service_pb2.ProcessRawDataResponse(
                        kem_id=kem_id_from_glm,
                        success=True,
                        status_message="КЕП успешно обработана и сохранена с ID: {}".format(kem_id_from_glm)
                    )
                else:
                    msg = "Ошибка: GLM.StoreKEM не вернул ожидаемый ответ или ID КЕП."
                    logger.error(msg)
                    return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)
            # Ошибки grpc.RpcError теперь обрабатываются декоратором _glm_store_kem_with_retry
            # и будут проброшены, если все попытки неудачны.
            # Этот try-блок ловит ошибки, которые могут возникнуть ПОСЛЕ успешного вызова _glm_store_kem_with_retry
            # или если _glm_store_kem_with_retry вернул None (хотя он должен пробрасывать ошибку).
            # Корректнее будет, если ProcessRawData также будет обернут в try-except для RpcError,
            # которые могут быть проброшены из _glm_store_kem_with_retry.
            except grpc.RpcError as e_glm: # Ловим RpcError, проброшенный из _glm_store_kem_with_retry
                logger.error(f"KPS: gRPC ошибка от GLM после retry: code={e_glm.code()}, details={e_glm.details()}")
                return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=f"Ошибка взаимодействия с GLM: {e_glm.details()}")
            except Exception as e_other_after_glm: # Другие ошибки после вызова GLM
                 logger.error(f"KPS: Ошибка после вызова GLM в ProcessRawData: {e_other_after_glm}", exc_info=True)
                 context.abort(grpc.StatusCode.INTERNAL, f"Внутренняя ошибка KPS после вызова GLM: {e_other_after_glm}")
                 return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=f"Внутренняя ошибка KPS: {e_other_after_glm}")

        except Exception as e: # Общий try-except для всего метода ProcessRawData
            logger.error("KPS: Непредвиденная ошибка в ProcessRawData: {}".format(e), exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, "Внутренняя ошибка KPS при обработке данных.")
            return kps_service_pb2.ProcessRawDataResponse(success=False, status_message="Внутренняя ошибка KPS.")

    def __del__(self):
        if self.glm_channel:
            self.glm_channel.close()
            logger.info("Канал GLM клиента в KPS закрыт.")

def serve():
    # Используем глобальный объект config
    logger.info(f"Конфигурация KPS: GLM Адрес={config.GLM_SERVICE_ADDRESS}, KPS Адрес={config.GRPC_LISTEN_ADDRESS}, Модель={config.SENTENCE_TRANSFORMER_MODEL}, Размер вектора={config.DEFAULT_VECTOR_SIZE}")

    servicer_instance = KnowledgeProcessorServiceImpl()

    if not servicer_instance.embedding_model:
        logger.warning("Сервер KPS запускается БЕЗ модели эмбеддингов. Текстовые эмбеддинги не будут генерироваться.")
    if not servicer_instance.glm_stub:
        logger.warning("Сервер KPS запускается БЕЗ КЛИЕНТА GLM. Сохранение КЕП будет невозможно.")


    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kps_service_pb2_grpc.add_KnowledgeProcessorServiceServicer_to_server(
        servicer_instance, server
    )
    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS) # Используем config
    logger.info(f"Запуск KPS (Knowledge Processor Service) на {config.GRPC_LISTEN_ADDRESS}...")
    server.start()
    logger.info(f"KPS запущен и ожидает соединений на {config.GRPC_LISTEN_ADDRESS}.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Остановка KPS...")
    finally:
        server.stop(None)
        logger.info("KPS остановлен.")

if __name__ == '__main__':
    serve()
