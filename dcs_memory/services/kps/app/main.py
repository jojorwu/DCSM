import grpc
from concurrent import futures
import sys
import os
import logging # uuid was already removed, SentenceTransformer is used.
import typing # Added back for Optional type hint
from sentence_transformers import SentenceTransformer

# Import configuration
from .config import KPSConfig

# Global configuration instance
config = KPSConfig()

# --- Logging Setup ---
logging.basicConfig(
    level=config.get_log_level_int(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
# --- End Logging Setup ---

# --- gRPC Code Import Block ---
current_script_path = os.path.abspath(__file__)
app_dir_kps = os.path.dirname(current_script_path)
service_root_dir_kps = os.path.dirname(app_dir_kps)

if service_root_dir_kps not in sys.path:
    sys.path.insert(0, service_root_dir_kps)

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2 # For GLM client
from generated_grpc import glm_service_pb2_grpc # For GLM client
from generated_grpc import kps_service_pb2 # For KPS server
from generated_grpc import kps_service_pb2_grpc # For KPS server
# Import retry decorator
from dcs_memory.common.grpc_utils import retry_grpc_call
# --- End gRPC Code Import Block ---

class KnowledgeProcessorServiceImpl(kps_service_pb2_grpc.KnowledgeProcessorServiceServicer):
    def __init__(self):
        logger.info("Initializing KnowledgeProcessorServiceImpl...")
        self.glm_channel = None
        self.glm_stub = None
        self.embedding_model: typing.Optional[SentenceTransformer] = None # Type hint
        self.config = config # Store config instance for easy access

        # Retry parameters for GLM client calls can be accessed via self.config if defined in KPSConfig
        # For now, assuming retry_grpc_call uses its own defaults or global ones from grpc_utils
        # self.retry_max_attempts = getattr(self.config, "GLM_RETRY_MAX_ATTEMPTS", 3)
        # self.retry_initial_delay_s = getattr(self.config, "GLM_RETRY_INITIAL_DELAY_S", 1.0)
        # self.retry_backoff_factor = getattr(self.config, "GLM_RETRY_BACKOFF_FACTOR", 2.0)

        try:
            logger.info(f"Loading sentence-transformer model: {self.config.SENTENCE_TRANSFORMER_MODEL}...")
            self.embedding_model = SentenceTransformer(self.config.SENTENCE_TRANSFORMER_MODEL)

            # Perform a test encoding to check model and get vector dimension
            test_embedding = self.embedding_model.encode(["test"])[0]
            model_vector_size = len(test_embedding)
            logger.info(f"Model {self.config.SENTENCE_TRANSFORMER_MODEL} loaded. Vector dimension: {model_vector_size}")

            if model_vector_size != self.config.DEFAULT_VECTOR_SIZE:
                logger.warning(
                    f"WARNING: Model vector dimension ({model_vector_size}) "
                    f"does not match KPS_DEFAULT_VECTOR_SIZE ({self.config.DEFAULT_VECTOR_SIZE}) from config. "
                    f"This may lead to issues when storing in GLM if GLM's vector DB is configured differently."
                )
        except Exception as e:
            logger.error(f"Error loading sentence-transformer model '{self.config.SENTENCE_TRANSFORMER_MODEL}': {e}", exc_info=True)
            self.embedding_model = None # Ensure model is None if loading failed

        try:
            self.glm_channel = grpc.insecure_channel(self.config.GLM_SERVICE_ADDRESS)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.glm_channel)
            logger.info(f"GLM client for KPS initialized, target address: {self.config.GLM_SERVICE_ADDRESS}")
        except Exception as e:
            logger.error(f"Error initializing GLM client in KPS: {e}")
            self.glm_stub = None # Ensure stub is None if channel creation failed

    @retry_grpc_call
    def _glm_store_kem_with_retry(self, request: glm_service_pb2.StoreKEMRequest, timeout: int = 10) -> glm_service_pb2.StoreKEMResponse:
        if not self.glm_stub:
            logger.error("KPS._glm_store_kem_with_retry: GLM stub is not initialized. This is a KPS internal configuration error.")
            # This indicates a setup/configuration problem within KPS.
            # Raising RuntimeError as it's an internal state issue, not a gRPC communication failure at this point.
            raise RuntimeError("KPS Internal Error: GLM client stub not available for storing KEM.")
        return self.glm_stub.StoreKEM(request, timeout=timeout)

    def ProcessRawData(self, request: kps_service_pb2.ProcessRawDataRequest, context) -> kps_service_pb2.ProcessRawDataResponse:
        logger.info(f"KPS: ProcessRawData called for data_id='{request.data_id}', content_type='{request.content_type}'")

        if not self.glm_stub:
            msg = "GLM service is unavailable to KPS (GLM client not initialized)."
            logger.error(msg)
            context.abort(grpc.StatusCode.INTERNAL, msg)
            # This return is technically unreachable due to abort, but good for linters/type checkers.
            return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)

        try:
            content_to_embed = ""
            is_text_content = False
            if request.content_type.startswith("text/"):
                try:
                    content_to_embed = request.raw_content.decode('utf-8')
                    is_text_content = True
                except UnicodeDecodeError:
                    msg = f"Error decoding raw_content as UTF-8 for content_type='{request.content_type}'"
                    logger.error(msg)
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
                    return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)
            else:
                logger.info(f"Content type '{request.content_type}' is not text; embeddings will not be generated by the current model.")

            embeddings = []
            if is_text_content and self.embedding_model:
                try:
                    logger.info(f"Generating embeddings for text (length: {len(content_to_embed)})...")
                    embeddings_np = self.embedding_model.encode([content_to_embed]) # encode expects a list
                    embeddings = embeddings_np[0].tolist()
                    logger.info(f"Embeddings generated by model (dimension: {len(embeddings)}).")

                    if len(embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                        msg = (f"Critical error: Model '{self.config.SENTENCE_TRANSFORMER_MODEL}' returned embedding of dimension {len(embeddings)}, "
                               f"but DEFAULT_VECTOR_SIZE from KPS config is {self.config.DEFAULT_VECTOR_SIZE}. "
                               f"Ensure this matches GLM's vector database configuration.")
                        logger.error(msg)
                        context.abort(grpc.StatusCode.INTERNAL, "Embedding dimension configuration mismatch.")
                        return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)
                except Exception as e:
                    msg = f"Error during embedding generation by model: {e}"
                    logger.error(msg, exc_info=True)
                    context.abort(grpc.StatusCode.INTERNAL, msg)
                    return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)
            elif is_text_content and not self.embedding_model:
                logger.warning("Embedding model not loaded; embeddings will not be generated for text content.")

            kem_to_store = kem_pb2.KEM(
                content_type=request.content_type,
                content=request.raw_content, # Storing raw bytes
                embeddings=embeddings,
                metadata=request.initial_metadata # Assuming initial_metadata is map<string,string>
            )
            # If data_id was provided in the request, add it to metadata for traceability
            if request.data_id:
                kem_to_store.metadata["source_data_id"] = request.data_id

            logger.info("KPS: Calling GLM.StoreKEM (with retry logic)...")
            store_kem_request = glm_service_pb2.StoreKEMRequest(kem=kem_to_store)

            try:
                store_kem_response = self._glm_store_kem_with_retry(store_kem_request, timeout=15) # Increased timeout for GLM store

                if store_kem_response and store_kem_response.kem and store_kem_response.kem.id:
                    kem_id_from_glm = store_kem_response.kem.id
                    logger.info(f"KPS: KEM successfully stored in GLM with ID: {kem_id_from_glm}")
                    return kps_service_pb2.ProcessRawDataResponse(
                        kem_id=kem_id_from_glm,
                        success=True,
                        status_message=f"KEM successfully processed and stored with ID: {kem_id_from_glm}"
                    )
                else:
                    # This case should ideally be an exception from _glm_store_kem_with_retry if something went wrong.
                    msg = "Error: GLM.StoreKEM did not return an expected KEM object or KEM ID."
                    logger.error(msg)
                    # Return a clear failure if the response is not as expected after successful RPC.
                    return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=msg)
            except grpc.RpcError as e_glm:
                logger.error(f"KPS: gRPC error from GLM after retries: code={e_glm.code()}, details={e_glm.details()}")
                return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=f"Error interacting with GLM: {e_glm.details()}")
            except Exception as e_other_after_glm: # Catch any other unexpected errors after GLM call
                 logger.error(f"KPS: Unexpected error after GLM call in ProcessRawData: {e_other_after_glm}", exc_info=True)
                 context.abort(grpc.StatusCode.INTERNAL, f"Internal KPS error after GLM call: {e_other_after_glm}")
                 return kps_service_pb2.ProcessRawDataResponse(success=False, status_message=f"Internal KPS error: {e_other_after_glm}")

        except Exception as e: # Broad exception catch for the entire method
            logger.error(f"KPS: Unexpected error in ProcessRawData: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, "Internal KPS error during data processing.")
            return kps_service_pb2.ProcessRawDataResponse(success=False, status_message="Internal KPS error.")

    def __del__(self):
        if self.glm_channel:
            self.glm_channel.close()
            logger.info("GLM client channel in KPS closed.")

def serve():
    logger.info(f"KPS Configuration: GLM Address={config.GLM_SERVICE_ADDRESS}, KPS Listen Address={config.GRPC_LISTEN_ADDRESS}, "
                f"Sentence Transformer Model={config.SENTENCE_TRANSFORMER_MODEL}, Default Vector Size={config.DEFAULT_VECTOR_SIZE}")

    servicer_instance = KnowledgeProcessorServiceImpl()

    if not servicer_instance.embedding_model:
        logger.warning("KPS server starting WITHOUT an embedding model. Text embeddings will not be generated.")
    if not servicer_instance.glm_stub:
        logger.warning("KPS server starting WITHOUT a GLM CLIENT. KEM persistence will be impossible.")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kps_service_pb2_grpc.add_KnowledgeProcessorServiceServicer_to_server(
        servicer_instance, server
    )
    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS)
    logger.info(f"Starting KPS (Knowledge Processor Service) on {config.GRPC_LISTEN_ADDRESS}...")
    server.start()
    logger.info(f"KPS started and listening for connections on {config.GRPC_LISTEN_ADDRESS}.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Stopping KPS server...")
    finally:
        server.stop(None) # Graceful stop
        logger.info("KPS server stopped.")

if __name__ == '__main__':
    serve()
