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
from pythonjsonlogger import jsonlogger # Import for JSON logging

config = KPSConfig()

# --- Logging Setup ---
def setup_logging(log_config: KPSConfig): # Changed type hint
    handlers_list = []

    if log_config.LOG_OUTPUT_MODE in ["json_stdout", "json_file"]:
        json_fmt_str = getattr(log_config, 'LOG_JSON_FORMAT', log_config.LOG_FORMAT)
        formatter = jsonlogger.JsonFormatter(fmt=json_fmt_str, datefmt=log_config.LOG_DATE_FORMAT)
    else: # For "stdout", "file"
        formatter = logging.Formatter(fmt=log_config.LOG_FORMAT, datefmt=log_config.LOG_DATE_FORMAT)

    if log_config.LOG_OUTPUT_MODE in ["stdout", "json_stdout"]:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        handlers_list.append(stream_handler)

    if log_config.LOG_OUTPUT_MODE in ["file", "json_file"]:
        if log_config.LOG_FILE_PATH:
            try:
                file_handler = logging.FileHandler(log_config.LOG_FILE_PATH)
                file_handler.setFormatter(formatter)
                handlers_list.append(file_handler)
            except Exception as e:
                print(f"Error setting up file logger at {log_config.LOG_FILE_PATH}: {e}. Falling back to stdout.", file=sys.stderr)
                if not any(isinstance(h, logging.StreamHandler) for h in handlers_list):
                    stream_handler_fallback = logging.StreamHandler(sys.stdout)
                    stream_handler_fallback.setFormatter(formatter)
                    handlers_list.append(stream_handler_fallback)
        else:
            print(f"LOG_OUTPUT_MODE is '{log_config.LOG_OUTPUT_MODE}' but LOG_FILE_PATH is not set. Defaulting to stdout.", file=sys.stderr)
            if not handlers_list:
                stream_handler_default = logging.StreamHandler(sys.stdout)
                stream_handler_default.setFormatter(formatter)
                handlers_list.append(stream_handler_default)

    if not handlers_list:
        print("Warning: No logging handlers configured. Defaulting to basic stdout.", file=sys.stderr)
        logging.basicConfig(level=log_config.get_log_level_int())
        return

    logging.basicConfig(
        level=log_config.get_log_level_int(),
        handlers=handlers_list,
        force=True
    )
    # Note: format and datefmt in basicConfig are less relevant when handlers are specified with their own formatters.

setup_logging(config)
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
from dcs_memory.common.config import KPSConfig # Ensure KPSConfig is imported for type hint

# Attempt to import pybreaker, make it optional
try:
    import pybreaker
except ImportError:
    pybreaker = None

# For Health Checks
from grpc_health.v1 import health_pb2 as health_pb2_types
from grpc_health.v1 import health_pb2_grpc as health_pb2_grpc_health
from grpc_health.v1.health_pb2 import HealthCheckRequest as HC_Request
# --- End gRPC Code Import Block ---

class KnowledgeProcessorServiceImpl(kps_service_pb2_grpc.KnowledgeProcessorServiceServicer):
    def __init__(self):
        logger.info("Initializing KnowledgeProcessorServiceImpl...")
        self.config: KPSConfig = config
        self.glm_channel = None
        self.glm_stub = None
        self.embedding_model: typing.Optional[SentenceTransformer] = None
        self._model_loaded_successfully: bool = False # Health check flag

        self.glm_circuit_breaker: typing.Optional[pybreaker.CircuitBreaker] = None
        if pybreaker and self.config.CIRCUIT_BREAKER_ENABLED:
            self.glm_circuit_breaker = pybreaker.CircuitBreaker(
                fail_max=self.config.CIRCUIT_BREAKER_FAIL_MAX,
                reset_timeout=self.config.CIRCUIT_BREAKER_RESET_TIMEOUT_S
            )
            logger.info(f"KPS: GLM client circuit breaker enabled: fail_max={self.config.CIRCUIT_BREAKER_FAIL_MAX}, reset_timeout={self.config.CIRCUIT_BREAKER_RESET_TIMEOUT_S}s")
        else:
            logger.info(f"KPS: GLM client circuit breaker is disabled (pybreaker not installed or CIRCUIT_BREAKER_ENABLED=False).")

        try:
            logger.info(f"Loading sentence-transformer model: {self.config.EMBEDDING_MODEL_NAME}...")
            self.embedding_model = SentenceTransformer(self.config.EMBEDDING_MODEL_NAME)

            # Perform a test encoding to check model and get vector dimension
            test_embedding = self.embedding_model.encode(["test"])[0]
            model_vector_size = len(test_embedding)
            logger.info(f"Model {self.config.EMBEDDING_MODEL_NAME} loaded. Vector dimension: {model_vector_size}")

            if model_vector_size != self.config.DEFAULT_VECTOR_SIZE:
                logger.warning(
                    f"WARNING: Model vector dimension ({model_vector_size}) "
                    f"does not match KPS_DEFAULT_VECTOR_SIZE ({self.config.DEFAULT_VECTOR_SIZE}) from config. "
                    f"This may lead to issues when storing in GLM if GLM's vector DB is configured differently."
                )
            self._model_loaded_successfully = True
        except Exception as e:
            logger.error(f"Error loading sentence-transformer model '{self.config.EMBEDDING_MODEL_NAME}': {e}", exc_info=True)
            self.embedding_model = None
            self._model_loaded_successfully = False

        # GLM Client for Health Check (separate, simpler channel)
        self.glm_health_check_channel: typing.Optional[grpc.Channel] = None
        self.glm_health_check_stub: typing.Optional[health_pb2_grpc_health.HealthStub] = None # Correct stub type
        if self.config.GLM_SERVICE_ADDRESS:
            try:
                # Options for health check client channel can be simpler or use a subset of main client options
                health_check_grpc_options = [
                    ('grpc.keepalive_time_ms', self.config.GRPC_KEEPALIVE_TIME_MS), # Reuse for consistency
                    ('grpc.keepalive_timeout_ms', self.config.GRPC_KEEPALIVE_TIMEOUT_MS),
                ]
                self.glm_health_check_channel = grpc.insecure_channel(self.config.GLM_SERVICE_ADDRESS, options=health_check_grpc_options)
                self.glm_health_check_stub = health_pb2_grpc_health.HealthStub(self.glm_health_check_channel)
                logger.info(f"KPS: GLM health check client initialized for target: {self.config.GLM_SERVICE_ADDRESS}")
            except Exception as e_hc_client:
                logger.error(f"KPS: Error initializing GLM health check client: {e_hc_client}", exc_info=True)

        try: # Main GLM client for operations
            grpc_options = [
                ('grpc.keepalive_time_ms', self.config.GRPC_KEEPALIVE_TIME_MS),
                ('grpc.keepalive_timeout_ms', self.config.GRPC_KEEPALIVE_TIMEOUT_MS),
                ('grpc.keepalive_permit_without_calls', 1 if self.config.GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS else 0),
                ('grpc.http2.min_ping_interval_without_data_ms', self.config.GRPC_HTTP2_MIN_PING_INTERVAL_WITHOUT_DATA_MS),
                ('grpc.max_receive_message_length', self.config.GRPC_MAX_RECEIVE_MESSAGE_LENGTH),
                ('grpc.max_send_message_length', self.config.GRPC_MAX_SEND_MESSAGE_LENGTH),
            ]
            if self.config.GRPC_CLIENT_LB_POLICY:
                grpc_options.append(('grpc.lb_policy_name', self.config.GRPC_CLIENT_LB_POLICY))

            target_address = self.config.GLM_SERVICE_ADDRESS
            # Ensure GRPC_DNS_RESOLVER=ares is set in the environment for dns:/// scheme to work reliably with round_robin.
            # No code change here for GRPC_DNS_RESOLVER, it's an environment setup.
            logger.info(f"KPS: GLM client attempting to connect to: {target_address} with LB policy: {self.config.GRPC_CLIENT_LB_POLICY or 'default (pick_first)'}")

            self.glm_channel = grpc.insecure_channel(target_address, options=grpc_options)
            self.glm_stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.glm_channel)
            logger.info(f"GLM client for KPS initialized, target: {target_address}, options: {grpc_options}")
        except Exception as e:
            logger.error(f"Error initializing GLM client in KPS: {e}", exc_info=True)
            self.glm_stub = None # Ensure stub is None if channel creation failed

    @retry_grpc_call(
        max_attempts=config.RETRY_MAX_ATTEMPTS, # These are now correctly sourced from config by the decorator through the instance
        initial_delay_s=config.RETRY_INITIAL_DELAY_S,
        backoff_factor=config.RETRY_BACKOFF_FACTOR,
        jitter_fraction=config.RETRY_JITTER_FRACTION,
        # Pass the instance's circuit breaker and its enabled status
        # Note: The decorator needs access to these. This implies the decorator should be applied
        # to an instance method where `self` is available, or these need to be passed differently if
        # the decorator is at class level or on static methods.
        # Current application to instance method `_glm_store_kem_with_retry` is fine for `self.glm_circuit_breaker`.
        # However, the decorator parameters are evaluated at definition time.
        # This means we need to pass `self.glm_circuit_breaker` and `self.config.CIRCUIT_BREAKER_ENABLED`
        # to the decorator when it's applied, or the decorator needs to fetch them from `self`.
        # Let's adjust how the decorator is called or how it accesses these.
        #
        # For simplicity, the decorator will take them as arguments.
        # The KPSConfig object (`config`) is global in this file.
        # The circuit breaker needs to be specific to this client instance.
        # This requires applying the decorator dynamically or accessing `self` from within the decorator.
        #
        # Let's make the decorator slightly smarter or pass the CB from the method call itself.
        # Simpler: The decorator is already defined. We need to ensure it *can* receive the CB.
        # The call to the decorator should be:
        # @retry_grpc_call(..., circuit_breaker=self.glm_circuit_breaker, cb_enabled=self.config.CIRCUIT_BREAKER_ENABLED)
        # This cannot be done directly as self is not available at decorator application time for the method.
        #
        # Alternative: The method itself fetches the CB and passes it to a helper.
        # Or, the decorator is modified to expect `self` as first arg if it's a method.
        #
        # Easiest path: Modify how the decorator is applied in the KPS class.
        # We will need to initialize the CB in __init__ and then use it.
        # The decorator parameters `circuit_breaker` and `cb_enabled` are new.
        # The retry_grpc_call in grpc_utils.py is now updated to accept these.
        # The KPS service's method needs to be decorated such that these are passed.
        # This means the decorator needs to be applied in a way that it has access to `self.config` and `self.glm_circuit_breaker`
        #
        # One way: The decorator itself, if the decorated function is a method of a class that has `config` and `glm_circuit_breaker` attributes,
        # could try to access them.
        # A cleaner way for now: The decorated method will be a wrapper that calls an inner method with CB.
        # No, this is also complex.
        #
        # Let's assume the decorator will be modified slightly if it's applied to a method,
        # to try to get `circuit_breaker` and `cb_enabled` from `self` (the first arg).
        # For now, I'll update the call here, assuming the decorator in grpc_utils can handle it.
        # The `config` object used for retry params is the global one.
        #
        # The decorator `retry_grpc_call` is defined globally in `grpc_utils`.
        # When KPS's `_glm_store_kem_with_retry` is decorated, the decorator's parameters
        # (max_attempts etc.) get fixed based on the global `config` at that point (KPSConfig).
        # To pass instance-specific circuit breaker, the decorator has to be applied to the instance's method
        # or the method itself has to use the circuit breaker.
        #
        # Let's assume the `retry_grpc_call` decorator will be applied like this:
        # No, the decorator parameters are fixed at decoration time.
        #
        # Simplest: The method itself uses the circuit breaker.
        # The retry decorator will then wrap the CB-protected call.
        # The retry_grpc_call decorator has been updated to accept circuit_breaker and cb_enabled.
        # So, KPS needs to instantiate the CB and pass it.
        # The decorator itself does not have access to `self`.
        #
        # The retry decorator is applied at method definition.
        # The parameters to the decorator (like max_attempts) are evaluated then.
        # We cannot pass `self.glm_circuit_breaker` to the decorator at definition time.
        #
        # This means the circuit breaker logic must be invoked *inside* the decorated method,
        # or the decorator itself must be more complex (e.g., a class-based decorator that gets `self`).
        #
        # Let's modify the method to use the circuit breaker internally,
        # and the retry decorator will wrap this.
        # The retry decorator in grpc_utils has already been modified to accept CB.
        # The challenge is how to pass the instance's CB to the globally defined decorator.
        #
        # The solution is to apply the decorator in __init__ if we want instance-specific CBs.
        # Or, make the decorator itself aware of instance attributes if it's decorating a method.
        #
        # Re-evaluating: The decorator parameters `circuit_breaker` and `cb_enabled` were added to `grpc_utils.py`.
        # The call site of the decorator in KPS needs to pass these.
        # Since `config` is global in `kps/main.py`, `cb_enabled=config.CIRCUIT_BREAKER_ENABLED` is fine.
        # But `circuit_breaker=self.glm_circuit_breaker` is not.
        #
        # The most straightforward way is that the decorated function (`_glm_store_kem_with_retry`)
        # becomes a simple wrapper that calls another method which is then protected by the CB.
        # OR the CB logic is directly inside `_glm_store_kem_with_retry` before the actual gRPC call.
        #
        # Let's try making the method use the CB internally for now.
        # The retry decorator will then wrap this CB-protected call.
        # The `protected_func` inside the retry decorator will become the one that uses the CB.
        # This means the `retry_grpc_call` decorator does not need the `circuit_breaker` and `cb_enabled` params directly.
        # It will simply retry the function it's given. That function, in turn, will use the CB.
        # This seems cleaner. I will revert the `grpc_utils.py` changes for CB params in decorator signature
        # and instead put the CB logic into the service methods.
        #
        # *** Re-Correction ***:
        # The `pybreaker.CircuitBreaker.call(func, *args, **kwargs)` pattern is what we need.
        # The `retry_grpc_call` should indeed take the `circuit_breaker` and `cb_enabled` args.
        # The KPS service will instantiate its `self.glm_circuit_breaker`.
        # The problem is applying the decorator: `@retry_grpc_call(..., circuit_breaker=self.glm_circuit_breaker)`.
        # This requires `self` at definition time.
        #
        # Solution: We can't use the decorator with instance-specific CBs directly on the method.
        # Instead, the method will explicitly call a function that is then decorated,
        # or the method will implement the retry/CB logic itself.
        #
        # Let's keep the retry decorator as is (already modified to accept CB).
        # In KPS `__init__`, we will store the CB.
        # In `_glm_store_kem_with_retry`, we will call the `self.glm_stub.StoreKEM` via the CB.
        # The retry decorator will wrap `_glm_store_kem_with_retry`.
        # The retry decorator needs to be passed the CB instance.
        # This is still the core issue.
        #
        # The simplest way for now, given the existing structure:
        # The retry decorator will be applied as is. The circuit breaker logic will be *inside* the
        # `_glm_store_kem_with_retry` method, wrapping the `self.glm_stub.StoreKEM` call.
        # This means the retry decorator will *not* be aware of the CB directly.
        # If the CB is open, `_glm_store_kem_with_retry` will raise `CircuitBreakerError`.
        # The retry decorator will catch this. We need to ensure `CircuitBreakerError` is NOT retryable by the retry_decorator.
        # This means `RETRYABLE_ERROR_CODES` in `grpc_utils.py` must not include `CircuitBreakerError`.
        # And the retry decorator should specifically let `CircuitBreakerError` propagate.
        # This is already handled in the modified `grpc_utils.py`.

        # So, the decorator call remains as is, configured by global `config`.
        # The CB logic is added *inside* the method.
    )
    def _glm_store_kem_with_retry(self, request: glm_service_pb2.StoreKEMRequest, timeout: int) -> glm_service_pb2.StoreKEMResponse:
        if not self.glm_stub:
            logger.error("KPS._glm_store_kem_with_retry: GLM stub is not initialized.")
            raise RuntimeError("KPS Internal Error: GLM client stub not available.")

        def actual_glm_call():
            return self.glm_stub.StoreKEM(request, timeout=timeout)

        if self.glm_circuit_breaker and self.config.CIRCUIT_BREAKER_ENABLED:
            try:
                return self.glm_circuit_breaker.call(actual_glm_call)
            except pybreaker.CircuitBreakerError as e:
                logger.error(f"KPS: GLM Circuit Breaker open for StoreKEM: {e}")
                # Map to a gRPC error or a specific KPS exception if needed by callers
                # For now, re-raising will be caught by the retry decorator's generic Exception handler
                # or propagate up if not caught by retry decorator's RpcError specific handler.
                # The retry decorator was already modified to let pybreaker.CircuitBreakerError propagate.
                raise
        else: # Circuit breaker disabled or not available
            return actual_glm_call()
        # The line below was a leftover from a previous merge, actual_glm_call already returns this.
        # return self.glm_stub.StoreKEM(request, timeout=timeout)

    @retry_grpc_call(
        max_attempts=config.RETRY_MAX_ATTEMPTS,
        initial_delay_s=config.RETRY_INITIAL_DELAY_S,
        backoff_factor=config.RETRY_BACKOFF_FACTOR,
        jitter_fraction=config.RETRY_JITTER_FRACTION
    )
    def _glm_retrieve_kems_with_retry(self, request: glm_service_pb2.RetrieveKEMsRequest, timeout: int) -> glm_service_pb2.RetrieveKEMsResponse:
        if not self.glm_stub:
            logger.error("KPS._glm_retrieve_kems_with_retry: GLM stub is not initialized.")
            raise RuntimeError("KPS Internal Error: GLM client stub not available.")

        def actual_glm_call():
            return self.glm_stub.RetrieveKEMs(request, timeout=timeout)

        if self.glm_circuit_breaker and self.config.CIRCUIT_BREAKER_ENABLED:
            try:
                return self.glm_circuit_breaker.call(actual_glm_call)
            except pybreaker.CircuitBreakerError as e: # type: ignore
                logger.error(f"KPS: GLM Circuit Breaker open for RetrieveKEMs: {e}")
                raise
        else:
            return actual_glm_call()

    def _check_model_loaded(self) -> bool:
        return self.embedding_model is not None and self._model_loaded_successfully

    def _check_glm_connectivity(self) -> bool:
        if not self.glm_health_check_stub:
            logger.warning("KPS Health Check: GLM health check client not available.")
            return False
        try:
            timeout = getattr(self.config, "HEALTH_CHECK_GLM_TIMEOUT_S", 2.0)
            health_check_req = HC_Request(service="") # Check overall GLM health
            # Note: GLM server itself needs to have grpc_health_checking enabled.
            # This assumes GLM's health check endpoint is standard.
            response = self.glm_health_check_stub.Check(health_check_req, timeout=timeout)
            if response.status == health_pb2_types.HealthCheckResponse.SERVING:
                return True
            else:
                logger.warning(f"KPS Health Check: GLM reported status {health_pb2_types.HealthCheckResponse.ServingStatus.Name(response.status)}.")
                return False
        except grpc.RpcError as e:
            logger.warning(f"KPS Health Check: GLM connectivity check failed with gRPC error: Code={e.code()}, Details='{e.details()}'.")
            return False
        except Exception as e_glm_check: # Catch other potential errors during check
            logger.warning(f"KPS Health Check: GLM connectivity check failed with unexpected error: {e_glm_check}")
            return False

    def check_overall_health(self) -> health_pb2_types.HealthCheckResponse.ServingStatus:
        model_ok = self._check_model_loaded()
        glm_ok = self._check_glm_connectivity()

        if model_ok and glm_ok:
            return health_pb2_types.HealthCheckResponse.SERVING

        if not model_ok:
             logger.warning("KPS Health Status: Potentially NOT_SERVING due to embedding model not loaded.")
        if not glm_ok: # Only log GLM failure if it's configured
             logger.warning("KPS Health Status: Potentially NOT_SERVING due to GLM connectivity issue.")
        return health_pb2_types.HealthCheckResponse.NOT_SERVING


    def ProcessRawData(self, request: kps_service_pb2.ProcessRawDataRequest, context) -> kps_service_pb2.ProcessRawDataResponse:
        logger.info(f"KPS: ProcessRawData called for data_id='{request.data_id}', content_type='{request.content_type}'")

        if not self.glm_stub:
            msg = "KPS Misconfiguration: GLM service client not initialized."
            logger.error(msg)
            # This is a setup issue for KPS itself.
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)
            return kps_service_pb2.ProcessRawDataResponse() # Unreachable

        if not self.embedding_model:
            msg = "KPS Misconfiguration: Embedding model not loaded."
            logger.error(msg)
            # If model is essential for all KPS ops, this is a FAILED_PRECONDITION.
            # If KPS could proceed for non-text data without model, then it's more nuanced.
            # Assuming for text, model is essential.
            if request.content_type.startswith("text/"):
                 context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)
                 return kps_service_pb2.ProcessRawDataResponse() # Unreachable
            else:
                 logger.warning(f"{msg} Proceeding without embeddings for non-text content.")


        # Idempotency Check
        if self.config.KPS_IDEMPOTENCY_CHECK_ENABLED and request.data_id:
            idempotency_key = self.config.KPS_IDEMPOTENCY_METADATA_KEY
            logger.info(f"KPS: Performing idempotency check for data_id '{request.data_id}' using metadata key '{idempotency_key}'.")
            query = glm_service_pb2.KEMQuery(
                metadata_filters={idempotency_key: request.data_id}
            )
            retrieve_request = glm_service_pb2.RetrieveKEMsRequest(query=query, page_size=1)

            try:
                timeout_check = getattr(self.config, "KPS_GLM_IDEMPOTENCY_CHECK_TIMEOUT_S", self.config.GLM_STORE_KEM_TIMEOUT_S)
                response = self._glm_retrieve_kems_with_retry(retrieve_request, timeout=timeout_check)
                if response and response.kems:
                    existing_kem_id = response.kems[0].id
                    logger.info(f"KPS: Idempotency check positive. Data_id '{request.data_id}' already processed as KEM ID '{existing_kem_id}'.")
                    return kps_service_pb2.ProcessRawDataResponse(
                        kem_id=existing_kem_id,
                        success=True, # Still success, but indicates pre-existence
                        status_message=f"Data already processed (KEM ID: {existing_kem_id})."
                    )
            except pybreaker.CircuitBreakerError as e_cb:
                 logger.warning(f"KPS: Idempotency check skipped due to GLM circuit breaker open: {e_cb}. Proceeding with processing (StoreKEM will likely also fail if CB remains open).")
                 # Fail-open for idempotency check if CB is open for GLM.
            except grpc.RpcError as e_rpc:
                # If GLM is UNAVAILABLE or DEADLINE_EXCEEDED during check, log and proceed (fail-open for check).
                # Other GLM errors during check might indicate issues but we still proceed.
                logger.warning(f"KPS: Idempotency check GLM query failed (Code: {e_rpc.code()}): {e_rpc.details()}. Proceeding with processing.", exc_info=False) # Log less verbosely
            except Exception as e_generic:
                logger.warning(f"KPS: Idempotency check failed with unexpected error: {e_generic}. Proceeding with processing.", exc_info=False)


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
                        # This is an internal KPS configuration or model setup error.
                        msg = (f"KPS Internal Error: Model '{self.config.EMBEDDING_MODEL_NAME}' output dimension {len(embeddings)} "
                               f"mismatches configured DEFAULT_VECTOR_SIZE {self.config.DEFAULT_VECTOR_SIZE}.")
                        logger.error(msg)
                        context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Embedding dimension configuration mismatch in KPS.")
                        return kps_service_pb2.ProcessRawDataResponse() # Unreachable
                except Exception as e_embed: # Catch specific model errors if possible
                    msg = f"Error during embedding generation by model: {e_embed}"
                    logger.error(msg, exc_info=True)
                    context.abort(grpc.StatusCode.INTERNAL, f"Embedding generation failed: {e_embed}")
                    return kps_service_pb2.ProcessRawDataResponse() # Unreachable
            elif is_text_content and not self.embedding_model: # Model loading failed earlier, and it's text
                # This case should have been caught by FAILED_PRECONDITION at the start of RPC if model is essential for text.
                # If we reach here, it implies model is optional or non-text.
                logger.warning("KPS: Embedding model not available; embeddings will not be generated for text content.")


            kem_to_store = kem_pb2.KEM(
                content_type=request.content_type,
                content=request.raw_content, # Storing raw bytes
                embeddings=embeddings,
                metadata=request.initial_metadata # Assuming initial_metadata is map<string,string>
            )
            # If data_id was provided in the request, add it to metadata for traceability
            if request.data_id:
                # Ensure the idempotency key is added to metadata for future checks
                kem_to_store.metadata[self.config.KPS_IDEMPOTENCY_METADATA_KEY] = request.data_id
            elif self.config.KPS_IDEMPOTENCY_CHECK_ENABLED and not request.data_id:
                logger.warning("KPS: Idempotency check enabled, but no data_id provided in request. Cannot perform check or guarantee idempotency via this mechanism.")


            logger.info("KPS: Calling GLM.StoreKEM (with retry logic)...")
            store_kem_request = glm_service_pb2.StoreKEMRequest(kem=kem_to_store)

            try:
                # Use configured timeout for this specific call
                store_kem_response = self._glm_store_kem_with_retry(
                    store_kem_request,
                    timeout=self.config.GLM_STORE_KEM_TIMEOUT_S
                )

                if store_kem_response and store_kem_response.kem and store_kem_response.kem.id:
                    kem_id_from_glm = store_kem_response.kem.id
                    logger.info(f"KPS: KEM successfully stored in GLM with ID: {kem_id_from_glm}")
                    return kps_service_pb2.ProcessRawDataResponse(
                        kem_id=kem_id_from_glm,
                        success=True,
                        status_message=f"KEM successfully processed and stored with ID: {kem_id_from_glm}"
                    )
                else:
                    # If _glm_store_kem_with_retry succeeded (no exception) but response is malformed.
                    msg = "KPS Internal Error: GLM.StoreKEM did not return an expected KEM object or KEM ID after a successful RPC."
                    logger.error(msg)
                    context.abort(grpc.StatusCode.INTERNAL, msg)
                    return kps_service_pb2.ProcessRawDataResponse() # Unreachable

            except pybreaker.CircuitBreakerError as e_cb_store:
                msg = f"KPS: GLM service unavailable (circuit breaker open) for StoreKEM. Data_id='{request.data_id}'"
                logger.error(f"{msg}: {e_cb_store}")
                context.abort(grpc.StatusCode.UNAVAILABLE, msg) # Abort KPS RPC
                return kps_service_pb2.ProcessRawDataResponse() # Unreachable
            except grpc.RpcError as e_glm_store:
                logger.error(f"KPS: gRPC error from GLM StoreKEM (Data_id='{request.data_id}', Code: {e_glm_store.code()}): {e_glm_store.details()}", exc_info=False)
                # Propagate specific error from GLM if possible, or map to a suitable KPS error.
                # Example: If GLM says INVALID_ARGUMENT because KPS formed a bad KEM, KPS might return INTERNAL.
                # If GLM says UNAVAILABLE, KPS returns UNAVAILABLE.
                mapped_code = e_glm_store.code()
                if mapped_code == grpc.StatusCode.INVALID_ARGUMENT: # If GLM says KEM is bad
                     mapped_code = grpc.StatusCode.INTERNAL # KPS should have formed it correctly
                context.abort(mapped_code, f"Failed to store KEM in GLM: {e_glm_store.details()}")
                return kps_service_pb2.ProcessRawDataResponse() # Unreachable
            except Exception as e_store_other:
                 logger.error(f"KPS: Unexpected error during GLM StoreKEM (Data_id='{request.data_id}'): {e_store_other}", exc_info=True)
                 context.abort(grpc.StatusCode.INTERNAL, f"Unexpected internal error storing KEM: {e_store_other}")
                 return kps_service_pb2.ProcessRawDataResponse() # Unreachable

        except UnicodeDecodeError as e_decode: # Already handled above, but as a safeguard if logic changes
            logger.error(f"KPS: Unicode decode error for data_id '{request.data_id}': {e_decode}", exc_info=True) # Should not be reached if handled earlier
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid UTF-8 content for data_id '{request.data_id}'.")
            return kps_service_pb2.ProcessRawDataResponse()
        except Exception as e_main_process:
            logger.error(f"KPS: Unexpected error in ProcessRawData for data_id '{request.data_id}': {e_main_process}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, "Internal KPS error during data processing.")
            return kps_service_pb2.ProcessRawDataResponse() # Unreachable

    def close_resources(self):
        """Closes gRPC channels and other resources held by the servicer instance."""
        if self.glm_channel:
            try:
                self.glm_channel.close()
                logger.info("KPS: Main GLM client channel closed.")
            except Exception as e:
                logger.error(f"KPS: Error closing main GLM client channel: {e}", exc_info=True)
        if self.glm_health_check_channel:
            try:
                self.glm_health_check_channel.close()
                logger.info("KPS: GLM health check client channel closed.")
            except Exception as e:
                logger.error(f"KPS: Error closing GLM health check client channel: {e}", exc_info=True)

    # __del__ is not a reliable way to close channels, use explicit close_resources
    # def __del__(self):
    #     self.close_resources()


def serve():
    logger.info(f"KPS Configuration: GLM Address={config.GLM_SERVICE_ADDRESS}, KPS Listen Address={config.GRPC_LISTEN_ADDRESS}, "
                f"Sentence Transformer Model={config.SENTENCE_TRANSFORMER_MODEL}, Default Vector Size={config.DEFAULT_VECTOR_SIZE}")

    servicer_instance = KnowledgeProcessorServiceImpl()

    if not servicer_instance.embedding_model:
        logger.warning("KPS server starting WITHOUT an embedding model. Text embeddings will not be generated.")
    if not servicer_instance.glm_stub:
        logger.warning("KPS server starting WITHOUT a GLM CLIENT. KEM persistence will be impossible.")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.GRPC_SERVER_MAX_WORKERS)) # Use configured max_workers
    kps_service_pb2_grpc.add_KnowledgeProcessorServiceServicer_to_server(
        servicer_instance, server
    )

    # Add Health Servicer
    from grpc_health.v1 import health, health_pb2, health_pb2_grpc # Ensure health_pb2 for enums
    # from grpc_health.v1.health_pb2 import HealthCheckRequest as HC_Request # Already imported above for class

    # Custom HealthServicer for KPS
    class KPSHealthServicer(health.HealthServicer):
        def __init__(self, kps_service_instance: KnowledgeProcessorServiceImpl, initial_status: health_pb2_types.HealthCheckResponse.ServingStatus = health_pb2_types.HealthCheckResponse.SERVING):
            super().__init__()
            self._kps_service = kps_service_instance
            self.set("", initial_status)
            logger.info(f"KPS Initial Health Status set to: {health_pb2_types.HealthCheckResponse.ServingStatus.Name(initial_status)}")

        def Check(self, request: health_pb2.HealthCheckRequest, context) -> health_pb2.HealthCheckResponse:
            current_status = self._kps_service.check_overall_health()
            self.set(request.service, current_status) # Update status for the requested service (or "" for overall)
            return super().Check(request, context)

    kps_health_servicer = KPSHealthServicer(servicer_instance, servicer_instance.check_overall_health())
    health_pb2_grpc.add_HealthServicer_to_server(kps_health_servicer, server)

    server.add_insecure_port(config.GRPC_LISTEN_ADDRESS)
    logger.info(f"Starting KPS (Knowledge Processor Service) on {config.GRPC_LISTEN_ADDRESS} with detailed health checks enabled...")
    server.start()
    logger.info(f"KPS started and listening for connections on {config.GRPC_LISTEN_ADDRESS}.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Stopping KPS server...")
    finally:
        if 'servicer_instance' in locals() and hasattr(servicer_instance, 'close_resources'):
            logger.info("KPS: Closing servicer resources...")
            servicer_instance.close_resources()

        logger.info(f"KPS: Stopping gRPC server with {config.GRPC_SERVER_SHUTDOWN_GRACE_S}s grace...")
        server.stop(config.GRPC_SERVER_SHUTDOWN_GRACE_S)
        logger.info("KPS server stopped.")

if __name__ == '__main__':
    serve()
