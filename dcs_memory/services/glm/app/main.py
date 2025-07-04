import grpc
from concurrent import futures
import time
import sys
import os
import uuid
import json
import sqlite3
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import PointStruct
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import ParseDict
from typing import List, Optional # Added for type hints
import logging

from .config import GLMConfig
from .repositories.base import StorageError, KemNotFoundError, BackendUnavailableError, InvalidQueryError # For specific exception handling
# from .repositories import SqliteKemRepository, QdrantKemRepository # Will be used by DefaultGLMRepository
from .repositories.base import BasePersistentStorageRepository #, StorageError, KemNotFoundError, BackendUnavailableError, InvalidQueryError
# Placeholder for the default implementation, will be created in a later step
# from .repositories.default_impl import DefaultGLMRepository

current_script_path = os.path.abspath(__file__)
app_dir = os.path.dirname(current_script_path)

config = GLMConfig()

from pythonjsonlogger import jsonlogger # Import for JSON logging

# Centralized logging setup
def setup_logging(log_config: GLMConfig):
    handlers_list = []

    # Determine formatter based on output mode
    if log_config.LOG_OUTPUT_MODE in ["json_stdout", "json_file"]:
        # For JSON, LOG_FORMAT can define the fields. A common practice:
        # Example: "(asctime) (levelname) (name) (module) (funcName) (lineno) (message)"
        # These will become keys in the JSON log.
        # If LOG_FORMAT is the default text one, JsonFormatter will still work but might produce less structured JSON.
        # It's better if LOG_FORMAT is tailored for JSON keys when using JsonFormatter.
        # For simplicity, we can use a default JSON format string or make LOG_FORMAT more flexible.
        # Let's assume LOG_FORMAT is suitable or use a generic set of fields for JSON.
        # The `python-json-logger` adds default fields like 'asctime', 'levelname', 'message'.
        # The format string for JsonFormatter specifies *additional* fields from the LogRecord.
        json_fmt_str = getattr(log_config, 'LOG_JSON_FORMAT', log_config.LOG_FORMAT) # Could add LOG_JSON_FORMAT to config
        formatter = jsonlogger.JsonFormatter(fmt=json_fmt_str, datefmt=log_config.LOG_DATE_FORMAT)
    else: # For "stdout", "file"
        formatter = logging.Formatter(fmt=log_config.LOG_FORMAT, datefmt=log_config.LOG_DATE_FORMAT)

    if log_config.LOG_OUTPUT_MODE in ["stdout", "json_stdout"]:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        handlers_list.append(stream_handler)

    if log_config.LOG_OUTPUT_MODE in ["file", "json_file"]:
        if log_config.LOG_FILE_PATH:
            # TODO: Ensure directory for LOG_FILE_PATH exists or handle creation/permissions.
            try:
                file_handler = logging.FileHandler(log_config.LOG_FILE_PATH)
                file_handler.setFormatter(formatter)
                handlers_list.append(file_handler)
            except Exception as e:
                # Fallback to stdout if file handler fails
                print(f"Error setting up file logger at {log_config.LOG_FILE_PATH}: {e}. Falling back to stdout.", file=sys.stderr)
                if not any(isinstance(h, logging.StreamHandler) for h in handlers_list): # Avoid duplicate stdout
                    stream_handler_fallback = logging.StreamHandler(sys.stdout)
                    stream_handler_fallback.setFormatter(formatter)
                    handlers_list.append(stream_handler_fallback)
        else:
            print(f"LOG_OUTPUT_MODE is '{log_config.LOG_OUTPUT_MODE}' but LOG_FILE_PATH is not set. Defaulting to stdout if no other handler.", file=sys.stderr)
            if not handlers_list: # If no handlers configured yet (e.g. only file mode was chosen but path was missing)
                stream_handler_default = logging.StreamHandler(sys.stdout)
                stream_handler_default.setFormatter(formatter)
                handlers_list.append(stream_handler_default)

    if not handlers_list: # Ultimate fallback if somehow no handlers were added
        print("Warning: No logging handlers configured. Defaulting to basic stdout.", file=sys.stderr)
        logging.basicConfig(level=log_config.get_log_level_int())
        return

    logging.basicConfig(
        level=log_config.get_log_level_int(),
        # format and datefmt in basicConfig are not used when handlers are specified,
        # as handlers have their own formatters.
        handlers=handlers_list,
        force=True # Override any existing basicConfig
    )
    # Ensure all loggers obtained via getLogger propagate to the handlers set by basicConfig.
    # The level set on basicConfig (root logger) will be the effective level unless child loggers override it.

setup_logging(config)
logger = logging.getLogger(__name__) # Get a logger specific to this module

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2, glm_service_pb2_grpc
from generated_grpc import kps_service_pb2, kps_service_pb2_grpc # For KPS Client
from google.protobuf import empty_pb2
from grpc_health.v1 import health_pb2 # For HealthCheckResponse.ServingStatus enum
import grpc # For KPS client channel

class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self):
        logger.info("Initializing GlobalLongTermMemoryServicerImpl...")
        self.config: GLMConfig = config
        self.kps_client_stub: Optional[kps_service_pb2_grpc.KnowledgeProcessingServiceStub] = None

        # Initialize the unified storage repository
        # The actual instantiation will depend on configuration (Step 5 of plan)
        # For now, this is a placeholder for where DefaultGLMRepository would be created.
        # from .repositories.default_impl import DefaultGLMRepository # Example import
        # self.storage_repository: BasePersistentStorageRepository = DefaultGLMRepository(config)
        # For this refactoring step, we'll assume it's instantiated and assigned.
        # This will be properly done in a later step.
        # To make the code runnable for now (though it won't fully work without DefaultGLMRepository):
        self.storage_repository: BasePersistentStorageRepository

        backend_type = self.config.GLM_STORAGE_BACKEND_TYPE
        logger.info(f"GLM: Configured storage backend type: {backend_type}")

        if backend_type == "default_sqlite_qdrant":
            try:
                from .repositories.default_impl import DefaultGLMRepository
                self.storage_repository = DefaultGLMRepository(self.config, app_dir)
                logger.info("DefaultGLMRepository (SQLite+Qdrant) initialized successfully.")
            except ImportError:
                logger.critical("DefaultGLMRepository not found. Ensure it's available for 'default_sqlite_qdrant' backend type.")
                raise RuntimeError("DefaultGLMRepository not found")
            except Exception as e_repo_init:
                logger.critical(f"CRITICAL ERROR initializing DefaultGLMRepository: {e_repo_init}", exc_info=True)
                raise BackendUnavailableError(f"Failed to initialize default_sqlite_qdrant backend: {e_repo_init}") from e_repo_init
        # Example for a future backend type:
        # elif backend_type == "custom_backend_xyz":
        #     from .repositories.custom_xyz import CustomXYZRepository # Assuming it exists
        #     self.storage_repository = CustomXYZRepository(self.config.GLM_STORAGE_BACKEND_CONFIG)
        #     logger.info("CustomXYZRepository initialized.")
        else:
            logger.critical(f"Unsupported GLM_STORAGE_BACKEND_TYPE: {backend_type}")
            raise ValueError(f"Unsupported GLM_STORAGE_BACKEND_TYPE: {backend_type}")

        logger.info("GLM servicer initialized with unified storage repository.")
        self._init_kps_client()


    def _init_kps_client(self):
        kps_host = self.config.KPS_SERVICE_HOST
        kps_port = self.config.KPS_SERVICE_PORT
        if kps_host and kps_port:
            kps_address = f"{kps_host}:{kps_port}"
            logger.info(f"Attempting to connect to KPS service at {kps_address}...")
            try:
                # TODO: Add TLS credentials for KPS client if KPS server is secured
                # For now, assuming insecure channel for PoC
                channel = grpc.insecure_channel(kps_address)

                # Quick connectivity test (optional, but good for early feedback)
                # grpc.channel_ready_future(channel).result(timeout=5) # Blocking, maybe not ideal in constructor

                self.kps_client_stub = kps_service_pb2_grpc.KnowledgeProcessingServiceStub(channel)
                logger.info(f"KPS client stub initialized for address {kps_address}.")
                # To verify connection, one might make a dummy call here or rely on first actual use.
                # For example, KPS might have a HealthCheck or a simple GetStatus RPC.
            except Exception as e:
                logger.error(f"Failed to initialize KPS client for address {kps_address}: {e}", exc_info=True)
                self.kps_client_stub = None # Ensure it's None if init fails
        else:
            logger.warning("KPS_SERVICE_HOST or KPS_SERVICE_PORT not configured. KPS client will not be available.")
            self.kps_client_stub = None

    # The _check_sqlite_health and _check_qdrant_health methods are no longer needed here,
    # as health checking will be delegated to self.storage_repository.check_health().

    def check_overall_health(self) -> health_pb2.HealthCheckResponse.ServingStatus:
        # This method is called by the custom HealthServicer.
        # It will now call the check_health() method of the storage_repository.
        # Note: The ABC's check_health returns Tuple[bool, str].
        # We'll need to adapt this or change the ABC if GLM's health check needs more detail
        # or if the health servicer expects a specific format.
        # For now, let's assume the boolean is the primary outcome.
        # This part will be refined when DefaultGLMRepository is implemented.
        try:
            # Placeholder: In a real implementation, self.storage_repository would be initialized.
            if not hasattr(self, 'storage_repository') or self.storage_repository is None:
                 logger.error("GLM Health Check: Storage repository not initialized.")
                 return health_pb2.HealthCheckResponse.NOT_SERVING

            is_healthy, message = asyncio.run(self.storage_repository.check_health()) # Assuming check_health can be run sync for now for simplicity of refactor
            if is_healthy:
                logger.info(f"GLM Health Check: Backend status: HEALTHY. Message: {message}")
                return health_pb2.HealthCheckResponse.SERVING
            else:
                logger.warning(f"GLM Health Check: Backend status: UNHEALTHY. Message: {message}")
                return health_pb2.HealthCheckResponse.NOT_SERVING
        except Exception as e_health:
            logger.error(f"GLM Health Check: Error during health check: {e_health}", exc_info=True)
            return health_pb2.HealthCheckResponse.NOT_SERVING


    def _kem_dict_to_proto(self, kem_data: dict) -> kem_pb2.KEM:
        kem_data_copy = kem_data.copy()
        if 'content' in kem_data_copy and isinstance(kem_data_copy['content'], str):
             kem_data_copy['content'] = kem_data_copy['content'].encode('utf-8')
        return ParseDict(kem_data_copy, kem_pb2.KEM(), ignore_unknown_fields=True)

    def _kem_from_db_dict(self, kem_db_dict: typing.Dict[str, typing.Any], embeddings_map: typing.Optional[typing.Dict[str, typing.List[float]]] = None) -> kem_pb2.KEM:
        if not kem_db_dict:
            logger.error("_kem_from_db_dict received empty or None kem_db_dict")
            return kem_pb2.KEM()

        kem_dict_for_proto = kem_db_dict.copy()
        metadata_str = kem_dict_for_proto.get('metadata', '{}')
        try:
            kem_dict_for_proto['metadata'] = json.loads(metadata_str)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse metadata JSON: {metadata_str} for KEM ID {kem_db_dict.get('id')}. Using empty metadata.", exc_info=True)
            kem_dict_for_proto['metadata'] = {}

        for ts_field_name in ['created_at', 'updated_at']:
            ts_str = kem_dict_for_proto.get(ts_field_name)
            if ts_str and isinstance(ts_str, str):
                ts_proto = Timestamp()
                if not ts_str.endswith("Z") and '+' not in ts_str and '-' not in ts_str[10:]:
                    ts_str_for_parse = ts_str + "Z"
                else:
                    ts_str_for_parse = ts_str
                try:
                    ts_proto.FromJsonString(ts_str_for_parse)
                    kem_dict_for_proto[ts_field_name] = ts_proto
                except Exception as e_ts_parse:
                     logger.warning(f"Failed to parse timestamp string '{ts_str}' (tried as '{ts_str_for_parse}') for field '{ts_field_name}' in KEM ID {kem_db_dict.get('id')}: {e_ts_parse}", exc_info=True)
                     if ts_field_name in kem_dict_for_proto: del kem_dict_for_proto[ts_field_name]
            elif isinstance(ts_str, Timestamp):
                 pass
            else:
                 if ts_field_name in kem_dict_for_proto: del kem_dict_for_proto[ts_field_name]

        if embeddings_map and kem_dict_for_proto.get('id') in embeddings_map:
            kem_dict_for_proto['embeddings'] = embeddings_map[kem_dict_for_proto['id']]
        else:
            kem_dict_for_proto.setdefault('embeddings', [])

        return self._kem_dict_to_proto(kem_dict_for_proto)

    def StoreKEM(self, request: glm_service_pb2.StoreKEMRequest, context) -> glm_service_pb2.StoreKEMResponse:
        start_time = time.monotonic()
        kem = request.kem
        logger.info(f"StoreKEM: Called with KEM ID '{kem.id if kem.id else '<new>'}'.")
        start_time = time.monotonic()

        # Input validation (example, more can be added)
        if kem.embeddings and len(kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
            msg = f"Embedding dimension ({len(kem.embeddings)}) does not match configured DEFAULT_VECTOR_SIZE ({self.config.DEFAULT_VECTOR_SIZE})."
            logger.warning(f"StoreKEM: {msg} KEM_ID='{kem.id}'")
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
            return glm_service_pb2.StoreKEMResponse()

        try:
            # The repository's store_kem method is now responsible for:
            # - Generating ID if kem.id is empty.
            # - Setting/preserving created_at and updated_at timestamps.
            # - Storing all parts of the KEM (metadata, content, embeddings) atomically or with compensation.
            stored_kem_proto = asyncio.run(self.storage_repository.store_kem(kem)) # Async method called synchronously for now

            duration = time.monotonic() - start_time
            logger.info(f"StoreKEM: Finished. KEM_ID='{stored_kem_proto.id}'. Duration: {duration:.4f}s.")
            return glm_service_pb2.StoreKEMResponse(kem=stored_kem_proto)

        except KemNotFoundError as e: # Should not happen for StoreKEM if it's an upsert/insert
            logger.error(f"StoreKEM: Unexpected KemNotFoundError for KEM ID '{kem.id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Unexpected error storing KEM: {e}")
        except BackendUnavailableError as e:
            logger.error(f"StoreKEM: Backend unavailable for KEM ID '{kem.id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except InvalidQueryError as e: # Should not happen for StoreKEM
            logger.error(f"StoreKEM: Unexpected InvalidQueryError for KEM ID '{kem.id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Invalid operation during KEM store: {e}")
        except StorageError as e:
            logger.error(f"StoreKEM: StorageError for KEM ID '{kem.id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Failed to store KEM due to storage error: {e}")
        except Exception as e:
            logger.error(f"StoreKEM: Unexpected error for KEM ID '{kem.id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")
        return glm_service_pb2.StoreKEMResponse() # Should be unreachable if aborts are called

    def RetrieveKEMs(self, request: glm_service_pb2.RetrieveKEMsRequest, context) -> glm_service_pb2.RetrieveKEMsResponse:
        start_time = time.monotonic()
        query = request.query
        page_size = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
        page_token = request.page_token # page_token is already Optional[str]

        logger.info(f"RetrieveKEMs: Called with query: {query}, page_size: {page_size}, page_token: '{page_token}'")
        start_time = time.monotonic()

        try:
            # Input validation (embedding dimension if vector query)
            if query.embedding_query and len(query.embedding_query) != self.config.DEFAULT_VECTOR_SIZE:
                msg = f"Invalid embedding dimension ({len(query.embedding_query)}) for vector search. Expected {self.config.DEFAULT_VECTOR_SIZE}."
                logger.warning(f"RetrieveKEMs: {msg}")
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
                return glm_service_pb2.RetrieveKEMsResponse()

            kems_protos, next_page_token_str = asyncio.run(
                self.storage_repository.retrieve_kems(query, page_size, page_token)
            ) # Async method called synchronously for now

            duration = time.monotonic() - start_time
            logger.info(f"RetrieveKEMs: Finished. Found {len(kems_protos)} KEMs. NextToken: '{next_page_token_str}'. Duration: {duration:.4f}s.")
            return glm_service_pb2.RetrieveKEMsResponse(kems=kems_protos, next_page_token=next_page_token_str or "")

        except KemNotFoundError as e: # Should be handled by repo returning empty list or specific error if query demands existence
            logger.warning(f"RetrieveKEMs: KemNotFoundError during query processing (might be expected for some queries): {e}", exc_info=True)
            context.abort(grpc.StatusCode.NOT_FOUND, str(e))
        except BackendUnavailableError as e:
            logger.error(f"RetrieveKEMs: Backend unavailable during query: {e}", exc_info=True)
            context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except InvalidQueryError as e:
            logger.warning(f"RetrieveKEMs: Invalid query: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid query for KEM retrieval: {e}")
        except StorageError as e:
            logger.error(f"RetrieveKEMs: StorageError during query: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Failed to retrieve KEMs due to storage error: {e}")
        except Exception as e:
            logger.error(f"RetrieveKEMs: Unexpected error: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred during KEM retrieval: {e}")
        return glm_service_pb2.RetrieveKEMsResponse() # Should be unreachable

    def UpdateKEM(self, request: glm_service_pb2.UpdateKEMRequest, context) -> kem_pb2.KEM:
        start_time = time.monotonic()
        kem_id = request.kem_id
        kem_data_update = request.kem_data_update # This is a KEM proto with fields to update

        if not kem_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID is required for update.")
            return kem_pb2.KEM()

        logger.info(f"UpdateKEM: Called for KEM_ID='{kem_id}'.")
        start_time = time.monotonic()

        # Input validation (example)
        if kem_data_update.embeddings and len(kem_data_update.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
            msg = f"Invalid embedding dimension ({len(kem_data_update.embeddings)}) for update. Expected {self.config.DEFAULT_VECTOR_SIZE}."
            logger.warning(f"UpdateKEM: {msg} KEM_ID='{kem_id}'")
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
            return kem_pb2.KEM()

        try:
            # The repository's update_kem method is responsible for:
            # - Fetching the existing KEM.
            # - Applying updates from kem_data_update.
            # - Updating timestamps.
            # - Persisting changes atomically or with compensation.
            updated_kem_proto = asyncio.run(
                self.storage_repository.update_kem(kem_id, kem_data_update)
            ) # Async method called synchronously for now

            duration = time.monotonic() - start_time
            logger.info(f"UpdateKEM: Finished. KEM_ID='{updated_kem_proto.id}'. Duration: {duration:.4f}s.")
            return updated_kem_proto

        except KemNotFoundError as e:
            logger.warning(f"UpdateKEM: KEM_ID '{kem_id}' not found: {e}", exc_info=False) # Log less verbosely for not found
            context.abort(grpc.StatusCode.NOT_FOUND, f"KEM ID '{kem_id}' not found for update.")
        except BackendUnavailableError as e:
            logger.error(f"UpdateKEM: Backend unavailable for KEM_ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except InvalidQueryError as e: # Or a more specific UpdateError if defined
            logger.error(f"UpdateKEM: Invalid operation or data for KEM_ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid update data or operation: {e}")
        except StorageError as e:
            logger.error(f"UpdateKEM: StorageError for KEM_ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Failed to update KEM due to storage error: {e}")
        except Exception as e:
            logger.error(f"UpdateKEM: Unexpected error for KEM_ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred during KEM update: {e}")
        return kem_pb2.KEM() # Should be unreachable

    def DeleteKEM(self, request: glm_service_pb2.DeleteKEMRequest, context) -> empty_pb2.Empty:
        kem_id = request.kem_id
        if not kem_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID is required for deletion.")
            return empty_pb2.Empty()

        logger.info(f"DeleteKEM: Called for KEM_ID='{kem_id}'.")
        start_time = time.monotonic()

        try:
            deleted = asyncio.run(self.storage_repository.delete_kem(kem_id)) # Async method called synchronously for now

            duration = time.monotonic() - start_time
            if deleted:
                logger.info(f"DeleteKEM: Finished. KEM_ID='{kem_id}' deleted. Duration: {duration:.4f}s.")
            else:
                # This case means the repository indicated the KEM was not found to be deleted.
                # For a DELETE operation, not finding the item is often treated as success (idempotency).
                # However, the ABC method returns bool, so we can distinguish.
                # The original code logged "not found" and returned success.
                # To maintain similar behavior, we don't abort here if `deleted` is False.
                # If the client needs to know if it was *actually* found and then deleted,
                # the API contract might need adjustment or the client can do a GET first.
                logger.info(f"DeleteKEM: Finished. KEM_ID='{kem_id}' was not found or already deleted. Duration: {duration:.4f}s.")
            return empty_pb2.Empty()

        except BackendUnavailableError as e:
            logger.error(f"DeleteKEM: Backend unavailable for KEM_ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except StorageError as e:
            logger.error(f"DeleteKEM: StorageError for KEM_ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Failed to delete KEM due to storage error: {e}")
        except Exception as e:
            logger.error(f"DeleteKEM: Unexpected error for KEM_ID '{kem_id}': {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred during KEM deletion: {e}")
        return empty_pb2.Empty() # Should be unreachable

    def BatchStoreKEMs(self, request: glm_service_pb2.BatchStoreKEMsRequest, context) -> glm_service_pb2.BatchStoreKEMsResponse:
        start_time = time.monotonic()
        num_req_kems = len(request.kems)
        logger.info(f"BatchStoreKEMs: Called with {num_req_kems} KEMs.")
        start_time = time.monotonic()

        if not request.kems:
            logger.info("BatchStoreKEMs: Received empty KEM list.")
            return glm_service_pb2.BatchStoreKEMsResponse(overall_error_message="Empty KEM list received.")

        # Basic validation (e.g., embedding dimension) can be done upfront
        # More complex validation or per-KEM processing is now delegated to the repository.
        validated_kems_for_repo: List[kem_pb2.KEM] = []
        initial_failed_refs: List[str] = [] # For KEMs failing pre-validation by servicer

        for idx, req_kem in enumerate(request.kems):
            # The repository will handle ID generation if not present, and timestamping.
            # Servicer can do upfront validation like embedding dimension.
            if req_kem.embeddings and len(req_kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                orig_ref = req_kem.id if req_kem.id else f"req_idx_{idx}"
                logger.error(f"BatchStoreKEMs: Invalid embedding dimension for KEM (ref: {orig_ref}). Skipping.")
                initial_failed_refs.append(orig_ref)
                continue
            validated_kems_for_repo.append(req_kem)

        if not validated_kems_for_repo and initial_failed_refs:
            # All KEMs failed pre-validation
            return glm_service_pb2.BatchStoreKEMsResponse(
                failed_kem_references=initial_failed_refs,
                overall_error_message="All KEMs failed pre-validation (e.g., embedding dimension)."
            )

        # If some passed pre-validation but others failed, initial_failed_refs will be part of final result.

        try:
            successful_kems_protos, repo_failed_refs = asyncio.run(
                self.storage_repository.batch_store_kems(validated_kems_for_repo)
            ) # Async method called synchronously for now

            all_failed_refs = list(set(initial_failed_refs + repo_failed_refs))

            resp = glm_service_pb2.BatchStoreKEMsResponse(
                successfully_stored_kems=successful_kems_protos,
                failed_kem_references=all_failed_refs
            )
            if all_failed_refs:
                resp.overall_error_message = f"Failed to store {len(all_failed_refs)} out of {num_req_kems} KEMs."

            duration = time.monotonic() - start_time
            logger.info(f"BatchStoreKEMs: Finished. Duration: {duration:.4f}s. Success: {len(successful_kems_protos)}, Failures: {len(all_failed_refs)} (Initial Pre-validation Fails: {len(initial_failed_refs)}).")
            return resp

        except BackendUnavailableError as e:
            logger.error(f"BatchStoreKEMs: Backend unavailable: {e}", exc_info=True)
            context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except StorageError as e:
            logger.error(f"BatchStoreKEMs: StorageError: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Failed to batch store KEMs due to storage error: {e}")
        except Exception as e:
            logger.error(f"BatchStoreKEMs: Unexpected error: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred during batch KEM store: {e}")

        # Fallback if abort wasn't called but an issue occurred (should be unreachable due to aborts)
        # Collect all original references/IDs as failed if we reach here.
        all_original_refs = [k.id if k.id else f"req_idx_{i}" for i, k in enumerate(request.kems)]
        return glm_service_pb2.BatchStoreKEMsResponse(
            failed_kem_references=list(set(initial_failed_refs + all_original_refs)),
            overall_error_message="Batch store failed due to an unexpected error after initial processing."
        )

    # --- Method for KPS Indexing of External Data Sources ---
    def IndexExternalDataSource(self, request: glm_service_pb2.IndexExternalDataSourceRequest, context) -> glm_service_pb2.IndexExternalDataSourceResponse:
        start_time = time.monotonic()
        data_source_name = request.data_source_name
        logger.info(f"IndexExternalDataSource: Called for data_source_name='{data_source_name}'.")

        items_processed_total = 0
        items_failed_total = 0
        page_size_for_fetch = self.config.DEFAULT_PAGE_SIZE # Or make this configurable for indexing

        if not data_source_name:
            msg = "data_source_name is required."
            logger.warning(f"IndexExternalDataSource: {msg}")
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
            return glm_service_pb2.IndexExternalDataSourceResponse()

        if not self.kps_client_stub:
            msg = "KPS client is not available in GLM. Cannot index external data."
            logger.error(f"IndexExternalDataSource: {msg} For source '{data_source_name}'.")
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)
            return glm_service_pb2.IndexExternalDataSourceResponse()

        external_repo = self.storage_repository.external_repos.get(data_source_name)
        if not external_repo:
            msg = f"External data source '{data_source_name}' not found or not configured."
            logger.warning(f"IndexExternalDataSource: {msg}")
            context.abort(grpc.StatusCode.NOT_FOUND, msg)
            return glm_service_pb2.IndexExternalDataSourceResponse()

        logger.info(f"IndexExternalDataSource: Starting data fetch and KPS indexing for source '{data_source_name}'. Page size: {page_size_for_fetch}")

        current_page_token: Optional[str] = None
        has_more_pages = True
        kps_batch_size = 50 # How many items to send to KPS in one BatchAddMemories call

        try:
            while has_more_pages:
                kems_from_external_source: List[kem_pb2.KEM] = []
                next_page_token_from_connector: Optional[str] = None

                # Construct a dummy KEMQuery for the external repo, as it might expect one.
                # The external repo for now mostly uses mapping_config for query details.
                # Filters in this dummy query won't be used by the basic connectors yet.
                dummy_query_for_connector = glm_service_pb2.KEMQuery()


                try:
                    # retrieve_mapped_kems is async, call it appropriately
                    kems_from_external_source, next_page_token_from_connector = asyncio.run(
                        external_repo.retrieve_mapped_kems(
                            internal_query=dummy_query_for_connector,
                            page_size=page_size_for_fetch,
                            page_token=current_page_token
                        )
                    )
                    logger.info(f"IndexExternalDataSource: Fetched {len(kems_from_external_source)} items from '{data_source_name}' (page_token: {current_page_token}). Next token: {next_page_token_from_connector}")

                except Exception as e_fetch:
                    logger.error(f"IndexExternalDataSource: Error fetching data from '{data_source_name}': {e_fetch}", exc_info=True)
                    items_failed_total += page_size_for_fetch # Approximate failure for the page
                    # Depending on error, might stop or try next page if pagination allows skipping
                    # For PoC, let's stop on fetch error for a page.
                    raise StorageError(f"Failed to fetch data from external source '{data_source_name}': {e_fetch}") from e_fetch

                if not kems_from_external_source:
                    logger.info(f"IndexExternalDataSource: No more items from '{data_source_name}'.")
                    has_more_pages = False
                else:
                    kps_payloads: List[kps_service_pb2.MemoryContent] = []
                    for kem_ext in kems_from_external_source:
                        # Construct unique KEM URI for KPS
                        # Format: kem:external:<data_source_name>:<original_kem_id>
                        kps_kem_uri = f"kem:external:{data_source_name}:{kem_ext.id}"

                        # Content for KPS is the KEM's content field (bytes, needs to be string for KPS AddMemory)
                        # Assuming KPS AddMemory expects string content.
                        try:
                            content_str = kem_ext.content.decode('utf-8')
                        except UnicodeDecodeError:
                            logger.warning(f"IndexExternalDataSource: Could not decode content for KEM ID '{kem_ext.id}' from source '{data_source_name}' as UTF-8. Skipping for KPS.")
                            items_failed_total +=1
                            continue

                        kps_payloads.append(kps_service_pb2.MemoryContent(kem_uri=kps_kem_uri, content=content_str))

                    if kps_payloads:
                        for i in range(0, len(kps_payloads), kps_batch_size):
                            batch_to_send = kps_payloads[i:i + kps_batch_size]
                            logger.debug(f"IndexExternalDataSource: Sending batch of {len(batch_to_send)} items to KPS for source '{data_source_name}'.")
                            try:
                                kps_request = kps_service_pb2.BatchAddMemoriesRequest(memories=batch_to_send)
                                kps_response: kps_service_pb2.BatchAddMemoriesResponse = self.kps_client_stub.BatchAddMemories(kps_request, timeout=30) # Add timeout

                                items_processed_this_batch = len(batch_to_send) - len(kps_response.failed_kem_references)
                                items_failed_this_batch = len(kps_response.failed_kem_references)
                                items_processed_total += items_processed_this_batch
                                items_failed_total += items_failed_this_batch

                                if kps_response.failed_kem_references:
                                    logger.warning(f"IndexExternalDataSource: KPS failed to process {items_failed_this_batch} items from batch for source '{data_source_name}'. References: {kps_response.failed_kem_references}")
                                if kps_response.overall_error_message:
                                     logger.warning(f"IndexExternalDataSource: KPS BatchAddMemories overall error: {kps_response.overall_error_message}")
                            except grpc.RpcError as e_kps:
                                logger.error(f"IndexExternalDataSource: gRPC error calling KPS BatchAddMemories for source '{data_source_name}': {e_kps}", exc_info=True)
                                items_failed_total += len(batch_to_send) # All in batch failed
                                # Depending on error, might stop or continue. For PoC, continue with next external page.
                            except Exception as e_kps_other:
                                logger.error(f"IndexExternalDataSource: Unexpected error calling KPS BatchAddMemories for source '{data_source_name}': {e_kps_other}", exc_info=True)
                                items_failed_total += len(batch_to_send)


                current_page_token = next_page_token_from_connector
                if not current_page_token:
                    has_more_pages = False

            # Loop finished
            duration = time.monotonic() - start_time
            status_msg = f"Completed indexing for '{data_source_name}'. Processed: {items_processed_total}, Failed: {items_failed_total}. Duration: {duration:.2f}s."
            logger.info(f"IndexExternalDataSource: {status_msg}")
            return glm_service_pb2.IndexExternalDataSourceResponse(
                status_message=status_msg,
                items_processed=items_processed_total,
                items_failed=items_failed_total
            )

        except StorageError as e_storage: # From external_repo.retrieve_mapped_kems
            logger.error(f"IndexExternalDataSource: StorageError during indexing of '{data_source_name}': {e_storage}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Storage error accessing external source '{data_source_name}': {e_storage}")
        except Exception as e_main:
            logger.error(f"IndexExternalDataSource: Unexpected error during indexing of '{data_source_name}': {e_main}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, f"Unexpected error indexing '{data_source_name}': {e_main}")

        # Should be unreachable if aborts are called
        return glm_service_pb2.IndexExternalDataSourceResponse(
            status_message=f"Failed indexing for '{data_source_name}' due to unexpected error after loop.",
            items_processed=items_processed_total,
            items_failed=items_failed_total # Or update based on where it failed
        )


def serve():
    logger.info(f"GLM Config: Qdrant={config.QDRANT_HOST}:{config.QDRANT_PORT} ('{config.QDRANT_COLLECTION}'), "
                f"SQLite='{os.path.join(app_dir, config.DB_FILENAME)}', gRPC={config.GRPC_LISTEN_ADDRESS}, LogLvl={config.LOG_LEVEL}")

    if config.QDRANT_HOST: # Pre-flight check only if Qdrant is configured
        try:
            client_test = QdrantClient(
                host=config.QDRANT_HOST,
                port=config.QDRANT_PORT,
                timeout=config.QDRANT_PREFLIGHT_CHECK_TIMEOUT_S # Use configured timeout
            )
            client_test.get_collections()
            logger.info(f"Qdrant pre-flight check OK: {config.QDRANT_HOST}:{config.QDRANT_PORT} (timeout: {config.QDRANT_PREFLIGHT_CHECK_TIMEOUT_S}s).")
        except Exception as e_preflight:
            logger.critical(f"CRITICAL: Qdrant unavailable at {config.QDRANT_HOST}:{config.QDRANT_PORT}. {e_preflight}. Server NOT STARTED if Qdrant is essential.")
            return # Exit if Qdrant is configured but unavailable

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.GRPC_SERVER_MAX_WORKERS)) # Use configured max_workers
    try:
        servicer_instance = GlobalLongTermMemoryServicerImpl()
    except Exception as e_servicer_init:
        logger.critical(f"CRITICAL: Error initializing ServicerImpl: {e_servicer_init}", exc_info=True)
        return

    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(servicer_instance, server)

    # Add Health Servicer
    from grpc_health.v1 import health, health_pb2, health_pb2_grpc # Ensure health_pb2 for enums

    # Custom HealthServicer for GLM
    class GLMHealthServicer(health.HealthServicer):
        def __init__(self, glm_service_instance: GlobalLongTermMemoryServicerImpl, initial_status: health_pb2.HealthCheckResponse.ServingStatus = health_pb2.HealthCheckResponse.SERVING):
            super().__init__()
            self._glm_service = glm_service_instance
            # Set initial overall status; can be updated by Check calls
            self.set("", initial_status)
            logger.info(f"GLM Initial Health Status set to: {health_pb2.HealthCheckResponse.ServingStatus.Name(initial_status)}")

        def Check(self, request: health_pb2.HealthCheckRequest, context) -> health_pb2.HealthCheckResponse:
            # request.service can be used to check specific sub-services if defined.
            # For GLM, we'll report overall health based on dependencies.
            current_status = self._glm_service.check_overall_health()

            # Update the status stored by the library HealthServicer.
            # This ensures that if a client uses the Watch API, it gets updates.
            # And also ensures that the Check response is based on the latest check.
            self.set(request.service, current_status)
            # For now, "" (overall) is what we update via check_overall_health.
            # If client sends empty string for service, it gets overall status.

            # The super().Check will use the status that was just set (or previously set for a specific service name)
            # logger.debug(f"GLM Health Check RPC for service '{request.service}': Status {health_pb2.HealthCheckResponse.ServingStatus.Name(current_status)}")
            return super().Check(request, context)

    glm_health_servicer = GLMHealthServicer(servicer_instance, servicer_instance.check_overall_health())
    health_pb2_grpc.add_HealthServicer_to_server(glm_health_servicer, server)

    if config.GRPC_SERVER_CERT_PATH and config.GRPC_SERVER_KEY_PATH:
        try:
            with open(config.GRPC_SERVER_KEY_PATH, 'rb') as f:
                server_key = f.read()
            with open(config.GRPC_SERVER_CERT_PATH, 'rb') as f:
                server_cert = f.read()

            # For now, server-side TLS only. mTLS would require root_certificates and require_client_auth=True.
            # root_ca_bundle = None
            # if config.GRPC_CLIENT_ROOT_CA_CERT_PATH: # This would be for mTLS client cert validation
            #     with open(config.GRPC_CLIENT_ROOT_CA_CERT_PATH, 'rb') as f:
            #         root_ca_bundle = f.read()

            server_credentials = grpc.ssl_server_credentials(
                private_key_certificate_chain_pairs=[(server_key, server_cert)]
                # root_certificates=root_ca_bundle, # Uncomment for mTLS
                # require_client_auth=True if root_ca_bundle else False # Uncomment for mTLS
            )
            server.add_secure_port(config.GRPC_LISTEN_ADDRESS, server_credentials)
            logger.info(f"Starting GLM server SECURELY on {config.GRPC_LISTEN_ADDRESS} with detailed health checks enabled.")
        except FileNotFoundError as e_certs:
            logger.critical(f"CRITICAL: TLS certificate/key file not found: {e_certs}. GLM server NOT STARTED securely. Check paths in config.")
            # Decide if to fallback to insecure or exit. For enabling TLS, exiting is safer if certs are specified but missing.
            return # Exit if certs are specified but not found
        except Exception as e_tls_setup:
            logger.critical(f"CRITICAL: Error setting up TLS for GLM server: {e_tls_setup}. GLM server NOT STARTED securely.", exc_info=True)
            return # Exit on other TLS setup errors
    else:
        server.add_insecure_port(config.GRPC_LISTEN_ADDRESS)
        logger.info(f"Starting GLM server INSECURELY on {config.GRPC_LISTEN_ADDRESS} with detailed health checks enabled (TLS cert/key not configured).")

    server.start()
    logger.info(f"GLM server started and listening on {config.GRPC_LISTEN_ADDRESS}.")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("GLM server stopping via KeyboardInterrupt...")
    finally:
        # No specific background tasks like periodic health checks were started in this revised sync model for GLM
        # servicer_instance.stop_background_tasks()
        server.stop(grace=config.GRPC_SERVER_SHUTDOWN_GRACE_S)
        logger.info("GLM server stopped.")

if __name__ == '__main__':
    serve()
