import grpc
import grpc.aio as grpc_aio # Import for async server
import asyncio # Import asyncio
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
from grpc_health.v1 import health_pb2, health_pb2_grpc, health_async # For HealthCheckResponse.ServingStatus enum and async HealthServicer
# import grpc # For KPS client channel - grpc.aio will be used for KPS client if it's async

class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self):
        logger.info("Initializing GlobalLongTermMemoryServicerImpl...")
        self.config: GLMConfig = config
        # KPS client initialization will need to be async if KPS is async, or run in executor
        # For now, assuming KPS client setup might remain synchronous if it's a simple client
        # or this service doesn't heavily depend on it at init time.
        # If KPS client needs to be async, its channel should be grpc_aio.Channel.
        self.kps_client_stub: Optional[kps_service_pb2_grpc.KnowledgeProcessingServiceStub] = None # Keep type for now

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
        else:
            logger.critical(f"Unsupported GLM_STORAGE_BACKEND_TYPE: {backend_type}")
            raise ValueError(f"Unsupported GLM_STORAGE_BACKEND_TYPE: {backend_type}")

        logger.info("GLM servicer initialized with unified storage repository.")
        self._init_kps_client() # KPS client init might need to be async if KPS is fully async

    # KPS client initialization might need to be async if KPS is an async service
    # For now, keeping it sync as it's a client, not a server part of GLM.
    def _init_kps_client(self):
        kps_host = self.config.KPS_SERVICE_HOST
        kps_port = self.config.KPS_SERVICE_PORT
        if kps_host and kps_port:
            kps_address = f"{kps_host}:{kps_port}"
            logger.info(f"Attempting to connect to KPS service at {kps_address}...")
            try:
                # If KPS is also async, this channel should be grpc_aio.insecure_channel or secure_channel
                # For now, assuming KPS client can be sync, or this part is simplified.
                # This might block if KPS is slow to respond during GLM init.
                channel = grpc.insecure_channel(kps_address) # Standard sync channel
                self.kps_client_stub = kps_service_pb2_grpc.KnowledgeProcessingServiceStub(channel)
                logger.info(f"KPS client stub initialized for address {kps_address}.")
            except Exception as e:
                logger.error(f"Failed to initialize KPS client for address {kps_address}: {e}", exc_info=True)
                self.kps_client_stub = None
        else:
            logger.warning("KPS_SERVICE_HOST or KPS_SERVICE_PORT not configured. KPS client will not be available.")
            self.kps_client_stub = None

    async def check_overall_health(self) -> health_pb2.HealthCheckResponse.ServingStatus:
        try:
            if not hasattr(self, 'storage_repository') or self.storage_repository is None:
                 logger.error("GLM Health Check: Storage repository not initialized.")
                 return health_pb2.HealthCheckResponse.NOT_SERVING

            is_healthy, message = await self.storage_repository.check_health() # Now awaited
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

    # This helper is mostly for internal use, not directly an RPC method.
    # If it were called from an async RPC method and did significant work, it might also need to be async.
    # For now, it's primarily data transformation.
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
                 pass # Already a Timestamp object
            else: # Not a string or Timestamp, remove if exists
                 if ts_field_name in kem_dict_for_proto: del kem_dict_for_proto[ts_field_name]


        if embeddings_map and kem_dict_for_proto.get('id') in embeddings_map:
            kem_dict_for_proto['embeddings'] = embeddings_map[kem_dict_for_proto['id']]
        else:
            # Ensure 'embeddings' field exists if it's expected by ParseDict for KEM
            # If KEM proto has 'repeated float embeddings', it defaults to empty list if not provided.
            kem_dict_for_proto.setdefault('embeddings', [])

        return self._kem_dict_to_proto(kem_dict_for_proto)

    async def StoreKEM(self, request: glm_service_pb2.StoreKEMRequest, context: grpc_aio.ServicerContext) -> glm_service_pb2.StoreKEMResponse:
        start_time = time.monotonic()
        kem = request.kem
        logger.info(f"StoreKEM: Called with KEM ID '{kem.id if kem.id else '<new>'}'.")

        if kem.embeddings and len(kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
            msg = f"Embedding dimension ({len(kem.embeddings)}) does not match configured DEFAULT_VECTOR_SIZE ({self.config.DEFAULT_VECTOR_SIZE})."
            logger.warning(f"StoreKEM: {msg} KEM_ID='{kem.id}'")
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
            return glm_service_pb2.StoreKEMResponse() # Should be unreachable due to abort

        try:
            stored_kem_proto = await self.storage_repository.store_kem(kem) # Now awaited
            duration = time.monotonic() - start_time
            logger.info(f"StoreKEM: Finished. KEM_ID='{stored_kem_proto.id}'. Duration: {duration:.4f}s.")
            return glm_service_pb2.StoreKEMResponse(kem=stored_kem_proto)
        except KemNotFoundError as e:
            logger.error(f"StoreKEM: Unexpected KemNotFoundError for KEM ID '{kem.id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Unexpected error storing KEM: {e}")
        except BackendUnavailableError as e:
            logger.error(f"StoreKEM: Backend unavailable for KEM ID '{kem.id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except InvalidQueryError as e:
            logger.error(f"StoreKEM: Unexpected InvalidQueryError for KEM ID '{kem.id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Invalid operation during KEM store: {e}")
        except StorageError as e:
            logger.error(f"StoreKEM: StorageError for KEM ID '{kem.id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to store KEM due to storage error: {e}")
        except Exception as e:
            logger.error(f"StoreKEM: Unexpected error for KEM ID '{kem.id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")
        return glm_service_pb2.StoreKEMResponse() # Should be unreachable

    async def RetrieveKEMs(self, request: glm_service_pb2.RetrieveKEMsRequest, context: grpc_aio.ServicerContext) -> glm_service_pb2.RetrieveKEMsResponse:
        start_time = time.monotonic()
        query = request.query
        page_size = request.page_size if request.page_size > 0 else self.config.DEFAULT_PAGE_SIZE
        page_token = request.page_token

        logger.info(f"RetrieveKEMs: Called with query: {query}, page_size: {page_size}, page_token: '{page_token}'")

        if query.embedding_query and len(query.embedding_query) != self.config.DEFAULT_VECTOR_SIZE:
            msg = f"Invalid embedding dimension ({len(query.embedding_query)}) for vector search. Expected {self.config.DEFAULT_VECTOR_SIZE}."
            logger.warning(f"RetrieveKEMs: {msg}")
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
            return glm_service_pb2.RetrieveKEMsResponse()

        try:
            kems_protos, next_page_token_str = await self.storage_repository.retrieve_kems(query, page_size, page_token) # Now awaited
            duration = time.monotonic() - start_time
            logger.info(f"RetrieveKEMs: Finished. Found {len(kems_protos)} KEMs. NextToken: '{next_page_token_str}'. Duration: {duration:.4f}s.")
            return glm_service_pb2.RetrieveKEMsResponse(kems=kems_protos, next_page_token=next_page_token_str or "")
        except KemNotFoundError as e:
            logger.warning(f"RetrieveKEMs: KemNotFoundError during query processing: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.NOT_FOUND, str(e))
        except BackendUnavailableError as e:
            logger.error(f"RetrieveKEMs: Backend unavailable: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except InvalidQueryError as e:
            logger.warning(f"RetrieveKEMs: Invalid query: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid query for KEM retrieval: {e}")
        except StorageError as e:
            logger.error(f"RetrieveKEMs: StorageError: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to retrieve KEMs due to storage error: {e}")
        except Exception as e:
            logger.error(f"RetrieveKEMs: Unexpected error: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")
        return glm_service_pb2.RetrieveKEMsResponse()

    async def UpdateKEM(self, request: glm_service_pb2.UpdateKEMRequest, context: grpc_aio.ServicerContext) -> kem_pb2.KEM:
        start_time = time.monotonic()
        kem_id = request.kem_id
        kem_data_update = request.kem_data_update

        if not kem_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID is required for update.")
            return kem_pb2.KEM()

        logger.info(f"UpdateKEM: Called for KEM_ID='{kem_id}'.")

        if kem_data_update.embeddings and len(kem_data_update.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
            msg = f"Invalid embedding dimension ({len(kem_data_update.embeddings)}) for update. Expected {self.config.DEFAULT_VECTOR_SIZE}."
            logger.warning(f"UpdateKEM: {msg} KEM_ID='{kem_id}'")
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
            return kem_pb2.KEM()

        try:
            updated_kem_proto = await self.storage_repository.update_kem(kem_id, kem_data_update) # Now awaited
            duration = time.monotonic() - start_time
            logger.info(f"UpdateKEM: Finished. KEM_ID='{updated_kem_proto.id}'. Duration: {duration:.4f}s.")
            return updated_kem_proto
        except KemNotFoundError as e:
            logger.warning(f"UpdateKEM: KEM_ID '{kem_id}' not found: {e}", exc_info=False)
            await context.abort(grpc.StatusCode.NOT_FOUND, f"KEM ID '{kem_id}' not found for update.")
        except BackendUnavailableError as e:
            logger.error(f"UpdateKEM: Backend unavailable for KEM_ID '{kem_id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except InvalidQueryError as e:
            logger.error(f"UpdateKEM: Invalid operation for KEM_ID '{kem_id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid update data or operation: {e}")
        except StorageError as e:
            logger.error(f"UpdateKEM: StorageError for KEM_ID '{kem_id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to update KEM due to storage error: {e}")
        except Exception as e:
            logger.error(f"UpdateKEM: Unexpected error for KEM_ID '{kem_id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")
        return kem_pb2.KEM()

    async def DeleteKEM(self, request: glm_service_pb2.DeleteKEMRequest, context: grpc_aio.ServicerContext) -> empty_pb2.Empty:
        kem_id = request.kem_id
        if not kem_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "KEM ID is required for deletion.")
            return empty_pb2.Empty()

        logger.info(f"DeleteKEM: Called for KEM_ID='{kem_id}'.")
        start_time = time.monotonic()

        try:
            deleted = await self.storage_repository.delete_kem(kem_id) # Now awaited
            duration = time.monotonic() - start_time
            if deleted:
                logger.info(f"DeleteKEM: Finished. KEM_ID='{kem_id}' deleted. Duration: {duration:.4f}s.")
            else:
                logger.info(f"DeleteKEM: Finished. KEM_ID='{kem_id}' was not found. Duration: {duration:.4f}s.")
            return empty_pb2.Empty()
        except BackendUnavailableError as e:
            logger.error(f"DeleteKEM: Backend unavailable for KEM_ID '{kem_id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except StorageError as e:
            logger.error(f"DeleteKEM: StorageError for KEM_ID '{kem_id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to delete KEM due to storage error: {e}")
        except Exception as e:
            logger.error(f"DeleteKEM: Unexpected error for KEM_ID '{kem_id}': {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")
        return empty_pb2.Empty()

    async def BatchStoreKEMs(self, request: glm_service_pb2.BatchStoreKEMsRequest, context: grpc_aio.ServicerContext) -> glm_service_pb2.BatchStoreKEMsResponse:
        start_time = time.monotonic()
        num_req_kems = len(request.kems)
        logger.info(f"BatchStoreKEMs: Called with {num_req_kems} KEMs.")

        if not request.kems:
            logger.info("BatchStoreKEMs: Received empty KEM list.")
            return glm_service_pb2.BatchStoreKEMsResponse(overall_error_message="Empty KEM list received.")

        validated_kems_for_repo: List[kem_pb2.KEM] = []
        initial_failed_refs: List[str] = []

        for idx, req_kem in enumerate(request.kems):
            if req_kem.embeddings and len(req_kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                orig_ref = req_kem.id if req_kem.id else f"req_idx_{idx}"
                logger.error(f"BatchStoreKEMs: Invalid embedding dimension for KEM (ref: {orig_ref}). Skipping.")
                initial_failed_refs.append(orig_ref)
                continue
            validated_kems_for_repo.append(req_kem)

        if not validated_kems_for_repo and initial_failed_refs:
            return glm_service_pb2.BatchStoreKEMsResponse(
                failed_kem_references=initial_failed_refs,
                overall_error_message="All KEMs failed pre-validation."
            )
        try:
            successful_kems_protos, repo_failed_refs = await self.storage_repository.batch_store_kems(validated_kems_for_repo) # Now awaited
            all_failed_refs = list(set(initial_failed_refs + repo_failed_refs))
            resp = glm_service_pb2.BatchStoreKEMsResponse(
                successfully_stored_kems=successful_kems_protos,
                failed_kem_references=all_failed_refs
            )
            if all_failed_refs:
                resp.overall_error_message = f"Failed to store {len(all_failed_refs)} out of {num_req_kems} KEMs."
            duration = time.monotonic() - start_time
            logger.info(f"BatchStoreKEMs: Finished. Duration: {duration:.4f}s. Success: {len(successful_kems_protos)}, Failures: {len(all_failed_refs)}.")
            return resp
        except BackendUnavailableError as e:
            logger.error(f"BatchStoreKEMs: Backend unavailable: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"Storage backend unavailable: {e}")
        except StorageError as e:
            logger.error(f"BatchStoreKEMs: StorageError: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Storage error during batch store: {e}")
        except Exception as e:
            logger.error(f"BatchStoreKEMs: Unexpected error: {e}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Unexpected error during batch store: {e}")

        all_original_refs = [k.id if k.id else f"req_idx_{i}" for i, k in enumerate(request.kems)]
        return glm_service_pb2.BatchStoreKEMsResponse(
            failed_kem_references=list(set(initial_failed_refs + all_original_refs)), # Ensure all are marked failed
            overall_error_message="Batch store failed due to an unexpected error after initial processing."
        )

    async def IndexExternalDataSource(self, request: glm_service_pb2.IndexExternalDataSourceRequest, context: grpc_aio.ServicerContext) -> glm_service_pb2.IndexExternalDataSourceResponse:
        start_time = time.monotonic()
        data_source_name = request.data_source_name
        logger.info(f"IndexExternalDataSource: Called for data_source_name='{data_source_name}'.")

        items_processed_total = 0; items_failed_total = 0
        page_size_for_fetch = self.config.DEFAULT_PAGE_SIZE

        if not data_source_name:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "data_source_name is required.")
            return glm_service_pb2.IndexExternalDataSourceResponse()

        if not self.kps_client_stub: # KPS client is sync for now
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "KPS client not available.")
            return glm_service_pb2.IndexExternalDataSourceResponse()

        external_repo = self.storage_repository.external_repos.get(data_source_name)
        if not external_repo:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"External data source '{data_source_name}' not configured.")
            return glm_service_pb2.IndexExternalDataSourceResponse()

        logger.info(f"IndexExternalDataSource: Starting data fetch for source '{data_source_name}'.")
        current_page_token: Optional[str] = None; has_more_pages = True
        kps_batch_size = 50

        try:
            while has_more_pages:
                kems_from_external_source: List[kem_pb2.KEM] = []
                next_page_token_from_connector: Optional[str] = None
                dummy_query_for_connector = glm_service_pb2.KEMQuery()
                try:
                    # Assuming external_repo.retrieve_mapped_kems is async
                    kems_from_external_source, next_page_token_from_connector = await external_repo.retrieve_mapped_kems(
                        internal_query=dummy_query_for_connector,
                        page_size=page_size_for_fetch,
                        page_token=current_page_token
                    )
                    logger.info(f"IndexExternalDataSource: Fetched {len(kems_from_external_source)} items from '{data_source_name}'.")
                except Exception as e_fetch:
                    logger.error(f"IndexExternalDataSource: Error fetching from '{data_source_name}': {e_fetch}", exc_info=True)
                    raise StorageError(f"Failed to fetch from external source '{data_source_name}': {e_fetch}") from e_fetch

                if not kems_from_external_source: has_more_pages = False; break

                kps_payloads: List[kps_service_pb2.MemoryContent] = []
                for kem_ext in kems_from_external_source:
                    kps_kem_uri = f"kem:external:{data_source_name}:{kem_ext.id}"
                    try: content_str = kem_ext.content.decode('utf-8')
                    except UnicodeDecodeError: logger.warning(f"UTF-8 decode error for KEM ID '{kem_ext.id}'. Skipping."); items_failed_total +=1; continue
                    kps_payloads.append(kps_service_pb2.MemoryContent(kem_uri=kps_kem_uri, content=content_str))

                if kps_payloads:
                    for i in range(0, len(kps_payloads), kps_batch_size):
                        batch_to_send = kps_payloads[i:i + kps_batch_size]
                        try:
                            # KPS client call is sync, run in executor for async context
                            kps_request = kps_service_pb2.BatchAddMemoriesRequest(memories=batch_to_send)
                            loop = asyncio.get_running_loop()
                            kps_response: kps_service_pb2.BatchAddMemoriesResponse = await loop.run_in_executor(
                                None, self.kps_client_stub.BatchAddMemories, kps_request, 30 # timeout
                            )
                            items_processed_total += len(batch_to_send) - len(kps_response.failed_kem_references)
                            items_failed_total += len(kps_response.failed_kem_references)
                            if kps_response.failed_kem_references: logger.warning(f"KPS failed items: {kps_response.failed_kem_references}")
                        except Exception as e_kps_batch:
                            logger.error(f"Error calling KPS BatchAddMemories: {e_kps_batch}", exc_info=True)
                            items_failed_total += len(batch_to_send)

                current_page_token = next_page_token_from_connector
                if not current_page_token: has_more_pages = False

            duration = time.monotonic() - start_time
            status_msg = f"Completed indexing for '{data_source_name}'. Processed: {items_processed_total}, Failed: {items_failed_total}. Duration: {duration:.2f}s."
            logger.info(f"IndexExternalDataSource: {status_msg}")
            return glm_service_pb2.IndexExternalDataSourceResponse(status_message=status_msg, items_processed=items_processed_total, items_failed=items_failed_total)

        except StorageError as e_storage_ext:
            await context.abort(grpc.StatusCode.INTERNAL, f"Storage error accessing external source: {e_storage_ext}")
        except Exception as e_main_idx:
            await context.abort(grpc.StatusCode.INTERNAL, f"Unexpected error indexing: {e_main_idx}")

        return glm_service_pb2.IndexExternalDataSourceResponse( # Should be unreachable
            status_message="Failed due to unexpected error after loop.",
            items_processed=items_processed_total, items_failed=items_failed_total
        )

async def serve(): # Changed to async def
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
            return

    server = grpc_aio.server() # Use async server
    try:
        servicer_instance = GlobalLongTermMemoryServicerImpl()
    except Exception as e_servicer_init:
        logger.critical(f"CRITICAL: Error initializing ServicerImpl: {e_servicer_init}", exc_info=True)
        return

    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(servicer_instance, server)

    # Use async HealthServicer
    class GLMHealthServicer(health_async.HealthServicer):
        def __init__(self, glm_service_instance: GlobalLongTermMemoryServicerImpl):
            super().__init__()
            self._glm_service = glm_service_instance
            # Schedule initial status check if needed, or let first Check call do it.
            # For simplicity, first Check call will set the status.
            # Or, can do: asyncio.create_task(self._set_initial_status())
            # async def _set_initial_status(self):
            #    initial_status = await self._glm_service.check_overall_health()
            #    self.set("", initial_status)
            #    logger.info(f"GLM Initial Health Status set to: {health_pb2.HealthCheckResponse.ServingStatus.Name(initial_status)}")


        async def Check(self, request: health_pb2.HealthCheckRequest, context: grpc_aio.ServicerContext) -> health_pb2.HealthCheckResponse:
            current_status = await self._glm_service.check_overall_health()
            self.set(request.service, current_status) # Update status for Watch
            # The super().Check will use the status that was just set.
            return await super().Check(request, context) # Await super call for async health servicer

    # Initial status can be set here or by the first Check call.
    # To set it eagerly:
    # initial_health_status = await servicer_instance.check_overall_health() # Requires serve() to be async to await this
    glm_health_servicer = GLMHealthServicer(servicer_instance)
    # logger.info(f"GLM Initial Health Status (before first check) set to: {health_pb2.HealthCheckResponse.ServingStatus.Name(initial_health_status)}")
    # glm_health_servicer.set("", initial_health_status) # Eagerly set initial status

    health_pb2_grpc.add_HealthServicer_to_server(glm_health_servicer, server)

    if config.GRPC_SERVER_CERT_PATH and config.GRPC_SERVER_KEY_PATH:
        try:
            with open(config.GRPC_SERVER_KEY_PATH, 'rb') as f: server_key = f.read()
            with open(config.GRPC_SERVER_CERT_PATH, 'rb') as f: server_cert = f.read()
            server_credentials = grpc.ssl_server_credentials([(server_key, server_cert)])
            server.add_secure_port(config.GRPC_LISTEN_ADDRESS, server_credentials)
            logger.info(f"Starting GLM server SECURELY (async) on {config.GRPC_LISTEN_ADDRESS}.")
        except FileNotFoundError as e_certs:
            logger.critical(f"CRITICAL: TLS certificate/key file not found: {e_certs}. GLM server NOT STARTED.")
            return
        except Exception as e_tls_setup:
            logger.critical(f"CRITICAL: Error setting up TLS for GLM server: {e_tls_setup}. GLM server NOT STARTED.", exc_info=True)
            return
    else:
        server.add_insecure_port(config.GRPC_LISTEN_ADDRESS)
        logger.info(f"Starting GLM server INSECURELY (async) on {config.GRPC_LISTEN_ADDRESS}.")

    await server.start() # await server start
    logger.info(f"GLM server (async) started and listening on {config.GRPC_LISTEN_ADDRESS}.")

    try:
        await server.wait_for_termination() # await termination
    except KeyboardInterrupt:
        logger.info("GLM server stopping via KeyboardInterrupt...")
    except asyncio.CancelledError: # Handle task cancellation
        logger.info("GLM server task cancelled.")
    finally:
        logger.info("GLM server stopping...")
        await server.stop(grace=config.GRPC_SERVER_SHUTDOWN_GRACE_S) # await stop
        logger.info("GLM server stopped.")

if __name__ == '__main__':
    try:
        asyncio.run(serve()) # Run the async serve function
    except KeyboardInterrupt:
        logger.info("GLM main process interrupted by user.")
    except Exception as e_main_run:
        logger.critical(f"GLM main unhandled exception: {e_main_run}", exc_info=True)
