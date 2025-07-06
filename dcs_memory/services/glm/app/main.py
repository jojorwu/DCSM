import grpc
import grpc.aio as grpc_aio
import asyncio
import time
import sys
import os
import uuid
import json
# import sqlite3 # No longer directly needed here, aiosqlite used in repo
from qdrant_client import QdrantClient
from typing import List, Optional
import logging

from .config import GLMConfig
from .repositories.base import StorageError, KemNotFoundError, BackendUnavailableError, InvalidQueryError
from .repositories.base import BasePersistentStorageRepository
from .repositories.default_impl import DefaultGLMRepository # Import DefaultGLMRepository directly

current_script_path = os.path.abspath(__file__)
app_dir = os.path.dirname(current_script_path)

config = GLMConfig()

from pythonjsonlogger import jsonlogger

# Centralized logging setup
def setup_logging(log_config: GLMConfig):
    handlers_list = []
    if log_config.LOG_OUTPUT_MODE in ["json_stdout", "json_file"]:
        json_fmt_str = getattr(log_config, 'LOG_JSON_FORMAT', log_config.LOG_FORMAT)
        formatter = jsonlogger.JsonFormatter(fmt=json_fmt_str, datefmt=log_config.LOG_DATE_FORMAT)
    else:
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
            print(f"LOG_OUTPUT_MODE is '{log_config.LOG_OUTPUT_MODE}' but LOG_FILE_PATH is not set. Defaulting to stdout if no other handler.", file=sys.stderr)
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
setup_logging(config)
logger = logging.getLogger(__name__)

from generated_grpc import kem_pb2
from generated_grpc import glm_service_pb2, glm_service_pb2_grpc
from generated_grpc import kps_service_pb2, kps_service_pb2_grpc
from google.protobuf import empty_pb2
from grpc_health.v1 import health_pb2, health_pb2_grpc, health_async


class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self):
        logger.info("Initializing GlobalLongTermMemoryServicerImpl...")
        self.config: GLMConfig = config
        self.kps_client_stub: Optional[kps_service_pb2_grpc.KnowledgeProcessingServiceStub] = None

        self.storage_repository: BasePersistentStorageRepository
        backend_type = self.config.GLM_STORAGE_BACKEND_TYPE
        logger.info(f"GLM: Configured storage backend type: {backend_type}")

        if backend_type == "default_sqlite_qdrant":
            try:
                # DefaultGLMRepository __init__ is sync. Async setup is in _ensure_repository_initialized
                self.storage_repository = DefaultGLMRepository(self.config, app_dir)
                logger.info("DefaultGLMRepository (SQLite+Qdrant) synchronously initialized.")
            except ImportError: # Should not happen if DefaultGLMRepository is in the same dir
                logger.critical("DefaultGLMRepository not found during dynamic import.")
                raise RuntimeError("DefaultGLMRepository not found")
            except Exception as e_repo_init:
                logger.critical(f"CRITICAL ERROR initializing DefaultGLMRepository: {e_repo_init}", exc_info=True)
                raise BackendUnavailableError(f"Failed to initialize default_sqlite_qdrant backend: {e_repo_init}") from e_repo_init
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
                channel = grpc.insecure_channel(kps_address)
                self.kps_client_stub = kps_service_pb2_grpc.KnowledgeProcessingServiceStub(channel)
                logger.info(f"KPS client stub initialized for address {kps_address}.")
            except Exception as e:
                logger.error(f"Failed to initialize KPS client for address {kps_address}: {e}", exc_info=True)
                self.kps_client_stub = None
        else:
            logger.warning("KPS_SERVICE_HOST or KPS_SERVICE_PORT not configured. KPS client will not be available.")
            self.kps_client_stub = None

    async def _ensure_repository_initialized(self):
        """Ensures any async initialization for repositories is done."""
        if hasattr(self.storage_repository, 'ensure_initialized') and \
           callable(self.storage_repository.ensure_initialized): # type: ignore
            try:
                await self.storage_repository.ensure_initialized() # type: ignore
                logger.info("GLM Storage Repository async initialization ensured.")
            except Exception as e_init_repo:
                logger.critical(f"CRITICAL: Failed to perform async initialization of storage repository: {e_init_repo}", exc_info=True)
                # This is a critical failure, the service should probably not start.
                # Depending on desired behavior, could raise an error here to stop `serve()`.
                # For now, logging critical and it might lead to NOT_SERVING in health checks.
                raise BackendUnavailableError(f"Async repository initialization failed: {e_init_repo}") from e_init_repo


    async def check_overall_health(self) -> health_pb2.HealthCheckResponse.ServingStatus:
        try:
            if not hasattr(self, 'storage_repository') or self.storage_repository is None:
                 logger.error("GLM Health Check: Storage repository not initialized.")
                 return health_pb2.HealthCheckResponse.NOT_SERVING

            # Ensure repo is initialized before checking health if it has such a method
            if hasattr(self.storage_repository, '_initialized') and not self.storage_repository._initialized: # type: ignore
                 logger.warning("GLM Health Check: Storage repository not yet async initialized. Reporting NOT_SERVING.")
                 return health_pb2.HealthCheckResponse.NOT_SERVING


            is_healthy, message = await self.storage_repository.check_health()
            if is_healthy:
                # logger.info(f"GLM Health Check: Backend status: HEALTHY. Message: {message}") # Too verbose for regular checks
                return health_pb2.HealthCheckResponse.SERVING
            else:
                logger.warning(f"GLM Health Check: Backend status: UNHEALTHY. Message: {message}")
                return health_pb2.HealthCheckResponse.NOT_SERVING
        except Exception as e_health:
            logger.error(f"GLM Health Check: Error during health check: {e_health}", exc_info=True)
            return health_pb2.HealthCheckResponse.NOT_SERVING

    async def StoreKEM(self, request: glm_service_pb2.StoreKEMRequest, context: grpc_aio.ServicerContext) -> glm_service_pb2.StoreKEMResponse:
        start_time = time.monotonic()
        kem = request.kem
        logger.info(f"StoreKEM: Called with KEM ID '{kem.id if kem.id else '<new>'}'.")

        if kem.embeddings and len(kem.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
            msg = f"Embedding dimension ({len(kem.embeddings)}) does not match configured DEFAULT_VECTOR_SIZE ({self.config.DEFAULT_VECTOR_SIZE})."
            logger.warning(f"StoreKEM: {msg} KEM_ID='{kem.id}'")
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
        try:
            stored_kem_proto = await self.storage_repository.store_kem(kem)
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
        return glm_service_pb2.StoreKEMResponse()


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
        try:
            kems_protos, next_page_token_str = await self.storage_repository.retrieve_kems(query, page_size, page_token)
            duration = time.monotonic() - start_time
            logger.info(f"RetrieveKEMs: Finished. Found {len(kems_protos)} KEMs. NextToken: '{next_page_token_str}'. Duration: {duration:.4f}s.")
            return glm_service_pb2.RetrieveKEMsResponse(kems=kems_protos, next_page_token=next_page_token_str or "")
        except KemNotFoundError as e:
            logger.warning(f"RetrieveKEMs: KemNotFoundError during query processing: {e}", exc_info=False)
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
        logger.info(f"UpdateKEM: Called for KEM_ID='{kem_id}'.")

        if kem_data_update.embeddings and len(kem_data_update.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
            msg = f"Invalid embedding dimension ({len(kem_data_update.embeddings)}) for update. Expected {self.config.DEFAULT_VECTOR_SIZE}."
            logger.warning(f"UpdateKEM: {msg} KEM_ID='{kem_id}'")
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)
        try:
            updated_kem_proto = await self.storage_repository.update_kem(kem_id, kem_data_update)
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
        logger.info(f"DeleteKEM: Called for KEM_ID='{kem_id}'.")
        start_time = time.monotonic()
        try:
            deleted = await self.storage_repository.delete_kem(kem_id)
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
            successful_kems_protos, repo_failed_refs = await self.storage_repository.batch_store_kems(validated_kems_for_repo)
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

        all_original_refs = [k_obj.id if k_obj.id else f"req_idx_{i}" for i, k_obj in enumerate(request.kems)]
        return glm_service_pb2.BatchStoreKEMsResponse(
            failed_kem_references=list(set(initial_failed_refs + all_original_refs)),
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
        if not self.kps_client_stub:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "KPS client not available.")

        external_repo = self.storage_repository.external_repos.get(data_source_name)
        if not external_repo:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"External data source '{data_source_name}' not configured.")

        logger.info(f"IndexExternalDataSource: Starting data fetch for source '{data_source_name}'.")
        current_page_token: Optional[str] = None; has_more_pages = True

        kps_process_timeout = getattr(self.config, "KPS_PROCESS_RAW_DATA_TIMEOUT_S", 30.0)

        try:
            while has_more_pages:
                kems_from_external_source: List[kem_pb2.KEM] = []
                next_page_token_from_connector: Optional[str] = None
                dummy_query_for_connector = glm_service_pb2.KEMQuery()
                try:
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

                for kem_ext in kems_from_external_source:
                    if not kem_ext.id:
                        logger.warning(f"IndexExternalDataSource: Skipping KEM from source '{data_source_name}' due to missing ID.")
                        items_failed_total += 1
                        continue

                    kps_data_id = kem_ext.id
                    kps_initial_metadata = dict(kem_ext.metadata) if kem_ext.metadata else {}

                    raw_content_bytes = kem_ext.content
                    if isinstance(raw_content_bytes, str):
                        logger.warning(f"IndexExternalDataSource: KEM content for '{kps_data_id}' was string, encoding to UTF-8 for KPS.")
                        raw_content_bytes = raw_content_bytes.encode('utf-8')

                    kps_request = kps_service_pb2.ProcessRawDataRequest(
                        data_id=kps_data_id,
                        content_type=kem_ext.content_type,
                        raw_content=raw_content_bytes,
                        initial_metadata=kps_initial_metadata
                    )

                    try:
                        logger.debug(f"IndexExternalDataSource: Calling KPS.ProcessRawData for data_id '{kps_data_id}' from source '{data_source_name}'.")
                        kps_response: kps_service_pb2.ProcessRawDataResponse = await asyncio.to_thread(
                            self.kps_client_stub.ProcessRawData, kps_request, timeout=kps_process_timeout
                        )
                        if kps_response.success:
                            items_processed_total += 1
                            logger.info(f"IndexExternalDataSource: Successfully processed data_id '{kps_data_id}' via KPS. KEM_ID: {kps_response.kem_id}")
                        else:
                            items_failed_total += 1
                            logger.warning(f"IndexExternalDataSource: KPS failed to process data_id '{kps_data_id}'. Status: {kps_response.status_message}")

                    except grpc.RpcError as e_kps_rpc:
                        items_failed_total += 1
                        logger.error(f"IndexExternalDataSource: gRPC error from KPS.ProcessRawData for data_id '{kps_data_id}': {e_kps_rpc.code()} - {e_kps_rpc.details()}", exc_info=True)
                        if e_kps_rpc.code() in [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.INTERNAL]:
                            logger.error(f"IndexExternalDataSource: Aborting for '{data_source_name}' due to critical KPS error for item '{kps_data_id}'.")
                            await context.abort(e_kps_rpc.code(), f"KPS error processing item '{kps_data_id}' from '{data_source_name}': {e_kps_rpc.code()}.")
                    except Exception as e_kps_call:
                        items_failed_total += 1
                        logger.error(f"IndexExternalDataSource: Unexpected error calling KPS.ProcessRawData for data_id '{kps_data_id}': {e_kps_call}", exc_info=True)
                        await context.abort(grpc.StatusCode.INTERNAL, f"Unexpected KPS call error for item '{kps_data_id}' from '{data_source_name}'.")

                current_page_token = next_page_token_from_connector
                if not current_page_token: has_more_pages = False

            duration = time.monotonic() - start_time
            status_msg = f"Completed indexing for '{data_source_name}'. Items submitted to KPS: {items_processed_total}, Items failed: {items_failed_total}. Duration: {duration:.2f}s."
            logger.info(f"IndexExternalDataSource: {status_msg}")
            return glm_service_pb2.IndexExternalDataSourceResponse(status_message=status_msg, items_processed=items_processed_total, items_failed=items_failed_total)

        except StorageError as e_storage_ext:
            logger.error(f"IndexExternalDataSource: StorageError fetching from '{data_source_name}': {e_storage_ext}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Storage error accessing external source '{data_source_name}': {e_storage_ext}")
        except grpc.RpcError:
            raise
        except Exception as e_main_idx:
            logger.error(f"IndexExternalDataSource: Unexpected error for '{data_source_name}': {e_main_idx}", exc_info=True)
            await context.abort(grpc.StatusCode.INTERNAL, f"Unexpected error indexing '{data_source_name}': {e_main_idx}")

        return glm_service_pb2.IndexExternalDataSourceResponse(
            status_message=f"Indexing for '{data_source_name}' concluded: Processed {items_processed_total}, Failed {items_failed_total}.",
            items_processed=items_processed_total, items_failed=items_failed_total
        )

async def serve():
    logger.info(f"GLM Config: Qdrant={config.QDRANT_HOST}:{config.QDRANT_PORT} ('{config.QDRANT_COLLECTION}'), "
                f"SQLite='{os.path.join(app_dir, config.DB_FILENAME)}', gRPC={config.GRPC_LISTEN_ADDRESS}, LogLvl={config.LOG_LEVEL}")

    if config.QDRANT_HOST:
        try:
            client_test = QdrantClient(host=config.QDRANT_HOST, port=config.QDRANT_PORT, timeout=config.QDRANT_PREFLIGHT_CHECK_TIMEOUT_S)
            client_test.get_collections()
            logger.info(f"Qdrant pre-flight check OK: {config.QDRANT_HOST}:{config.QDRANT_PORT}.")
        except Exception as e_preflight:
            logger.critical(f"CRITICAL: Qdrant unavailable at {config.QDRANT_HOST}:{config.QDRANT_PORT}. {e_preflight}. Server NOT STARTED.")
            return

    server = grpc_aio.server()
    servicer_instance = None # Define before try block
    try:
        servicer_instance = GlobalLongTermMemoryServicerImpl()
        await servicer_instance._ensure_repository_initialized() # Ensure async init for repo is done

    except Exception as e_servicer_init:
        logger.critical(f"CRITICAL: Error initializing ServicerImpl or its repository: {e_servicer_init}", exc_info=True)
        return

    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(servicer_instance, server)

    class GLMHealthServicer(health_async.HealthServicer):
        def __init__(self, glm_service_instance: GlobalLongTermMemoryServicerImpl):
            super().__init__()
            self._glm_service = glm_service_instance
            # asyncio.create_task(self._set_initial_status()) # Optional: set initial status
        # async def _set_initial_status(self):
        #    initial_status = await self._glm_service.check_overall_health()
        #    self.set("", initial_status) # Sets overall status

        async def Check(self, request: health_pb2.HealthCheckRequest, context: grpc_aio.ServicerContext) -> health_pb2.HealthCheckResponse:
            current_status = await self._glm_service.check_overall_health()
            self.set(request.service, current_status)
            return await super().Check(request, context)

    if servicer_instance: # Ensure servicer_instance was created
        glm_health_servicer = GLMHealthServicer(servicer_instance)
        health_pb2_grpc.add_HealthServicer_to_server(glm_health_servicer, server)
    else: # Should not happen if init error handling is correct (raises or returns)
        logger.critical("CRITICAL: Servicer instance not available for HealthServicer. Server NOT STARTED.")
        return


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

    await server.start()
    logger.info(f"GLM server (async) started and listening on {config.GRPC_LISTEN_ADDRESS}.")

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("GLM server stopping via KeyboardInterrupt...")
    except asyncio.CancelledError:
        logger.info("GLM server task cancelled.")
    finally:
        logger.info("GLM server stopping...")
        if servicer_instance and hasattr(servicer_instance.storage_repository, 'close_connections'):
            try:
                # If storage_repository has an async close method for its clients (e.g. Qdrant async client)
                await servicer_instance.storage_repository.close_connections() # type: ignore
                logger.info("GLM Storage Repository connections closed.")
            except Exception as e_close_repo:
                logger.error(f"Error closing storage repository connections: {e_close_repo}", exc_info=True)

        await server.stop(grace=config.GRPC_SERVER_SHUTDOWN_GRACE_S)
        logger.info("GLM server stopped.")

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("GLM main process interrupted by user.")
    except Exception as e_main_run:
        logger.critical(f"GLM main unhandled exception: {e_main_run}", exc_info=True)

[end of dcs_memory/services/glm/app/main.py]
