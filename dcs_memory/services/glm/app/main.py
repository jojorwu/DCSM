import asyncio
import logging
import os
import sys
import grpc
import grpc.aio
from typing import Optional

from dcs_memory.services.glm.app.config import GLMConfig
from dcs_memory.services.glm.app.repositories.base import (
    BackendUnavailableError,
    InvalidQueryError,
    KemNotFoundError,
    StorageError,
)
from dcs_memory.generated_grpc import (
    glm_service_pb2,
    glm_service_pb2_grpc,
    kps_service_pb2,
    kps_service_pb2_grpc,
    kem_pb2,
)
from google.protobuf import empty_pb2
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from pythonjsonlogger import jsonlogger

config = GLMConfig()

def setup_logging(log_config: GLMConfig):
    logging.basicConfig(level=log_config.get_log_level_int(), format=log_config.LOG_FORMAT, datefmt=log_config.LOG_DATE_FORMAT)

setup_logging(config)
logger = logging.getLogger(__name__)

class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self, storage_repository):
        self.storage_repository = storage_repository
        self.config = config
        self.kps_client_stub: Optional[kps_service_pb2_grpc.KnowledgeProcessingServiceStub] = None

    async def _init_kps_client(self):
        kps_address = self.config.KPS_SERVICE_ADDRESS
        if kps_address:
            try:
                channel = grpc.aio.insecure_channel(kps_address)
                self.kps_client_stub = kps_service_pb2_grpc.KnowledgeProcessingServiceStub(channel)
                logger.info(f"KPS client stub initialized for address {kps_address}.")
            except Exception as e:
                logger.error(f"Failed to initialize KPS client for address {kps_address}: {e}", exc_info=True)
        else:
            logger.warning("KPS_SERVICE_ADDRESS not configured. KPS client will not be available.")

    async def StoreKEM(self, request: glm_service_pb2.StoreKEMRequest, context: grpc.aio.ServicerContext) -> glm_service_pb2.StoreKEMResponse:
        try:
            stored_kem = await self.storage_repository.store_kem(request.kem)
            return glm_service_pb2.StoreKEMResponse(kem=stored_kem)
        except (StorageError, BackendUnavailableError) as e:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except Exception as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")

    async def RetrieveKEMs(self, request: glm_service_pb2.RetrieveKEMsRequest, context: grpc.aio.ServicerContext) -> glm_service_pb2.RetrieveKEMsResponse:
        try:
            kems, next_page_token = await self.storage_repository.retrieve_kems(request.query, request.page_size, request.page_token)
            return glm_service_pb2.RetrieveKEMsResponse(kems=kems, next_page_token=next_page_token or "")
        except InvalidQueryError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except (StorageError, BackendUnavailableError) as e:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except Exception as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")

    async def UpdateKEM(self, request: glm_service_pb2.UpdateKEMRequest, context: grpc.aio.ServicerContext) -> kem_pb2.KEM:
        try:
            updated_kem = await self.storage_repository.update_kem(request.kem_id, request.kem_data_update)
            return updated_kem
        except KemNotFoundError as e:
            await context.abort(grpc.StatusCode.NOT_FOUND, str(e))
        except (StorageError, BackendUnavailableError) as e:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except Exception as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")

    async def DeleteKEM(self, request: glm_service_pb2.DeleteKEMRequest, context: grpc.ServicerContext) -> empty_pb2.Empty:
        try:
            await self.storage_repository.delete_kem(request.kem_id)
            return empty_pb2.Empty()
        except (StorageError, BackendUnavailableError) as e:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except Exception as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")

    async def BatchStoreKEMs(self, request: glm_service_pb2.BatchStoreKEMsRequest, context: grpc.ServicerContext) -> glm_service_pb2.BatchStoreKEMsResponse:
        try:
            successful, failed = await self.storage_repository.batch_store_kems(request.kems)
            return glm_service_pb2.BatchStoreKEMsResponse(successfully_stored_kems=successful, failed_kem_references=failed)
        except (StorageError, BackendUnavailableError) as e:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except Exception as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")

    async def IndexExternalDataSource(self, request: glm_service_pb2.IndexExternalDataSourceRequest, context: grpc.ServicerContext) -> glm_service_pb2.IndexExternalDataSourceResponse:
        data_source_name = request.data_source_name
        logger.info(f"IndexExternalDataSource called for: {data_source_name}")

        if not self.kps_client_stub:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "KPS client not available.")

        external_repo = self.storage_repository.external_repos.get(data_source_name)
        if not external_repo:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"External data source '{data_source_name}' not found.")

        items_processed_total = 0
        items_failed_total = 0
        page_token = None
        has_more = True
        kps_batch_size = 50

        while has_more:
            try:
                kems, next_page_token = await external_repo.retrieve_mapped_kems(
                    internal_query=glm_service_pb2.KEMQuery(),
                    page_size=kps_batch_size,
                    page_token=page_token
                )
                if not kems:
                    has_more = False
                    continue

                kps_payloads = []
                for kem in kems:
                    try:
                        content_str = kem.content.decode('utf-8')
                        kps_payloads.append(kps_service_pb2.MemoryContent(kem_uri=f"kem:external:{data_source_name}:{kem.id}", content=content_str))
                    except UnicodeDecodeError:
                        items_failed_total += 1

                if kps_payloads:
                    kps_request = kps_service_pb2.BatchAddMemoriesRequest(memories=kps_payloads)
                    kps_response = await self.kps_client_stub.BatchAddMemories(kps_request, timeout=30)

                    processed_count = len(kps_payloads) - len(kps_response.failed_kem_references)
                    items_processed_total += processed_count
                    items_failed_total += len(kps_response.failed_kem_references)

                page_token = next_page_token
                if not page_token:
                    has_more = False

            except Exception as e:
                logger.error(f"Error during external source indexing for '{data_source_name}': {e}", exc_info=True)
                await context.abort(grpc.StatusCode.INTERNAL, f"Error indexing source '{data_source_name}': {e}")

        status_msg = f"Completed indexing for '{data_source_name}'. Processed: {items_processed_total}, Failed: {items_failed_total}."
        logger.info(status_msg)
        return glm_service_pb2.IndexExternalDataSourceResponse(
            status_message=status_msg,
            items_processed=items_processed_total,
            items_failed=items_failed_total
        )

async def serve():
    server = grpc.aio.server()

    from dcs_memory.services.glm.app.repositories.default_impl import DefaultGLMRepository
    app_dir = os.path.dirname(os.path.abspath(__file__))
    storage_repo = DefaultGLMRepository(config, app_dir)
    await storage_repo.initialize()

    servicer = GlobalLongTermMemoryServicerImpl(storage_repo)
    await servicer._init_kps_client()

    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(servicer, server)
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    listen_addr = config.GRPC_LISTEN_ADDRESS
    server.add_insecure_port(listen_addr)
    logger.info(f"Starting server on {listen_addr}")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())
