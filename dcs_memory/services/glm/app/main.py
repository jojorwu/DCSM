import asyncio
import logging
import os
import sys
import grpc.aio as grpc

from dcs_memory.services.glm.app.config import GLMConfig
from dcs_memory.services.glm.app.repositories.base import (
    BackendUnavailableError,
    InvalidQueryError,
    KemNotFoundError,
    StorageError,
)
from dcs_memory.services.glm.generated_grpc import (
    glm_service_pb2,
    glm_service_pb2_grpc,
)
from google.protobuf import empty_pb2
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from pythonjsonlogger import jsonlogger

config = GLMConfig()

def setup_logging(log_config: GLMConfig):
    # ... (setup_logging implementation remains the same)
    pass # Placeholder for brevity

setup_logging(config)
logger = logging.getLogger(__name__)

class GlobalLongTermMemoryServicerImpl(glm_service_pb2_grpc.GlobalLongTermMemoryServicer):
    def __init__(self, storage_repository):
        self.storage_repository = storage_repository
        self.config = config

    async def StoreKEM(self, request: glm_service_pb2.StoreKEMRequest, context: grpc.ServicerContext) -> glm_service_pb2.StoreKEMResponse:
        try:
            stored_kem = await self.storage_repository.store_kem(request.kem)
            return glm_service_pb2.StoreKEMResponse(kem=stored_kem)
        except (StorageError, BackendUnavailableError) as e:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except Exception as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")

    async def RetrieveKEMs(self, request: glm_service_pb2.RetrieveKEMsRequest, context: grpc.ServicerContext) -> glm_service_pb2.RetrieveKEMsResponse:
        try:
            kems, next_page_token = await self.storage_repository.retrieve_kems(
                request.query, request.page_size, request.page_token
            )
            return glm_service_pb2.RetrieveKEMsResponse(kems=kems, next_page_token=next_page_token or "")
        except InvalidQueryError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except (StorageError, BackendUnavailableError) as e:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        except Exception as e:
            await context.abort(grpc.StatusCode.INTERNAL, f"An unexpected error occurred: {e}")

    async def UpdateKEM(self, request: glm_service_pb2.UpdateKEMRequest, context: grpc.ServicerContext) -> kem_pb2.KEM:
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

async def serve():
    server = grpc.server()

    # Initialize repository
    from dcs_memory.services.glm.app.repositories.default_impl import DefaultGLMRepository
    app_dir = os.path.dirname(os.path.abspath(__file__))
    storage_repo = DefaultGLMRepository(config, app_dir)
    await storage_repo.initialize()

    # Add servicers
    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(
        GlobalLongTermMemoryServicerImpl(storage_repo), server
    )
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    listen_addr = config.GRPC_LISTEN_ADDRESS
    server.add_insecure_port(listen_addr)
    logger.info(f"Starting server on {listen_addr}")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())
