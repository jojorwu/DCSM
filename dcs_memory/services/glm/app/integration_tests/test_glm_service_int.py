import pytest
import pytest_asyncio
import asyncio
import uuid
import grpc
import os
from typing import AsyncGenerator

from dcs_memory.services.glm.app.main import GlobalLongTermMemoryServicerImpl
from dcs_memory.services.glm.app.repositories.default_impl import DefaultGLMRepository
from dcs_memory.services.glm.app.config import GLMConfig
from dcs_memory.generated_grpc import glm_service_pb2, glm_service_pb2_grpc, kem_pb2

pytestmark = pytest.mark.asyncio

@pytest_asyncio.fixture(scope="function")
async def grpc_server():
    """Fixture to start and stop an in-process gRPC server."""
    server = grpc.aio.server()

    # Configure repository with an in-memory db for testing
    config = GLMConfig(
        DB_FILENAME=f"file:test_int_{uuid.uuid4()}?mode=memory&cache=shared",
        QDRANT_HOST="", # Disable Qdrant for these tests
        # Add other required GLMConfig fields with dummy values
        QDRANT_PORT=6333,
        QDRANT_COLLECTION="test",
        DEFAULT_VECTOR_SIZE=4,
        DEFAULT_PAGE_SIZE=10,
        KPS_SERVICE_ADDRESS="", # Disable KPS client
    )
    app_dir = os.path.dirname(os.path.abspath(__file__))
    storage_repo = DefaultGLMRepository(config, app_dir)
    await storage_repo.initialize()

    servicer = GlobalLongTermMemoryServicerImpl(storage_repo)
    glm_service_pb2_grpc.add_GlobalLongTermMemoryServicer_to_server(servicer, server)

    port = server.add_insecure_port('[::]:0')
    await server.start()
    yield f"localhost:{port}"
    await server.stop(0)
    await storage_repo.sqlite_repo.close()

@pytest_asyncio.fixture(scope="function")
async def grpc_client_stub(grpc_server: str) -> AsyncGenerator[glm_service_pb2_grpc.GlobalLongTermMemoryStub, None]:
    """Fixture to create a client stub connected to the test server."""
    async with grpc.aio.insecure_channel(grpc_server) as channel:
        yield glm_service_pb2_grpc.GlobalLongTermMemoryStub(channel)


async def test_store_and_retrieve_kem(grpc_client_stub):
    """Tests that a KEM can be stored and then retrieved."""
    kem_id = str(uuid.uuid4())
    content = b"This is the content of the KEM."
    metadata = {"source": "integration_test"}

    # Store KEM
    store_request = glm_service_pb2.StoreKEMRequest(
        kem=kem_pb2.KEM(id=kem_id, content=content, metadata=metadata)
    )
    store_response = await grpc_client_stub.StoreKEM(store_request)
    assert store_response.kem.id == kem_id

    # Retrieve KEM
    retrieve_request = glm_service_pb2.RetrieveKEMsRequest(
        query=glm_service_pb2.KEMQuery(ids=[kem_id])
    )
    retrieve_response = await grpc_client_stub.RetrieveKEMs(retrieve_request)

    assert len(retrieve_response.kems) == 1
    retrieved_kem = retrieve_response.kems[0]
    assert retrieved_kem.id == kem_id
    assert retrieved_kem.content == content
    assert retrieved_kem.metadata["source"] == "integration_test"

async def test_update_kem(grpc_client_stub):
    """Tests that a KEM can be updated."""
    kem_id = str(uuid.uuid4())
    store_request = glm_service_pb2.StoreKEMRequest(
        kem=kem_pb2.KEM(id=kem_id, content=b"original", metadata={"tag": "v1"})
    )
    await grpc_client_stub.StoreKEM(store_request)

    # Update KEM
    update_request = glm_service_pb2.UpdateKEMRequest(
        kem_id=kem_id,
        kem_data_update=kem_pb2.KEM(content=b"updated", metadata={"tag": "v2"})
    )
    updated_kem = await grpc_client_stub.UpdateKEM(update_request)

    assert updated_kem.content == b"updated"
    assert updated_kem.metadata["tag"] == "v2"

async def test_delete_kem(grpc_client_stub):
    """Tests that a KEM can be deleted."""
    kem_id = str(uuid.uuid4())
    store_request = glm_service_pb2.StoreKEMRequest(
        kem=kem_pb2.KEM(id=kem_id, content=b"to be deleted")
    )
    await grpc_client_stub.StoreKEM(store_request)

    # Delete KEM
    delete_request = glm_service_pb2.DeleteKEMRequest(kem_id=kem_id)
    await grpc_client_stub.DeleteKEM(delete_request)

    # Verify deletion
    retrieve_request = glm_service_pb2.RetrieveKEMsRequest(
        query=glm_service_pb2.KEMQuery(ids=[kem_id])
    )
    retrieve_response = await grpc_client_stub.RetrieveKEMs(retrieve_request)
    assert len(retrieve_response.kems) == 0

async def test_fts_search_via_rpc(grpc_client_stub):
    """Tests the FTS search functionality via the gRPC interface."""
    kem_id = str(uuid.uuid4())
    search_term = "searchableword"
    content = f"This document contains a very specific {search_term}.".encode('utf-8')

    store_request = glm_service_pb2.StoreKEMRequest(
        kem=kem_pb2.KEM(id=kem_id, content=content)
    )
    await grpc_client_stub.StoreKEM(store_request)

    # Retrieve KEM using FTS
    retrieve_request = glm_service_pb2.RetrieveKEMsRequest(
        query=glm_service_pb2.KEMQuery(text_query=search_term)
    )
    retrieve_response = await grpc_client_stub.RetrieveKEMs(retrieve_request)

    assert len(retrieve_response.kems) == 1
    assert retrieve_response.kems[0].id == kem_id
