import pytest
import pytest_asyncio
import asyncio
import uuid
import logging
from dcs_memory.services.glm.app.repositories.default_impl import DefaultGLMRepository
from dcs_memory.services.glm.app.sqlite_repo import SqliteKemRepository
from dcs_memory.services.glm.app.config import GLMConfig
from dcs_memory.services.glm.generated_grpc import kem_pb2, glm_service_pb2

# Mark all tests in this file as async
pytestmark = pytest.mark.asyncio

@pytest.fixture
def event_loop():
    """Ensure each test runs in its own event loop."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture
async def in_memory_repo() -> DefaultGLMRepository:
    """
    Fixture to create a DefaultGLMRepository with a clean, isolated,
    in-memory SQLite DB for each test.
    """
    # Use a uniquely named in-memory database to ensure isolation between tests.
    db_name = f"file:{uuid.uuid4()}?mode=memory&cache=shared"
    config = GLMConfig(
        DB_FILENAME=db_name,
        QDRANT_HOST="",
        QDRANT_PORT=6333,
        QDRANT_COLLECTION="test",
        DEFAULT_VECTOR_SIZE=4,
        DEFAULT_PAGE_SIZE=10,
    )
    # The app_dir is not used when DB_FILENAME is an in-memory URI
    repo = DefaultGLMRepository(config=config, app_dir="/tmp")
    yield repo
    # The connection is held by the repo, and will be closed when the repo is garbage collected.
    # For in-memory DBs, this effectively drops the database.

async def test_fts_search_finds_kem_by_content(in_memory_repo: DefaultGLMRepository):
    """
    Tests if a KEM can be found via full-text search on its content.
    """
    repo = in_memory_repo
    kem_id = str(uuid.uuid4())
    search_term = "findme"
    kem_to_store = kem_pb2.KEM(
        id=kem_id,
        content=f"This is a test KEM with the magic word: {search_term}".encode('utf-8'),
        metadata={"source": "test"}
    )

    # Store the KEM
    await repo.store_kem(kem_to_store)

    # Perform FTS search
    query = glm_service_pb2.KEMQuery(text_query=search_term)
    results, _ = await repo.retrieve_kems(query=query, page_size=10, page_token=None)

    # Assertions
    assert len(results) == 1
    assert results[0].id == kem_id
    assert search_term in results[0].content.decode('utf-8')

async def test_fts_search_finds_kem_by_metadata(in_memory_repo: DefaultGLMRepository):
    """
    Tests if a KEM can be found via full-text search on its metadata.
    """
    repo = in_memory_repo
    kem_id = str(uuid.uuid4())
    search_term = "specialvalue"
    kem_to_store = kem_pb2.KEM(
        id=kem_id,
        content=b"Some random content.",
        metadata={"description": f"This item has a {search_term} in its metadata."}
    )

    # Store the KEM
    await repo.store_kem(kem_to_store)

    # Perform FTS search
    query = glm_service_pb2.KEMQuery(text_query=search_term)
    results, _ = await repo.retrieve_kems(query=query, page_size=10, page_token=None)

    # Assertions
    assert len(results) == 1
    assert results[0].id == kem_id
    assert results[0].metadata["description"] == f"This item has a {search_term} in its metadata."

async def test_fts_search_returns_empty_for_no_match(in_memory_repo: DefaultGLMRepository):
    """
    Tests if FTS returns an empty list when no KEMs match the search term.
    """
    repo = in_memory_repo
    kem_id = str(uuid.uuid4())
    kem_to_store = kem_pb2.KEM(
        id=kem_id,
        content=b"This is a sample KEM.",
        metadata={"source": "test"}
    )

    # Store the KEM
    await repo.store_kem(kem_to_store)

    # Perform FTS search with a term that doesn't exist
    query = glm_service_pb2.KEMQuery(text_query="nonexistentterm")
    results, _ = await repo.retrieve_kems(query=query, page_size=10, page_token=None)

    # Assertions
    assert len(results) == 0

async def test_fts_with_other_filters_intersection(in_memory_repo: DefaultGLMRepository):
    """
    Tests if FTS search results are correctly intersected with other filters (e.g., IDs).
    """
    repo = in_memory_repo
    search_term = "searchable"

    # KEM 1: Matches search term, will be included in ID filter
    kem1_id = str(uuid.uuid4())
    kem1 = kem_pb2.KEM(id=kem1_id, content=f"This is a {search_term} document.".encode('utf-8'))
    await repo.store_kem(kem1)

    # KEM 2: Matches search term, but will be excluded by ID filter
    kem2_id = str(uuid.uuid4())
    kem2 = kem_pb2.KEM(id=kem2_id, content=f"This is another {search_term} document.".encode('utf-8'))
    await repo.store_kem(kem2)

    # KEM 3: Does not match search term, but is included in ID filter
    kem3_id = str(uuid.uuid4())
    kem3 = kem_pb2.KEM(id=kem3_id, content=b"This document is irrelevant.")
    await repo.store_kem(kem3)

    # Perform search with FTS query and an ID filter that includes KEM1 and KEM3
    query = glm_service_pb2.KEMQuery(
        text_query=search_term,
        ids=[kem1_id, kem3_id]
    )
    results, _ = await repo.retrieve_kems(query=query, page_size=10, page_token=None)

    # Assertions: Only KEM 1 should be returned
    assert len(results) == 1
    assert results[0].id == kem1_id
