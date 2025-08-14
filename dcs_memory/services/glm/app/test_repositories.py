import pytest
import pytest_asyncio
import asyncio
import uuid
import logging
from dcs_memory.services.glm.app.repositories.default_impl import DefaultGLMRepository, KemNotFoundError
from dcs_memory.services.glm.app.sqlite_repository import SqliteKemRepository
from dcs_memory.services.glm.app.config import GLMConfig
from dcs_memory.generated_grpc import kem_pb2, glm_service_pb2

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
    await repo.initialize()
    yield repo
    await repo.sqlite_repo.close()

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


async def test_update_kem_updates_fields_correctly(in_memory_repo: DefaultGLMRepository):
    """
    Tests that update_kem correctly updates specific fields of a KEM
    and changes the updated_at timestamp.
    """
    repo = in_memory_repo
    kem_id = str(uuid.uuid4())
    original_content = b"original content"
    original_metadata = {"tag": "original"}

    # 1. Store the initial KEM
    kem_to_store = kem_pb2.KEM(id=kem_id, content=original_content, metadata=original_metadata)
    stored_kem = await repo.store_kem(kem_to_store)
    original_created_at = stored_kem.created_at
    original_updated_at = stored_kem.updated_at

    # Ensure there's a small delay before updating
    await asyncio.sleep(0.01)

    # 2. Create an update request and call update_kem
    updated_content = b"updated content"
    update_request = kem_pb2.KEM(content=updated_content, metadata={"tag": "updated", "new_field": "yes"})
    updated_kem = await repo.update_kem(kem_id=kem_id, kem_update_data=update_request)

    # 3. Assertions
    assert updated_kem.id == kem_id
    assert updated_kem.content == updated_content
    assert updated_kem.metadata["tag"] == "updated"
    assert updated_kem.metadata["new_field"] == "yes"
    assert updated_kem.created_at == original_created_at  # created_at should NOT change
    # updated_at SHOULD change (be greater than original)
    updated_is_later = (updated_kem.updated_at.seconds > original_updated_at.seconds) or \
                       (updated_kem.updated_at.seconds == original_updated_at.seconds and
                        updated_kem.updated_at.nanos > original_updated_at.nanos)
    assert updated_is_later

async def test_delete_kem_removes_kem(in_memory_repo: DefaultGLMRepository):
    """
    Tests that delete_kem successfully removes a KEM and is idempotent.
    """
    repo = in_memory_repo
    kem_id = str(uuid.uuid4())
    kem_to_store = kem_pb2.KEM(id=kem_id, content=b"content to be deleted")
    await repo.store_kem(kem_to_store)

    # 1. Delete the KEM and assert it was successful
    delete_result_1 = await repo.delete_kem(kem_id)
    assert delete_result_1 is True

    # 2. Verify the KEM is gone
    with pytest.raises(KemNotFoundError):
        await repo.update_kem(kem_id, kem_pb2.KEM()) # update_kem raises KemNotFoundError

    # 3. Delete again and assert it returns False (idempotency)
    delete_result_2 = await repo.delete_kem(kem_id)
    assert delete_result_2 is False

async def test_batch_store_kems_stores_multiple_kems(in_memory_repo: DefaultGLMRepository):
    """
    Tests that batch_store_kems can store multiple KEMs successfully.
    """
    repo = in_memory_repo
    kem1 = kem_pb2.KEM(id=str(uuid.uuid4()), content=b"batch kem 1")
    kem2 = kem_pb2.KEM(id=str(uuid.uuid4()), content=b"batch kem 2")
    kem3 = kem_pb2.KEM(id=str(uuid.uuid4()), content=b"batch kem 3")
    kems_to_store = [kem1, kem2, kem3]

    # Call batch_store_kems
    successful_kems, failed_refs = await repo.batch_store_kems(kems_to_store)

    # Assertions
    assert len(successful_kems) == 3
    assert not failed_refs
    assert {k.id for k in successful_kems} == {k.id for k in kems_to_store}

    # Verify one of the KEMs was actually stored
    retrieved_kem_dict = await repo.sqlite_repo.get_full_kem_by_id(kem2.id)
    assert retrieved_kem_dict is not None
    retrieved_kem = repo._kem_from_db_dict_to_proto(dict(retrieved_kem_dict))
    assert retrieved_kem.content == kem2.content

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
