import logging
from typing import List, Optional, Any
from qdrant_client import AsyncQdrantClient, models # Changed to AsyncQdrantClient
from qdrant_client.http.models import PointStruct, ScoredPoint, Distance, VectorParams
import asyncio # For async operations

logger = logging.getLogger(__name__)

class StorageError(Exception):
    pass
class BackendUnavailableError(Exception):
    pass

class QdrantKemRepository:
    def __init__(self,
                 collection_name: str,
                 default_vector_size: int,
                 default_distance_metric: str = "Cosine",
                 qdrant_host: Optional[str] = None, # Added for client creation
                 qdrant_port: Optional[int] = None, # Added for client creation
                 qdrant_api_key: Optional[str] = None, # Added for client creation
                 qdrant_client_timeout_s: Optional[int] = 10, # Added
                 async_qdrant_client: Optional[AsyncQdrantClient] = None): # Allow passing existing client

        if async_qdrant_client:
            self.async_client: AsyncQdrantClient = async_qdrant_client
            logger.info("QdrantKemRepository initialized with provided AsyncQdrantClient.")
        elif qdrant_host and qdrant_port:
            self.async_client: AsyncQdrantClient = AsyncQdrantClient(
                host=qdrant_host,
                port=qdrant_port,
                api_key=qdrant_api_key,
                timeout=qdrant_client_timeout_s
            )
            logger.info(f"QdrantKemRepository created new AsyncQdrantClient for {qdrant_host}:{qdrant_port}.")
        else:
            raise ValueError("Either an async_qdrant_client instance or qdrant_host and qdrant_port must be provided.")

        self.collection_name = collection_name
        self.default_vector_size = default_vector_size

        if default_distance_metric.upper() == "COSINE":
            self.default_distance = Distance.COSINE
        elif default_distance_metric.upper() == "DOT":
            self.default_distance = Distance.DOT
        elif default_distance_metric.upper() == "EUCLID":
            self.default_distance = Distance.EUCLID
        else:
            logger.warning(f"Unsupported Qdrant distance metric '{default_distance_metric}'. Defaulting to COSINE.")
            self.default_distance = Distance.COSINE

        self._collection_checked = False


    async def ensure_collection(self):
        if self._collection_checked:
            return
        try:
            await self.async_client.get_collection(collection_name=self.collection_name)
            logger.info(f"Qdrant collection '{self.collection_name}' already exists.")
        except Exception as e:
            # A more specific exception type from qdrant_client for "not found" would be better
            # For now, assuming any exception here means we might need to create it.
            # A common pattern is that a 404 or similar would be raised by get_collection.
            logger.info(f"Qdrant collection '{self.collection_name}' not found or error accessing: {type(e).__name__} - {e}. Attempting to create.")
            try:
                await self.async_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(size=self.default_vector_size, distance=self.default_distance)
                )
                logger.info(f"Successfully created Qdrant collection '{self.collection_name}'.")
            except Exception as e_create:
                logger.error(f"Failed to create Qdrant collection '{self.collection_name}': {e_create}", exc_info=True)
                raise StorageError(f"Failed to create Qdrant collection: {e_create}") from e_create
        self._collection_checked = True


    async def upsert_point(self, point: PointStruct):
        await self.ensure_collection()
        try:
            await self.async_client.upsert(collection_name=self.collection_name, points=[point], wait=True)
            logger.debug(f"Upserted point ID '{point.id}' to Qdrant collection '{self.collection_name}'.")
        except Exception as e:
            logger.error(f"Error upserting point ID '{point.id}' to Qdrant: {e}", exc_info=True)
            raise StorageError(f"Qdrant upsert error: {e}") from e

    async def upsert_points_batch(self, points: List[PointStruct]):
        if not points:
            return
        await self.ensure_collection()
        try:
            await self.async_client.upsert(collection_name=self.collection_name, points=points, wait=True)
            logger.debug(f"Upserted batch of {len(points)} points to Qdrant collection '{self.collection_name}'.")
        except Exception as e:
            logger.error(f"Error upserting batch of points to Qdrant: {e}", exc_info=True)
            raise StorageError(f"Qdrant batch upsert error: {e}") from e

    async def delete_points_by_ids(self, ids: List[str]):
        if not ids:
            return
        await self.ensure_collection()
        try:
            await self.async_client.delete(
                collection_name=self.collection_name,
                points_selector=models.PointIdsList(points=ids),
                wait=True
            )
            logger.debug(f"Deleted points with IDs {ids} from Qdrant collection '{self.collection_name}'.")
        except Exception as e:
            logger.error(f"Error deleting points {ids} from Qdrant: {e}", exc_info=True)
            raise StorageError(f"Qdrant delete error: {e}") from e

    async def search_points(
        self,
        query_vector: List[float],
        query_filter: Optional[models.Filter] = None,
        limit: int = 10,
        offset: int = 0,
        with_payload: bool = True,
        with_vectors: bool = True # Changed default to True, often needed
    ) -> List[ScoredPoint]:
        await self.ensure_collection()
        try:
            search_result = await self.async_client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                query_filter=query_filter,
                limit=limit,
                offset=offset,
                with_payload=with_payload,
                with_vectors=with_vectors
            )
            logger.debug(f"Qdrant search returned {len(search_result)} points.")
            return search_result
        except Exception as e: # Consider more specific Qdrant exceptions if available
            logger.error(f"Error searching Qdrant: {e}", exc_info=True)
            # Example of checking for connection-related errors:
            # This is highly dependent on the specific exceptions qdrant-client might raise
            if "connect" in str(e).lower() or "unavailable" in str(e).lower() or "timeout" in str(e).lower():
                 raise BackendUnavailableError(f"Qdrant unavailable for search: {e}") from e
            raise StorageError(f"Qdrant search error: {e}") from e

    async def get_points_by_ids(self, ids: List[str], with_payload: bool = True, with_vectors: bool = False) -> List[models.Record]:
        if not ids:
            return []
        await self.ensure_collection()
        try:
            records = await self.async_client.get_points(
                collection_name=self.collection_name,
                ids=ids,
                with_payload=with_payload,
                with_vectors=with_vectors
            )
            return records
        except Exception as e:
            logger.error(f"Error retrieving points by IDs {ids} from Qdrant: {e}", exc_info=True)
            raise StorageError(f"Qdrant get_points error: {e}") from e

    async def get_collection_info(self) -> models.CollectionInfo:
        # ensure_collection is not strictly needed here if just getting info,
        # but good practice to call it if subsequent ops depend on collection existing.
        # For a health check type call, direct get_collection is fine.
        try:
            return await self.async_client.get_collection(collection_name=self.collection_name)
        except Exception as e:
            logger.error(f"Error getting Qdrant collection info for '{self.collection_name}': {e}", exc_info=True)
            raise BackendUnavailableError(f"Could not get Qdrant collection info: {e}") from e

    async def close(self):
        """Closes the AsyncQdrantClient."""
        if self.async_client:
            await self.async_client.close()
            logger.info("AsyncQdrantClient closed by QdrantKemRepository.")
            # Set to None to indicate it's closed, though client might handle multiple close calls.
            # self.async_client = None
        else:
            logger.info("QdrantKemRepository: No AsyncQdrantClient to close (was None).")

    # Async context manager support
    async def __aenter__(self):
        # The client is created in __init__. ensure_collection might be called here if desired on entry.
        # await self.ensure_collection() # Or call it on first data operation
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
