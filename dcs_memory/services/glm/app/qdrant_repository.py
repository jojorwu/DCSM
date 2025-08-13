import logging
from typing import List, Optional
from qdrant_client import models, AsyncQdrantClient
from qdrant_client.http.models import PointStruct

logger = logging.getLogger(__name__)

class QdrantKemRepository:
    def __init__(self, qdrant_client: AsyncQdrantClient, collection_name: str, default_vector_size: int, default_distance_metric: str):
        self.client = qdrant_client
        self.collection_name = collection_name
        self.default_vector_size = default_vector_size
        self.default_distance_metric_str = default_distance_metric.upper()

    def _get_qdrant_distance_metric(self) -> models.Distance:
        if self.default_distance_metric_str == "COSINE":
            return models.Distance.COSINE
        elif self.default_distance_metric_str == "DOT":
            return models.Distance.DOT
        elif self.default_distance_metric_str == "EUCLID":
            return models.Distance.EUCLID
        else:
            logger.warning(f"Unsupported Qdrant distance metric '{self.default_distance_metric_str}'. Defaulting to COSINE.")
            return models.Distance.COSINE

    async def ensure_collection(self):
        logger.info(f"Repository: Ensuring Qdrant collection '{self.collection_name}' exists.")
        try:
            await self.client.get_collection(collection_name=self.collection_name)
        except Exception as e:
            is_not_found_error = ("not found" in str(e).lower() or "404" in str(e).lower() or (hasattr(e, 'status_code') and e.status_code == 404))
            if is_not_found_error:
                distance_metric = self._get_qdrant_distance_metric()
                await self.client.recreate_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(size=self.default_vector_size, distance=distance_metric)
                )
            else:
                logger.error(f"Repository: Error checking/creating Qdrant collection '{self.collection_name}': {e}", exc_info=True)
                raise

    async def upsert_point(self, point: models.PointStruct):
        await self.client.upsert(collection_name=self.collection_name, points=[point], wait=True)

    async def upsert_points_batch(self, points: List[models.PointStruct]):
        if not points:
            return
        await self.client.upsert(collection_name=self.collection_name, points=points, wait=True)

    async def delete_points_by_ids(self, kem_ids: List[str]):
        if not kem_ids:
            return
        await self.client.delete(
            collection_name=self.collection_name,
            points_selector=models.PointIdsList(points=kem_ids),
        )

    async def search_points(self, query_vector: List[float], query_filter: Optional[models.Filter], limit: int, offset: int = 0, with_vectors: bool = False) -> List[models.ScoredPoint]:
        return await self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=limit,
            offset=offset,
            with_payload=True,
            with_vectors=with_vectors
        )

    async def retrieve_points_by_ids(self, kem_ids: List[str], with_vectors: bool = False) -> List[models.PointStruct]:
        if not kem_ids:
            return []
        return await self.client.retrieve(
            collection_name=self.collection_name,
            ids=kem_ids,
            with_payload=True,
            with_vectors=with_vectors
        )
