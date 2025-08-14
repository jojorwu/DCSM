from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Dict, Any
from dcs_memory.generated_grpc import kem_pb2, glm_service_pb2 # KEMQuery is in glm_service_pb2

class BasePersistentStorageRepository(ABC):
    """
    Abstract Base Class for a persistent storage repository for KEMs in GLM.
    This interface aims to abstract the underlying storage mechanism(s)
    (e.g., SQLite + Qdrant, a unified database, etc.) from the GLM servicer.
    """

    @abstractmethod
    async def store_kem(self, kem: kem_pb2.KEM) -> kem_pb2.KEM:
        """
        Stores or updates a single KEM.
        If kem.id is empty, the backend should generate one.
        The method should handle preserving created_at timestamps on updates.
        Returns the stored KEM, potentially with server-assigned ID and updated timestamps.
        Should raise an appropriate exception (e.g., StorageError, BackendUnavailableError) on failure.
        """
        pass

    @abstractmethod
    async def retrieve_kems(
        self,
        query: glm_service_pb2.KEMQuery,
        page_size: int,
        page_token: Optional[str]
    ) -> Tuple[List[kem_pb2.KEM], Optional[str]]:
        """
        Retrieves KEMs based on the provided KEMQuery, supporting pagination.
        The KEMQuery can include metadata filters, ID lookups, date range filters,
        and vector similarity search parameters.

        Args:
            query: The KEMQuery object defining search criteria.
            page_size: The maximum number of KEMs to return.
            page_token: An opaque token for pagination (typically an offset or cursor).

        Returns:
            A tuple containing:
                - A list of KEM protobuf objects matching the query.
                - An optional next page token string (empty or None if no more pages).
        Should raise an appropriate exception on failure.
        """
        pass

    @abstractmethod
    async def update_kem(
        self,
        kem_id: str,
        kem_update_data: kem_pb2.KEM,
        # Optional: field_mask: Optional[field_mask_pb2.FieldMask] = None
        # (Consider adding FieldMask later for more granular partial updates if needed)
    ) -> kem_pb2.KEM:
        """
        Partially updates an existing KEM identified by kem_id.
        Fields present in kem_update_data will be updated. Other fields remain unchanged.
        `updated_at` timestamp should be set by this method. `created_at` should be preserved.

        Args:
            kem_id: The ID of the KEM to update.
            kem_update_data: A KEM protobuf object containing the fields to update.
                             If a field is set in this object, it should be updated.
                             If embeddings are provided, they replace existing ones.
                             If metadata is provided, it replaces the entire metadata map.
                             (More granular metadata updates could be a future enhancement).

        Returns:
            The fully updated KEM protobuf object.
        Should raise NotFoundError if KEM ID does not exist, or other StorageError on failure.
        """
        pass

    @abstractmethod
    async def delete_kem(self, kem_id: str) -> bool:
        """
        Deletes a KEM by its ID.

        Args:
            kem_id: The ID of the KEM to delete.

        Returns:
            True if the KEM was found and deleted, False if not found.
        Should raise an appropriate exception on storage error during delete.
        """
        pass

    @abstractmethod
    async def batch_store_kems(
        self,
        kems: List[kem_pb2.KEM]
    ) -> Tuple[List[kem_pb2.KEM], List[str]]: # (successful_kems, failed_original_ids_or_references)
        """
        Stores or updates a batch of KEMs.
        For each KEM, if an ID is provided and exists, it's an update; otherwise, it's an insert.
        If no ID is provided, the backend should generate one.
        Handles preserving created_at timestamps on updates.

        Args:
            kems: A list of KEM protobuf objects to store/update.

        Returns:
            A tuple containing:
                - A list of KEM protobuf objects that were successfully stored/updated (with server-set IDs/timestamps).
                - A list of original KEM IDs (or other references if IDs were not initially provided) for KEMs that failed processing.
        Should raise an appropriate exception if the entire batch operation fails critically (e.g., backend unavailable).
        Individual KEM failures within the batch should be reported in the second element of the tuple.
        """
        pass

    @abstractmethod
    async def check_health(self) -> Tuple[bool, str]:
        """
        Checks the health of the underlying storage backend(s).

        Returns:
            A tuple (is_healthy: bool, message: str).
            `is_healthy` is True if the backend is operational, False otherwise.
            `message` provides details about the health status.
        """
        pass

# Define potential custom exceptions (optional, could also use generic ones)
class StorageError(Exception):
    """Base class for storage related errors."""
    pass

class BackendUnavailableError(StorageError):
    """Raised when a storage backend is unavailable."""
    pass

class KemNotFoundError(StorageError):
    """Raised when a KEM is not found for an operation that requires it to exist."""
    pass

class InvalidQueryError(StorageError):
    """Raised for invalid or unsupported queries."""
    pass
