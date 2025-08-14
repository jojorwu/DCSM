from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Dict, Any

# Forward import for type hinting KEMQuery if not already globally available
# For now, assume kem_pb2 and glm_service_pb2 are accessible in the environment
# where concrete implementations will exist.
# A more robust way might be to define a non-proto KEMQuery-like dataclass here
# if we want to decouple this interface from specific proto versions.
# However, for now, using the proto types directly for clarity.
from dcs_memory.generated_grpc import kem_pb2, glm_service_pb2
from ..config import ExternalDataSourceConfig # Relative import for config within the same app


class BaseExternalRepository(ABC):
    """
    Abstract Base Class for a repository connecting to an external data source.
    Implementations of this class will provide GLM with the ability to retrieve
    data from various user-configured external databases or storage systems,
    mapping them to KEM-like structures.
    """

    def __init__(self, config: ExternalDataSourceConfig, glm_config: 'GLMConfig'):
        """
        Initializes the external repository.

        Args:
            config: The specific configuration for this external data source instance.
            glm_config: The global GLM configuration, which might contain useful defaults
                        (e.g. default page size, KEM field mappings if not overridden).
        """
        self.config = config
        self.glm_config = glm_config # Store global GLM config if needed by implementations
        super().__init__()

    @abstractmethod
    async def connect(self) -> None:
        """
        Establishes a connection to the external data source if necessary.
        Should be idempotent or handle multiple calls gracefully.
        Raises:
            BackendUnavailableError: If connection fails.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Closes the connection to the external data source if necessary.
        Should be idempotent or handle multiple calls gracefully.
        """
        pass

    @abstractmethod
    async def retrieve_mapped_kems(
        self,
        internal_query: glm_service_pb2.KEMQuery, # Full KEMQuery from the user request
        page_size: int,
        page_token: Optional[str]
    ) -> Tuple[List[kem_pb2.KEM], Optional[str]]:
        """
        Retrieves data from the external source based on the provided KEMQuery,
        maps it to KEM protobuf objects, and handles pagination.
        Connectors are responsible for translating relevant parts of internal_query
        into native queries for their external source.
        and handles pagination.

        Args:
            page_size: The maximum number of KEMs to return.
            page_token: An opaque token for pagination, specific to the external source's
                        pagination mechanism (e.g., offset, cursor, keyset values).

        Returns:
            A tuple containing:
                - A list of KEM protobuf objects.
                - An optional next page token string (empty or None if no more pages).

        Raises:
            StorageError: For general errors during retrieval or mapping.
            InvalidQueryError: If the query (or parts of it relevant to this source) is invalid.
            BackendUnavailableError: If the external source is not reachable.
        """
        pass

    @abstractmethod
    async def check_health(self) -> Tuple[bool, str]:
        """
        Checks the health of the connection to the external data source.

        Returns:
            A tuple (is_healthy: bool, message: str).
            `is_healthy` is True if the backend is operational, False otherwise.
            `message` provides details about the health status.
        """
        pass

# It might be useful to also import common exceptions from .base if they are reused.
# from .base import StorageError, KemNotFoundError, BackendUnavailableError, InvalidQueryError
# For now, assuming concrete implementations will raise appropriate exceptions.
# If GLMConfig is needed for type hint in __init__
from ..config import GLMConfig
