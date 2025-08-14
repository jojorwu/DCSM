import asyncio
import uuid
import logging
import time # For timestamps
from typing import List, Tuple, Optional, Dict, Any, Set
import copy # For deep copying KEMs to simulate storage

from dcs_memory.generated_grpc import kem_pb2, glm_service_pb2
from google.protobuf.timestamp_pb2 import Timestamp

from .base import BasePersistentStorageRepository, StorageError, KemNotFoundError, InvalidQueryError, BackendUnavailableError
from ..config import GLMConfig # For accessing potential in-memory specific configs

logger = logging.getLogger(__name__)

class InMemoryRepository(BasePersistentStorageRepository):
    """
    An in-memory implementation of BasePersistentStorageRepository for testing and development.
    KEMs are stored in a Python dictionary. Not persistent across restarts.
    Vector search capabilities are minimal (e.g., not implemented or basic filtering).
    """

    def __init__(self, config: GLMConfig, specific_backend_config: Optional[Dict[str, Any]] = None):
        self.config = config # GLMConfig for things like DEFAULT_VECTOR_SIZE
        self.backend_config = specific_backend_config or {}

        self.kems: Dict[str, kem_pb2.KEM] = {}
        self._lock = asyncio.Lock() # For thread-safe operations on self.kems

        self.max_kems = self.backend_config.get("max_kems", 10000) # Example specific config
        logger.info(f"InMemoryRepository initialized. Max KEMs (conceptual): {self.max_kems}")

    async def store_kem(self, kem: kem_pb2.KEM) -> kem_pb2.KEM:
        async with self._lock:
            kem_to_store = kem_pb2.KEM()
            kem_to_store.CopyFrom(kem)

            kem_id = kem_to_store.id if kem_to_store.id else str(uuid.uuid4())
            kem_to_store.id = kem_id

            current_time_proto = Timestamp(); current_time_proto.GetCurrentTime()

            if kem_id in self.kems and self.kems[kem_id].HasField("created_at"):
                kem_to_store.created_at.CopyFrom(self.kems[kem_id].created_at)
            elif kem_to_store.HasField("created_at") and kem_to_store.created_at.seconds > 0:
                pass # Use provided created_at
            else:
                kem_to_store.created_at.CopyFrom(current_time_proto)

            kem_to_store.updated_at.CopyFrom(current_time_proto)

            # Simulate storage by deep copying
            self.kems[kem_id] = copy.deepcopy(kem_to_store)
            logger.debug(f"InMemoryRepository: Stored KEM ID '{kem_id}'. Total KEMs: {len(self.kems)}")
            return copy.deepcopy(self.kems[kem_id])

    async def retrieve_kems(
        self,
        query: glm_service_pb2.KEMQuery,
        page_size: int,
        page_token: Optional[str]
    ) -> Tuple[List[kem_pb2.KEM], Optional[str]]:
        async with self._lock:
            # Basic filtering implementation for in-memory store
            # This will be a simplified version, not a full query engine.

            candidate_kems = list(self.kems.values())

            # Filter by IDs if provided
            if query.ids:
                ids_set = set(query.ids)
                candidate_kems = [k for k in candidate_kems if k.id in ids_set]

            # Filter by metadata (exact match on all provided filters)
            if query.metadata_filters:
                filtered_by_meta = []
                for k_kem in candidate_kems:
                    match = True
                    for meta_key, meta_val in query.metadata_filters.items():
                        if k_kem.metadata.get(meta_key) != meta_val:
                            match = False
                            break
                    if match:
                        filtered_by_meta.append(k_kem)
                candidate_kems = filtered_by_meta

            # Date filtering (simplified - assumes timestamps are comparable)
            def check_ts_range(kem_ts: Timestamp, start_ts: Timestamp, end_ts: Timestamp) -> bool:
                passes_start = True
                passes_end = True
                if start_ts.seconds > 0 or start_ts.nanos > 0:
                    if kem_ts.seconds < start_ts.seconds or \
                       (kem_ts.seconds == start_ts.seconds and kem_ts.nanos < start_ts.nanos):
                        passes_start = False
                if end_ts.seconds > 0 or end_ts.nanos > 0:
                    if kem_ts.seconds > end_ts.seconds or \
                       (kem_ts.seconds == end_ts.seconds and kem_ts.nanos > end_ts.nanos):
                        passes_end = False
                return passes_start and passes_end

            if query.HasField("created_at_start") or query.HasField("created_at_end"):
                candidate_kems = [k for k in candidate_kems if check_ts_range(k.created_at, query.created_at_start, query.created_at_end)]

            if query.HasField("updated_at_start") or query.HasField("updated_at_end"):
                candidate_kems = [k for k in candidate_kems if check_ts_range(k.updated_at, query.updated_at_start, query.updated_at_end)]

            # Vector search - NOT IMPLEMENTED for in-memory (returns empty if vector query is primary)
            if query.embedding_query:
                logger.warning("InMemoryRepository: Vector search requested but not implemented. Returning based on filters only if combined, or empty.")
                # If vector query is the only criteria, return empty.
                # If combined with filters, this implementation will just return based on filters.
                # For a stricter approach, if embedding_query exists, one might return []
                # unless a specific (mocked) vector match is implemented.
                # For now, we just ignore the vector part of the query.
                pass # Filters above already applied

            # Sort by updated_at desc (default behavior if no other sort specified by query object, which it isn't yet)
            candidate_kems.sort(key=lambda k: (k.updated_at.seconds, k.updated_at.nanos), reverse=True)

            # Pagination
            start_offset = 0
            if page_token:
                try:
                    start_offset = int(page_token)
                except ValueError:
                    logger.warning(f"Invalid page_token '{page_token}', using offset 0.")

            end_offset = start_offset + page_size
            page_kems = candidate_kems[start_offset:end_offset]

            next_page_token_val: Optional[str] = None
            if end_offset < len(candidate_kems):
                next_page_token_val = str(end_offset)

            return [copy.deepcopy(k) for k in page_kems], next_page_token_val

    async def update_kem(self, kem_id: str, kem_update_data: kem_pb2.KEM) -> kem_pb2.KEM:
        async with self._lock:
            if kem_id not in self.kems:
                raise KemNotFoundError(f"KEM ID '{kem_id}' not found for update.")

            existing_kem = self.kems[kem_id]
            updated_kem = kem_pb2.KEM()
            updated_kem.CopyFrom(existing_kem) # Start with existing KEM

            # Apply updates from kem_update_data
            # This is a simple field-by-field update. FieldMask would be more precise.
            if kem_update_data.HasField("content_type"):
                updated_kem.content_type = kem_update_data.content_type
            if kem_update_data.HasField("content"): # Checks if content wrapper is present
                updated_kem.content = kem_update_data.content

            if kem_update_data.metadata: # If metadata map is not empty in update
                updated_kem.metadata.clear()
                updated_kem.metadata.update(kem_update_data.metadata) # Replaces entire metadata

            if kem_update_data.embeddings:
                updated_kem.embeddings[:] = kem_update_data.embeddings # Replaces entire embeddings list

            # Update timestamp
            current_time_proto = Timestamp(); current_time_proto.GetCurrentTime()
            updated_kem.updated_at.CopyFrom(current_time_proto)
            # created_at is preserved from existing_kem

            self.kems[kem_id] = copy.deepcopy(updated_kem)
            logger.debug(f"InMemoryRepository: Updated KEM ID '{kem_id}'.")
            return copy.deepcopy(updated_kem)

    async def delete_kem(self, kem_id: str) -> bool:
        async with self._lock:
            if kem_id in self.kems:
                del self.kems[kem_id]
                logger.debug(f"InMemoryRepository: Deleted KEM ID '{kem_id}'. Total KEMs: {len(self.kems)}")
                return True
            logger.debug(f"InMemoryRepository: KEM ID '{kem_id}' not found for deletion.")
            return False

    async def batch_store_kems(
        self, kems: List[kem_pb2.KEM]
    ) -> Tuple[List[kem_pb2.KEM], List[str]]:
        # For simplicity, call store_kem for each. A real batch might be more optimized.
        # This also means it's not truly atomic for the batch in this in-memory version.
        successful_kems: List[kem_pb2.KEM] = []
        failed_refs: List[str] = []

        for idx, kem_to_process in enumerate(kems):
            original_ref = kem_to_process.id if kem_to_process.id else f"batch_idx_{idx}"
            try:
                # Embedding dimension check (from GLM servicer's BatchStoreKEMs pre-val)
                if kem_to_process.embeddings and len(kem_to_process.embeddings) != self.config.DEFAULT_VECTOR_SIZE:
                    logger.error(f"InMemoryRepository Batch: Invalid embedding dimension for KEM (ref: {original_ref}). Skipping.")
                    failed_refs.append(original_ref)
                    continue

                stored_kem = await self.store_kem(kem_to_process) # Calls the locked store_kem
                successful_kems.append(stored_kem)
            except Exception as e:
                logger.error(f"InMemoryRepository Batch: Error storing KEM (ref: {original_ref}): {e}", exc_info=True)
                failed_refs.append(original_ref)

        return successful_kems, failed_refs

    async def check_health(self) -> Tuple[bool, str]:
        # In-memory is always "healthy" as long as the service is running.
        return True, "InMemoryRepository is active."

```
