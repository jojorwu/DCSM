import grpc
# Используем относительные импорты, если glm_client.py находится в том же пакете,
# что и generated_grpc_code. Если generated_grpc_code - это подпакет,
# то импорты должны быть from .generated_grpc_code import ...
# Предполагаем, что dcsm_agent_sdk_python будет добавлен в PYTHONPATH
# или используется структура пакетов, где generated_grpc_code доступен.
# Для простоты, если generated_grpc_code - это директория на том же уровне, что и glm_client.py,
# и оба они являются частью пакета dcsm_agent_sdk_python, то:
from .generated_grpc_code import kem_pb2
from .generated_grpc_code import glm_service_pb2
from .generated_grpc_code import glm_service_pb2_grpc
# from google.protobuf.json_format import MessageToDict, ParseDict # Replaced by utils
from google.protobuf import empty_pb2 # For DeleteKEM
from dcs_memory.common.grpc_utils import retry_grpc_call
from .proto_utils import kem_dict_to_proto, kem_proto_to_dict

import logging
logger = logging.getLogger(__name__)


class GLMClient:
    def __init__(self, server_address='localhost:50051',
                 retry_max_attempts=3,
                 retry_initial_delay_s=1.0,
                 retry_backoff_factor=2.0):
        self.server_address = server_address
        self.channel = None
        self.stub = None
        self.retry_max_attempts = retry_max_attempts
        self.retry_initial_delay_s = retry_initial_delay_s
        self.retry_backoff_factor = retry_backoff_factor
        # self.connect() # Connection is now typically managed by __enter__ or _ensure_connected

    def connect(self):
        if not self.channel:
            self.channel = grpc.insecure_channel(self.server_address)
            self.stub = glm_service_pb2_grpc.GlobalLongTermMemoryStub(self.channel)
            logger.info(f"GLMClient connected to: {self.server_address}")

    def _ensure_connected(self):
        if not self.stub:
            self.connect()

    # _kem_dict_to_proto and _kem_proto_to_dict are now imported from proto_utils

    @retry_grpc_call
    def batch_store_kems(self, kems_data: list[dict]) -> tuple[list[dict] | None, list[str] | None, str | None]:
        """Stores a batch of KEMs and returns a list of successfully stored KEMs (as dicts) and errors."""
        self._ensure_connected()
        proto_kems = [kem_dict_to_proto(data) for data in kems_data]
        request = glm_service_pb2.BatchStoreKEMsRequest(kems=proto_kems)
        response = self.stub.BatchStoreKEMs(request, timeout=20) # type: ignore
        successfully_stored_kems_as_dicts = [kem_proto_to_dict(k) for k in response.successfully_stored_kems]
        return successfully_stored_kems_as_dicts, list(response.failed_kem_references), response.overall_error_message

    @retry_grpc_call
    def retrieve_kems(self, text_query: str = None, embedding_query: list[float] = None,
                      metadata_filters: dict = None, ids_filter: list[str] = None,
                      page_size: int = 10, page_token: str = None) -> tuple[list[dict] | None, str | None]:
        self._ensure_connected()
        query_proto = glm_service_pb2.KEMQuery()
        if text_query:
            query_proto.text_query = text_query
        if embedding_query:
            query_proto.embedding_query.extend(embedding_query)
        if metadata_filters:
            for key, value in metadata_filters.items():
                query_proto.metadata_filters[key] = str(value) # Ensure value is string for proto
        if ids_filter:
            query_proto.ids.extend(ids_filter)
        request = glm_service_pb2.RetrieveKEMsRequest(query=query_proto, page_size=page_size, page_token=page_token if page_token else "")
        response = self.stub.RetrieveKEMs(request, timeout=10) # type: ignore
        kems_as_dicts = [kem_proto_to_dict(kem) for kem in response.kems]
        return kems_as_dicts, response.next_page_token

    @retry_grpc_call
    def update_kem(self, kem_id: str, kem_data_update: dict) -> dict | None:
        self._ensure_connected()
        kem_proto_update = kem_dict_to_proto(kem_data_update)
        request = glm_service_pb2.UpdateKEMRequest(kem_id=kem_id, kem_data_update=kem_proto_update)
        response_kem_proto = self.stub.UpdateKEM(request, timeout=10) # type: ignore
        return kem_proto_to_dict(response_kem_proto)

    @retry_grpc_call
    def delete_kem(self, kem_id: str) -> bool:
        self._ensure_connected()
        request = glm_service_pb2.DeleteKEMRequest(kem_id=kem_id)
        self.stub.DeleteKEM(request, timeout=10) # type: ignore
        return True

    def close(self):
        if self.channel:
            self.channel.close()
            self.channel = None
            self.stub = None
            logger.info("GLMClient channel closed.")

    def __enter__(self):
        self.connect() # Connect on entering context manager
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

if __name__ == '__main__':
    # This block executes only when the file is run directly (python glm_client.py)
    # It will not run when GLMClient is imported from another module.
    # Requires GLM server to be running on localhost:50051 for this example.

    import os
    import sys
    # Add parent directory to sys.path to allow direct execution for demonstration
    # and to make generated_grpc_code imports work.
    # This is not ideal if generated_grpc_code is not an installable package.
    # Easiest to run example from SDK root, adding it to PYTHONPATH,
    # or ensure generated_grpc_code is installed as part of the package.

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Running GLMClient example (requires GLM server on localhost:50051)")
    logger.info("This example will not run automatically during automated tasks.")

    if os.getenv("RUN_GLM_CLIENT_EXAMPLE") == "true": # Run example only if env var is set
        try:
            with GLMClient() as client:
                # 1. Store KEMs
                logger.info("\n--- Testing batch_store_kems ---")
                kems_to_store = [
                    {"id": "kem_sdk_001", "content_type": "text/plain", "content": "KEM 1 from SDK.", "metadata": {"sdk_source": "python_glm_client", "topic": "sdk_test"}},
                    {"id": "kem_sdk_002", "content_type": "application/json", "content": '{"data_key": "data_value"}', "metadata": {"sdk_source": "python_glm_client", "status": "draft"}, "embeddings": [0.4, 0.5, 0.6]}
                ]
                successful_kems, failed_refs, error_msg = client.batch_store_kems(kems_to_store) # Corrected method name

                if successful_kems:
                    logger.info(f"Successfully stored KEMs: {[k['id'] for k in successful_kems]} (Total: {len(successful_kems)})")
                if failed_refs:
                    logger.error(f"Failed to store KEMs (references): {failed_refs}")
                if error_msg:
                    logger.error(f"Overall error from batch_store_kems: {error_msg}")

                # 2. Retrieve KEMs
                logger.info("\n--- Testing retrieve_kems (by metadata) ---")
                retrieved_kems_tuple = client.retrieve_kems(metadata_filters={"sdk_source": "python_glm_client"}, page_size=5) # Corrected parameter name
                retrieved_kems_meta = retrieved_kems_tuple[0] if retrieved_kems_tuple else None

                if retrieved_kems_meta is not None:
                    logger.info(f"Retrieved {len(retrieved_kems_meta)} KEMs by metadata:")
                    for k in retrieved_kems_meta:
                        content_preview = k.get('content', '')[:30] if isinstance(k.get('content'), str) else 'N/A'
                        logger.info(f"  ID: {k.get('id')}, Content: {content_preview}...")
                else:
                    logger.warning("Could not retrieve KEMs by metadata.")

                # 3. Update KEM (assuming kem_sdk_001 exists)
                logger.info("\n--- Testing update_kem ---")
                if successful_kems and any(k_dict.get('id') == "kem_sdk_001" for k_dict in successful_kems): # Check against successful_kems
                    updated_data = {"metadata": {"sdk_source": "python_glm_client_v2", "status": "final"}}
                    updated_kem = client.update_kem("kem_sdk_001", updated_data)
                    if updated_kem:
                        logger.info(f"KEM kem_sdk_001 updated: {updated_kem.get('metadata')}")
                    else:
                        logger.error("Failed to update KEM kem_sdk_001")
                else:
                    logger.info("Skipping UpdateKEM test as kem_sdk_001 was not successfully stored or ID not found.")

                # 4. Delete KEM (assuming kem_sdk_002 exists)
                logger.info("\n--- Testing delete_kem ---")
                if successful_kems and any(k_dict.get('id') == "kem_sdk_002" for k_dict in successful_kems): # Check against successful_kems
                    delete_success = client.delete_kem("kem_sdk_002")
                    if delete_success:
                        logger.info("KEM kem_sdk_002 successfully deleted.")
                    else:
                        logger.error("Failed to delete KEM kem_sdk_002.")
                else:
                    logger.info("Skipping DeleteKEM test as kem_sdk_002 was not successfully stored or ID not found.")
        except Exception as e:
            logger.error(f"An error occurred during the example run: {e}", exc_info=True)
            import traceback
            traceback.print_exc()
    else:
        logger.info("Environment variable RUN_GLM_CLIENT_EXAMPLE not set to 'true', example will not run.")
