# GLM (Global Long-Term Memory) Service

This document describes the gRPC service for the Global Long-Term Memory (GLM) component of the Dynamic Contextualized Shared Memory (DCSM) project. The service provides an API for storing, retrieving, updating, and deleting Contextualized Memory Units (KEMs).

This service uses centralized `.proto` definitions located in `dcs_memory/common/grpc_protos/`.

## Storage Architecture

The GLM service uses a dual-backend storage architecture to handle both structured metadata and high-dimensional vector data efficiently.

*   **Vector Database: Qdrant**
    -   Used for storing KEM embeddings and performing high-speed similarity searches.

*   **Metadata and Content Database: SQLite**
    -   Used for storing the KEMs themselves, including their ID, content type, main content, metadata (as a JSON string), and timestamps.
    -   The database file is created by default as `app/glm_metadata.sqlite3` within the service's directory.
    -   The SQLite backend is fully asynchronous, using `aiosqlite` for non-blocking I/O operations.
    -   It supports full-text search (FTS5) on the content and metadata of KEMs.

## Prerequisites

*   Python 3.8+
*   Pip
*   Docker (for running Qdrant locally if not using a cloud version)

## Setup and Running

1.  **Clone the repository** (if you haven't already).

2.  **Set up Qdrant:**
    For local development, it is recommended to run Qdrant using Docker:
    ```bash
    docker pull qdrant/qdrant
    docker run -p 6333:6333 -p 6334:6334 \
        -v $(pwd)/qdrant_storage:/qdrant/storage:z \
        qdrant/qdrant
    ```
    This will start Qdrant and persist its data in the `qdrant_storage` directory in your current working directory. The GLM service will attempt to connect to Qdrant at the address specified in the configuration.

3.  **Navigate to the service directory:**
    ```bash
    cd dcs_memory/services/glm
    ```

4.  **Create and activate a virtual environment (recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

5.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    (This includes `qdrant-client`, `grpcio`, `aiosqlite`, etc.)

6.  **Generate gRPC code (if you modify `.proto` files):**
    This service uses `.proto` files from the central `../../common/grpc_protos/` directory. The `generate_grpc_code.sh` script in the current directory generates the Python code into the local `generated_grpc/` directory.
    ```bash
    ./generate_grpc_code.sh
    ```
    *Ensure the script is executable (`chmod +x generate_grpc_code.sh`).*

7.  **Configure Environment Variables (optional):**
    The service is configured via the central `config.yml` file, but can be overridden by environment variables. For the GLM service, these variables should be prefixed with `GLM_`.
    *   `GLM_SQLITE_DB_FILENAME`: The filename for the SQLite database (default: `glm_metadata.sqlite3` inside the `app/` directory).
    *   `GLM_QDRANT_HOST`: The Qdrant host (default: `localhost`).
    *   `GLM_QDRANT_PORT`: The Qdrant gRPC port (default: `6333`).
    *   `GLM_QDRANT_COLLECTION`: The collection name in Qdrant (default: `glm_kems_demo_collection`).
    *   `GLM_DEFAULT_VECTOR_SIZE`: The dimension of vectors for Qdrant (default: `25`). **Important:** This value must match the dimension of the embeddings you will be storing.
    *   `GLM_DEFAULT_PAGE_SIZE`: The default page size for `RetrieveKEMs` (default: `10`).
    *   `GLM_GRPC_LISTEN_ADDRESS`: The address for the gRPC server to listen on (default: `[::]:50051`).
    *   `GLM_KPS_SERVICE_ADDRESS`: The address of the KPS service, used for the `IndexExternalDataSource` RPC.

8.  **Run the gRPC Server:**
    From the `dcs_memory/services/glm/` directory:
    ```bash
    python -m app.main
    ```
    The server will attempt to connect to Qdrant and initialize the SQLite database. Logs will be output to the console.

## API (gRPC)

The service implements the gRPC interface defined in `glm_service.proto` (located in `dcs_memory/common/grpc_protos/`).

Key Methods:
*   **`StoreKEM(StoreKEMRequest) returns (StoreKEMResponse)`**:
    *   Stores a single KEM. If the `id` in the `KEM` is not specified, the server generates one.
    *   Returns the complete stored `KEM` object, including the server-generated `id` and timestamps.
*   **`RetrieveKEMs(RetrieveKEMsRequest) returns (RetrieveKEMsResponse)`**:
    *   Retrieves KEMs based on various criteria specified in the `KEMQuery`.
    *   Supports filtering by a list of IDs, vector search (via `embedding_query`), full-text search (via `text_query`), metadata filters, and date ranges.
    *   Supports pagination via `page_size` and `page_token`.
    *   Returns a list of KEMs and a `next_page_token`.
*   **`UpdateKEM(UpdateKEMRequest) returns (KEM)`**:
    *   Updates an existing KEM identified by `kem_id`. The fields to be updated are passed in `kem_data_update`.
    *   The `updated_at` timestamp is automatically updated. `created_at` is preserved.
    *   Returns the full, updated `KEM` object.
*   **`DeleteKEM(DeleteKEMRequest) returns (google.protobuf.Empty)`**:
    *   Deletes a KEM by `kem_id` from all storage backends (SQLite and Qdrant).
*   **`BatchStoreKEMs(BatchStoreKEMsRequest) returns (BatchStoreKEMsResponse)`**:
    *   Efficiently stores a batch of KEMs in a single transaction.
*   **`IndexExternalDataSource(IndexExternalDataSourceRequest) returns (IndexExternalDataSourceResponse)`**:
    *   Triggers the indexing of a configured external data source by fetching its data and sending it to the KPS for processing.

Please refer to the `kem.proto` and `glm_service.proto` files for detailed descriptions of all messages and fields.

## Testing

The `app/` directory contains a `test_repositories.py` file with unit and integration tests for the repository layer. These tests use an in-memory SQLite database and can be run with `pytest`.

For manual testing of the gRPC service, you can use a gRPC client such as `grpcurl` or a Python client (e.g., from the `dcsm_agent_sdk_python` package).
