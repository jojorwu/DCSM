# DCSM (Dynamic Contextualized Shared Memory) System Architecture

## 1. Introduction

The Dynamic Contextualized Shared Memory (DCSM) system is designed for efficient knowledge management in complex, potentially multi-agent systems. It extends the ideas of approaches like Cache-Augmented Generation (CAG) and Retrieval-Augmented Generation (RAG), focusing on dynamic, distributed, and contextualized access to information. The primary goal is to provide agents with relevant knowledge at the right moment, while optimizing resource usage and fostering collaborative knowledge accumulation.

## 2. Core Principles of DCSM

*   **Contextualized Memory Units (KEMs):** Knowledge is not monolithic but is broken down into semantically coherent "Contextualized Memory Units." Each KEM contains not only the content itself but also rich metadata (e.g., topic, source, recency, access rights, vector embeddings), allowing for more granular and intelligent knowledge management.
*   **Hierarchical Memory Structure:** The system uses a multi-level memory hierarchy to balance access speed and storage capacity:
    *   **Global/Long-Term Memory (GLM):** Persistent storage for all KEMs.
    *   **Shared Working Memory (SWM):** An active caching layer for "hot" KEMs that agents interact with.
    *   **Local Agent Memory (LAM):** A short-term cache on the agent's side (implemented in the Agent SDK) for its most frequently used KEMs.
*   **Shared Memory Bus:** Conceptually implemented through SWM, it allows agents to "subscribe" to relevant KEMs or "publish" new ones, ensuring scalability and flexibility.
*   **Dynamic Pre-loading and Eviction:** SWM dynamically manages its cache by loading relevant KEMs from GLM and evicting less current ones.
*   **Inter-Agent Memory Exchange:** Facilitated by publishing KEMs to SWM, from where they become accessible to other agents or can be saved to GLM.

## 3. System Components

### 3.1. Common Protobuf Definitions (`dcs_memory/common/grpc_protos/`)

*   **Role:** Define data contracts (messages) and service contracts (gRPC) for interaction between all system components. They ensure strong typing and cross-language compatibility.
*   **Key files:**
    *   `kem.proto`: Definition of the Contextualized Memory Unit (KEM) - the basic data structure.
    *   `glm_service.proto`: API for the Global/Long-Term Memory Service.
    *   `kps_service.proto`: API for the Knowledge Processor Service.
    *   `swm_service.proto`: API for the Shared Working Memory Service.

### 3.2. Global/Long-Term Memory Service (GLM)

*   **Location:** `dcs_memory/services/glm/`
*   **Purpose:** Provides reliable and scalable persistent storage for all KEMs in the system.
*   **Core Functions:**
    *   Storing KEMs, including their content, metadata, and vector embeddings.
    *   Retrieving KEMs by their unique identifiers.
    *   Complex querying of KEMs using metadata filtering and date ranges.
    *   Vector searchsimilarity search based on embeddings to find semantically similar KEMs.
    *   Updating and deleting existing KEMs.
    *   **Write Conflict Handling**: When saving KEMs (`StoreKEM`, `BatchStoreKEMs`) with an existing ID, the data is overwritten (logic of `INSERT OR REPLACE` in SQLite and `upsert` in Qdrant).
    *   **Batch Storing and Consistency (`BatchStoreKEMs`)**: The `BatchStoreKEMs` method attempts to save each KEM in the batch. If embeddings for any KEM (if present) cannot be saved to Qdrant, the corresponding record for that KEM is deleted from SQLite to maintain basic consistency between metadata and vector stores. Internally, Qdrant updates for batches are optimized by sending points to Qdrant in a single batch call where possible.
*   **Technologies (current implementation):**
    *   gRPC for API definition and serving.
    *   Qdrant: Used as a vector database for storing embeddings and performing similarity searches.
    *   SQLite: Used for storing structured metadata and the main content of KEMs.
*   **API (gRPC - `glm_service.proto`):**
    *   `StoreKEM`: Saves or updates a single KEM. Generates an ID if not provided.
    *   `RetrieveKEMs`: Retrieves KEMs based on complex criteria, including filters, vector queries, and pagination.
    *   `UpdateKEM`: Partially updates an existing KEM by its ID.
    *   `DeleteKEM`: Deletes a KEM by its ID.

### 3.3. Knowledge Processor Service (KPS)

*   **Location:** `dcs_memory/services/kps/`
*   **Purpose:** Acts as the entry point for new knowledge into the system. It accepts raw data, processes it, generates embeddings, and forms KEMs for subsequent storage in GLM.
*   **Core Functions:**
    *   Accepting raw data of various types (e.g., text, with other formats pTMlanned for the future).
    *   Generating vector embeddings for textual content using models from the `sentence-transformers` library.
    *   Forming KEM objects, including extracting or receiving `content_type`, `raw_content`, `initial_metadata`, and adding the generated `embeddings`.
    *   Interacting with GLM (via its gRPC client) to save the created KEMs.
*   **Technologies (current implementation):**
    *   gRPC for API.
    *   `sentence-transformers`: for generating embeddings.
    *   GLM gRPC client: This client is equipped with retry mechanisms (`@retry_grpc_call`) and circuit breaker patterns for resilience. It also supports client-side load balancing (e.g., `round_robin` via DNS service discovery when GLM is scaled) configurable via shared gRPC client settings. For effective DNS load balancing, the `GRPC_DNS_RESOLVER=ares` environment variable should be set for the KPS service.
*   **API (gRPC - `kps_service.proto`):**
    *   `ProcessRawData`: Accepts raw data and metadata, processes them, and initiates storage in GLM. Returns the ID of the saved KEM and the operation status.

### 3.4. Shared Working Memory Service (SWM)

*   **Location:** `dcs_memory/services/swm/`
*   **Purpose:** Serves as an active, fast caching layer based on Redis, with which agents interact directly. It manages "hot" KEMs, enables collective access, and coordinates memory-related activities.
*   **Core Functions:**
    *   **Active KEM Caching in Redis:** Uses Redis to store KEMs. KEMs are stored as Redis Hashes. Secondary indexes on configurable metadata fields (via Redis Sets) and on creation/update dates (via Redis Sorted Sets) are supported.
    *   **LRU Eviction Policy:** Implemented via Redis configuration (`maxmemory` and `maxmemory-policy allkeys-lru`).
    *   **Loading from GLM:** Retrieves KEMs from GLM upon request (`LoadKEMsFromGLM`) and places them into the Redis cache.
    *   **Cache Querying:** Provides an interface (`QuerySWM`) for agents to request KEMs from the Redis cache with filtering by ID, indexed metadata, and dates. Default sorting is by update date.
    *   **KEM Publishing:** Agents can publish KEMs to SWM (`PublishKEMToSWM`). KEMs are stored in the Redis cache. If persistence is flagged, KEMs are queued for asynchronous batch writing to GLM.
    *   **Event Subscription:** The `SubscribeToSWMEvents` method allows agents to subscribe to KEM creation (`KEM_PUBLISHED`), update (`KEM_UPDATED`), and eviction (`KEM_EVICTED`) events within SWM. Filtering by ID, metadata, and event type is supported. `KEM_EVICTED` events are generated based on Redis Keyspace Notifications.
    *   **Distributed Lock and Counter Management:** Provides mechanisms for agent coordination using Redis as the backend.
        *   `DistributedLockManager` uses Redis `SET resource_key unique_value NX PX lease_ms` for initial lock acquisition. Lock release is performed atomically using a Lua script that verifies the unique lock value. A separate Lua script has been designed for atomic lock lease renewal (checking the unique lock value and using `PEXPIRE`), intended for a future dedicated `renew_lock(current_lock_id, ...)` method; the current `acquire_lock` method always attempts to acquire a new lock instance rather than renewing an existing one based on a prior lock ID.
        *   `DistributedCounterManager` uses Redis atomic increment/decrement operations (`INCRBY`, `GET`).
*   **Caching and Eviction Strategies in SWM**:
    *   **Storage**: Redis. KEMs are stored as Redis Hashes. Secondary indexes for metadata (on keys specified in `SWM_INDEXED_METADATA_KEYS`) and for `created_at`/`updated_at` dates are maintained using Redis Sets and Sorted Sets, respectively.
    *   **Eviction**: Managed by Redis based on the `allkeys-lru` policy and the `maxmemory` setting.
    *   **Eviction Notification**: SWM subscribes to Redis Keyspace Notifications (`__keyevent@<db>__:evicted`). Upon receiving a notification, a `KEM_EVICTED` event (containing only the KEM ID) is generated for SWM subscribers.
*   **SWM Synchronization with GLM**:
    *   **Loading from GLM (`LoadKEMsFromGLM`)**: SWM loads KEMs from GLM and places them into the Redis cache. If a KEM with the same ID already exists in the cache, it is updated. `KEM_PUBLISHED` or `KEM_UPDATED` events are generated.
    *   **Saving to GLM (`PublishKEMToSWM`)**: When publishing a KEM to SWM with the `persist_to_glm_if_new_or_updated` flag, the KEM is added to an internal queue. A background worker (`_glm_persistence_worker`) processes this queue, forming batches and sending them to GLM via `BatchStoreKEMs`. After successful persistence in GLM, the SWM cache is updated with the KEM version returned by GLM (for ID and timestamp consistency).
*   **Technologies (current implementation):**
    *   gRPC for API.
    *   Redis: For the primary KEM cache and secondary indexes. Uses the `aioredis` client.
    *   GLM gRPC client (asynchronous): This client also incorporates retry mechanisms (`@async_retry_grpc_call`) and circuit breakers. It supports client-side load balancing (e.g., `round_robin` via DNS) similarly to the KPS GLM client, requiring appropriate configuration and the `GRPC_DNS_RESOLVER=ares` environment variable for the SWM service.
    *   Internal `asyncio.Queue` for SWM's Pub/Sub mechanism and for the GLM persistence queue.
*   **API (gRPC - `swm_service.proto`):**
    *   `PublishKEMToSWM`: Publishes a KEM to SWM (Redis). Optionally queues it for asynchronous persistence to GLM.
    *   `SubscribeToSWMEvents`: Subscribes to SWM events.
    *   `QuerySWM`: Queries KEMs from the SWM cache (Redis) with filtering.
    *   `LoadKEMsFromGLM`: Initiates loading of KEMs from GLM into the SWM cache (Redis).
    *   `AcquireLock`, `ReleaseLock`, `GetLockInfo`: Manage distributed locks.
    *   `IncrementCounter`, `GetCounter`: Work with distributed counters.

### 3.5. Agent SDK (`dcsm_agent_sdk_python`)

*   **Location:** `dcsm_agent_sdk_python/` (developed as a separate package)
*   **Purpose:** Provides a convenient Python interface for agents to interact with DCSM services (GLM, SWM, KPS).
*   **Core Functions (implemented for GLM and SWM):**
    *   Client for GLM (`GLMClient`) for direct interaction with long-term memory (includes retry logic).
    *   Local Agent Memory (`LocalAgentMemory` or LAM):
        *   Implemented using `IndexedLRUCache` (see section 3.6), providing LRU eviction and metadata indexing capabilities.
        *   Stores KEMs as Python dictionaries.
        *   Indexable keys are configured during `AgentSDK` initialization (`lpa_indexed_keys=...`).
        *   Allows local cache querying via `AgentSDK.query_local_memory()`.
    *   High-level methods in `AgentSDK` (`get_kem`, `store_kems`, `update_kem`, `delete_kem`):
        *   **`get_kem(kem_id, force_remote)`**:
            *   First checks LAM. If found and `force_remote=False`, returns it.
            *   Otherwise, contacts GLM to retrieve the KEM.
            *   The KEM retrieved from GLM is saved/updated in LAM before being returned to the agent.
        *   **`store_kems(kems_data)`**:
            *   Sends a batch of KEMs to GLM for storage.
            *   All successfully stored/updated KEMs (returned by GLM with server-side IDs and timestamps) are placed/updated in LAM.
        *   **`update_kem(kem_id, kem_data_update)`**:
            *   Sends a KEM update request to GLM.
            *   The updated KEM returned by GLM is placed/updated in LAM.
        *   **`delete_kem(kem_id)`**:
            *   Sends a KEM deletion request to GLM.
            *   If successful, the KEM is also deleted from LAM.
        *   Thus, LAM strives to maintain the consistency of cached data relative to the operations the agent itself performs via the SDK. There is no automatic synchronization of LAM with changes made by other agents or directly in GLM/SWM (for this, an agent would need to use the SWM subscription mechanism).
    *   **Interaction with SWM**:
        *   Provides methods for batch publishing KEMs to SWM (`publish_kems_to_swm_batch`) and loading KEMs from SWM into LAM (`load_kems_to_lpa_from_swm`).
        *   Implements utilities for subscribing to SWM events (`handle_swm_events`), including an option for automatic LAM updates based on these events.
        *   Provides high-level methods for working with distributed locks (`acquire_distributed_lock`, `release_distributed_lock`, `get_distributed_lock_info`) and a `distributed_lock` context manager for convenient lock management.
        *   Includes methods for working with SWM's distributed counters (`increment_distributed_counter`, `get_distributed_counter`).
    *   Future prospects: KPS client, more sophisticated coordination utilities for multi-agent systems.

## 4. Component Interaction and Data Flows

### 4.1. Adding New Knowledge to the System (via KPS)

1.  **Initiator** (e.g., an external process, another service, or an agent via a special interface) sends raw data (text, file, etc.) and initial metadata to **KPS** through its `ProcessRawData` method.
2.  **KPS**:
    *   Decodes content if it's text.
    *   Generates vector embeddings for textual content using `sentence-transformers`.
    *   Forms a `KEM` object, populating `content`, `content_type`, `embeddings`, and `initial_metadata`.
    *   Calls the `StoreKEM` method of **GLM**, passing the formed KEM (without an ID; GLM will generate it).
3.  **GLM**:
    *   Generates a unique ID for the KEM.
    *   Sets `created_at` and `updated_at` timestamps.
    *   Saves embeddings and associated payload to Qdrant (see Qdrant point structure description below).
    *   Saves other KEM data to SQLite (see `kems` table description below).
    *   Returns the complete saved KEM (with ID and timestamps) back to KPS.
4.  **KPS** receives the response from GLM and returns the `kem_id` and operation status to the request initiator.

#### 4.1.1. Data Structure in GLM

##### `kems` Table in SQLite (GLM)

*   **Purpose**: Stores the main content and metadata of KEMs.
*   **Structure**:
    *   `id TEXT PRIMARY KEY`: Unique KEM identifier (UUID).
    *   `content_type TEXT`: MIME type of the content (e.g., "text/plain", "application/json").
    *   `content BLOB`: Main KEM content as binary data. Text data is UTF-8 encoded before saving.
    *   `metadata TEXT`: KEM metadata stored as a JSON string. Example: `{"source": "doc.pdf", "topic": "AI"}`.
    *   `created_at TEXT`: KEM creation timestamp in ISO 8601 format (e.g., "YYYY-MM-DDTHH:MM:SS.ffffff"). Set upon first save.
    *   `updated_at TEXT`: KEM last update timestamp in ISO 8601 format. Updated on every save/modification.
*   **Indexes**:
    *   `PRIMARY KEY` on `id`.
    *   Index on `created_at`.
    *   Index on `updated_at`.
*   **PRAGMA Settings (applied at each connection)**:
    *   `PRAGMA journal_mode=WAL;`: Write-Ahead Logging mode for improved concurrency and write performance.
    *   `PRAGMA synchronous=NORMAL;`: Speeds up write operations (with a slight risk of data loss on system power failure).
    *   `PRAGMA foreign_keys=ON;`: Enables foreign key support (good practice, though not used in the current schema).
    *   `PRAGMA busy_timeout = 7500;`: Sets a timeout (in milliseconds) for operations waiting for database lock release.

##### Point Structure in Qdrant (GLM)

*   **Purpose**: Stores vector embeddings for semantic search and filterable attributes.
*   **Point ID**: Corresponds to the KEM `id` from SQLite.
*   **Vector**: An array of floating-point numbers representing the KEM content embedding. Vector dimensionality is defined by configuration (`DEFAULT_VECTOR_SIZE`). The distance metric used is Cosine similarity.
*   **Payload**: A JSON object containing the following fields for filtering and linking to SQLite:
    *   `kem_id_ref: string`: A copy of the KEM ID (for convenience).
    *   `md_<key>: string | number | boolean`: Fields from KEM `metadata`. Each key from the KEM `metadata` dictionary is saved with an `md_` prefix. For example, if `metadata` contains `{"source": "web"}`, the Qdrant payload will have `md_source: "web"`. This allows Qdrant to index and filter by these fields.
    *   `created_at_ts: integer`: KEM creation timestamp in Unix timestamp format (seconds since epoch). Used for date range filtering in Qdrant.
    *   `updated_at_ts: integer`: KEM last update timestamp in Unix timestamp format. Used for date range filtering in Qdrant.

### 4.2. Agent Knowledge Retrieval (via SWM and Agent SDK)

1.  **Agent** (via **Agent SDK**) requests KEMs.
2.  **Agent SDK**:
    *   First checks its Local Agent Memory (LAM). If the KEM is found, returns it.
    *   If not in LAM, the SDK contacts **SWM** with a `QuerySWM` request.
3.  **SWM**:
    *   Searches its internal cache for KEMs matching the criteria from `QuerySWM`.
    *   If KEMs are found in the cache, SWM returns them to the Agent SDK.
    *   If KEMs are not found in the cache, SWM (upon a `LoadKEMsFromGLM` request, initiated by the SDK or the agent itself) forms a `RetrieveKEMs` request to **GLM**.
4.  **GLM**:
    *   Executes the query (ID lookup, vector search, metadata/date filtering).
    *   Returns the requested KEMs to SWM.
5.  **SWM**:
    *   Receives KEMs from GLM, saves/updates them in its internal cache.
    *   Returns the requested KEMs to the Agent SDK.
6.  **Agent SDK**:
    *   Receives KEMs from SWM, saves them in LAM.
    *   Passes the KEMs to the agent.

### 4.3. Agent Knowledge Publication (via SWM and Agent SDK)

1.  **Agent** (via **Agent SDK**) forms a new KEM or data to update an existing one.
2.  **Agent SDK** calls the `PublishKEMToSWM` method of **SWM**, passing the KEM and the `persist_to_glm_if_new_or_updated` flag.
3.  **SWM**:
    *   Assigns/updates timestamps for the KEM within the SWM context.
    *   Saves/updates the KEM in its internal LRU cache.
    *   If the `persist_to_glm_if_new_or_updated` flag is true and a GLM client is available:
        *   SWM calls GLM's `StoreKEM` method to save/update the KEM in long-term memory.
        *   Upon successful persistence in GLM, SWM may update the KEM in its cache with the version returned by GLM (for ID and GLM server-side timestamp consistency).
4.  **SWM** returns a response to the agent about the SWM publication status and (if applicable) the status of the GLM operation.

#### 4.3.1. Data Structure in SWM (Redis-based Cache)

*   **Purpose**: Fast storage for "hot" KEMs for agents' operational access, capable of handling large amounts of data.
*   **Storage**: Redis.
*   **KEM Storage**:
    *   **Main KEM object key:** `swm_kem:<kem_id>` (e.g., `swm_kem:a1b2c3d4-e5f6-7890-1234-567890abcdef`).
    *   **Redis Data Type:** Hash.
    *   **Hash fields for KEM:**
        *   `id`: (string) `<kem_id_str>`
        *   `content_type`: (string) `<kem_mime_type>`
        *   `content`: (byte string) Serialized KEM content (protobuf `kem.content`).
        *   `metadata`: (string) JSON string of all KEM metadata.
        *   `created_at`: (string) ISO 8601 timestamp string of creation.
        *   `updated_at`: (string) ISO 8601 timestamp string of update.
        *   `created_at_ts`: (stringified number) Unix timestamp (seconds) for `created_at` (used for sorting/filtering in Redis).
        *   `updated_at_ts`: (stringified number) Unix timestamp (seconds) for `updated_at` (used for sorting/filtering in Redis).
        *   `embeddings`: (byte string) Serialized list of floats (e.g., via MessagePack).
*   **Secondary Indexing (Manual, without RediSearch):**
    *   **Metadata Indexes:**
        *   **Redis Data Type:** Set.
        *   **Index Key:** `swm_idx:meta:<metadata_key>:<metadata_value>` (e.g., `swm_idx:meta:type:document`).
        *   **Set Members:** KEM IDs (`<kem_id>`).
        *   Atomically updated during KEM set/delete operations using `MULTI/EXEC` transactions.
    *   **Date Indexes:**
        *   **Redis Data Type:** Sorted Set.
        *   **Index Keys:** `swm_idx:date:created_at` and `swm_idx:date:updated_at`.
        *   **Score:** Unix timestamp (integer, seconds).
        *   **Member:** KEM ID (`<kem_id>`).
        *   Atomically updated during KEM set/delete operations.
*   **LRU Eviction Policy:**
    *   Configured in Redis: `maxmemory <size>` and `maxmemory-policy allkeys-lru`.
*   **Eviction Notifications:**
    *   SWM subscribes to Redis Keyspace Notifications (`__keyevent@<db>__:evicted`). The `KEM_EVICTED` event in SWM contains only the ID of the evicted KEM.

#### 4.3.2. Data Structure in LAM (Local Agent Memory in Agent SDK)

*   **Purpose**: Caching an agent's most frequently used KEMs client-side to minimize latency and network traffic.
*   **Storage**: `IndexedLRUCache` (similar to SWM, but operates in the agent's address space).
*   **KEM Format**: KEMs are stored in LAM as Python dictionaries (`dict`). These dictionaries are the result of converting `KEM` protobuf messages received from GLM or SWM.
    *   The dictionary structure accurately mirrors the `KEM` protobuf message fields (e.g., `kem_dict['id']`, `kem_dict['content_type']`, `kem_dict['metadata']` (which is itself a dictionary), `kem_dict['embeddings']` (list of floats), `kem_dict['created_at']` (ISO 8601 string), `kem_dict['updated_at']` (ISO 8601 string)).
*   **Indexing**:
    *   Like in SWM, `IndexedLRUCache` in LAM can index KEMs by the values of specified metadata keys. The list of indexable keys is passed during `AgentSDK` initialization (`AgentSDK(lpa_indexed_keys=["key1", "key2"])`).
    *   The `AgentSDK.query_local_memory()` method uses these indexes for fast KEM filtering within LAM.

## 5. Architecture Diagram (Textual Representation)

```
+-------------------+     +---------------------+     +-------------------+
|      Agents       |<--->|     Agent SDK       |<--->|        SWM        |
|  (KEM Consumers)  |     | (LAM, S/G Clients)  |     |  (Cache, Mem Bus) |
+-------------------+     +---------------------+     +-------------------+
      ^       |                                           |      ^
      |       | (Event Subscription)                      |      | (Load/Persist)
      |       +-------------------------------------------+      |
      | (Direct GLM access, if SWM is bypassed)                |
      |                                                          |
      |               +---------------------+                    v
      +-------------->|         GLM         |<-------------------+
                      | (Qdrant, SQLite)  |
                      +---------------------+
                              ^
                              | (KEM Persistence)
                              |
                      +---------------------+
                      |         KPS         |
                      | (Processing, Embed.)|
                      +---------------------+
                              ^
                              | (Raw Data)
                      +-------------------+
                      |   External Data   |
                      +-------------------+
```
**Key Flows:**
*   **Ingesting New Knowledge:** External Data -> KPS -> GLM.
*   **Agent Interaction with Knowledge (Primary Path):** Agent <-> Agent SDK <-> SWM <-> GLM.
*   **Notifying Agents of Changes (Future Prospect):** SWM -> (event stream) -> Agent SDK -> Agent.

## 6. Future Directions

*   **Full-fledged Pub/Sub in SWM:** Further enhancements to the Pub/Sub mechanism in SWM. While SWM currently uses `asyncio` and per-subscriber `asyncio.Queue`s for event handling, future work could include more sophisticated server-side event filtering, durable subscriptions, dead-letter queues, or optional integration with dedicated message brokers (e.g., Redis Pub/Sub, Kafka, RabbitMQ) for even greater scalability and reliability in large-scale deployments.
*   **More Sophisticated Caching and Eviction Policies in SWM:** For example, based on access frequency, KEM size, or explicit agent-set priorities.
*   **Enhanced Filtering in `QuerySWM`:** Support for more complex queries on cached data, if feasible without turning SWM into a full-fledged database.
*   **Implementation of Other Services:** Such as a service for managing agent configurations or a memory usage analytics service.
*   **Security:** Adding authentication (e.g., gRPC token-based) and authorization (granular access rights to KEMs and service methods).
*   **Containerization and Deployment:** Preparing `Dockerfile` for each service and `docker-compose.yml` examples to simplify local deployment and testing of the entire system. Transitioning to Kubernetes for production environments.
*   **Comprehensive Integration and Load Testing.**
*   **Error Handling and Fault Tolerance (Current State and Development):**
    *   **Current State**:
        *   Inter-service gRPC calls (KPS->GLM, SWM->GLM, AgentSDK->Services) are protected by configurable retry mechanisms and circuit breakers to handle transient network issues or temporary service unavailability.
        *   Client-side load balancing (e.g., `round_robin`) can be configured for these calls if the target service (e.g., GLM) is deployed with multiple instances and service discovery (e.g., DNS) is set up accordingly. This typically requires using the `dns:///` scheme in the target address and setting the `GRPC_DNS_RESOLVER=ares` environment variable in the client service's environment.
        *   The GLM service has basic logic (`BatchStoreKEMs`, `StoreKEM`) to improve consistency between SQLite and Qdrant during writes, including some compensating actions on failure.
        *   The GLM service will not start if Qdrant is unavailable at startup. KPS and SWM log warnings if GLM is unavailable at their startup but may continue with limited functionality. SWM also has a critical dependency on Redis.
        *   SWM includes a Dead Letter Queue (DLQ) for KEMs that fail persistence to GLM after multiple retries.
        *   KPS includes an optional idempotency check for `ProcessRawData` requests based on `data_id`.
    *   **Development Directions**:
        *   More granular error handling and rollback strategies, particularly for GLM's multi-datastore operations.
        *   Further refinement of circuit breaker interaction with specific gRPC error codes if needed.
        *   Improved service behavior logique during prolonged unavailability of critical dependencies.
        *   **Health Checks**: All gRPC services (GLM, KPS, SWM) expose standard gRPC health check endpoints (`grpc.health.v1.Health/Check`). These checks verify critical dependencies (e.g., GLM checks SQLite/Qdrant; KPS checks model/GLM; SWM checks Redis/GLM), allowing orchestrators to assess service health accurately.

### 3.6. The `IndexedLRUCache` Mechanism

The SWM and Agent SDK (for LAM) components use a custom cache implementation called `IndexedLRUCache`. This cache extends standard LRU (Least Recently Used) logic by adding the capability to index stored objects by their metadata values for faster selective queries.

**Key Characteristics and Operational Logic:**

1.  **LRU Eviction Strategy**:
    *   `IndexedLRUCache` is based on `cachetools.LRUCache`. When the cache reaches its maximum size (`maxsize`), adding a new item automatically evicts the least recently used item.
    *   Any access operation on an item (read `get` or update `__setitem__`) marks that item as "recently used," delaying its eviction.

2.  **Metadata Indexing**:
    *   When initializing `IndexedLRUCache` (e.g., in SWM or LAM), a list of metadata keys (`indexed_keys`) can be specified for indexing.
    *   For each such key, the cache creates an internal reverse index (a dictionary) where each value of that metadata key maps to a set (`set`) of KEM IDs that have this metadata key-value pair.
    *   **Example**: If `indexed_keys = ["topic", "source"]`, and a KEM with `id="kem1"` and `metadata={"topic": "AI", "source": "doc.pdf"}` is added to the cache, then:
        *   In the index for `topic`, `"kem1"` will be added under the key `"AI"`.
        *   In the index for `source`, `"kem1"` will be added under the key `"doc.pdf"`.
    *   Only string metadata values are indexed.

3.  **Index Updates**:
    *   **Adding/Updating KEMs (`__setitem__`)**:
        *   If a KEM is updated, its old metadata values are first removed from the indexes.
        *   Then, the new (or current) values of indexable metadata are added to the indexes.
        *   Finally, the KEM is placed (or updated) in the main LRU cache.
    *   **Deleting KEMs (`__delitem__`, `pop`)**: When a KEM is deleted from the cache, corresponding entries are also removed from all metadata indexes.
    *   **Eviction**: When an item is evicted by the LRU logic, it is also removed from the metadata indexes. In SWM, an `on_evict_callback` is used for this.

4.  **Using Indexes in Queries**:
    *   The `QuerySWM` method (in SWM) and `LocalAgentMemory.query()` (in Agent SDK) use these indexes to speed up filtering.
    *   If a query contains filters on indexed metadata keys, the cache first retrieves sets of KEM IDs from the respective indexes.
    *   These sets of IDs are then intersected (if there are multiple indexed key filters) to get a list of KEM IDs 맛있는isfying all indexed conditions.
    *   Only then are the full KEM objects retrieved from the main LRU storage by these IDs for further non-indexed filtering (if any) and returning the result.
    *   This is significantly more efficient than a full scan of all cache items for every filtered query.

5.  **Thread Safety**:
    *   All operations on `IndexedLRUCache` (add, delete, read, update indexes) are protected by a `threading.Lock`. This ensures correct operation in the multi-threaded environment of SWM services and potentially in multi-threaded agents using LAM.

6.  **Stored Data**:
    *   In SWM, `IndexedLRUCache` stores `kem_pb2.KEM` protobuf messages directly.
    *   In LAM (Agent SDK), `IndexedLRUCache` stores KEMs as Python dictionaries. The indexing logic is adapted to work with dictionaries.

`IndexedLRUCache` represents a trade-off between access speed, memory consumption for indexes, and implementation complexity, allowing for efficient work with cached data in operational memory.

## 7. API Usage Examples (gRPC)

This section demonstrates examples of forming requests to the system's main services. The examples are presented as conceptual structures, similar to how they would look when using a Python gRPC client.

### 7.1. GLM (GlobalLongTermMemory)

#### 7.1.1. `RetrieveKEMs`: Vector Search with Filtering

**Scenario**: Find up to 10 KEMs semantically similar to a given text, but only those created after a specific date and having metadata `source="manual"` and `topic="AI"`.

```python
# Example Python-like code for request formation
# import kem_pb2 # Assuming kem_pb2 is available for KEM structure if needed by client
# import glm_service_pb2 # Assuming available for KEMQuery, RetrieveKEMsRequest
# from google.protobuf.timestamp_pb2 import Timestamp # For date fields
# from datetime import datetime # For creating datetime objects

# 1. KEMQuery
kem_query = glm_service_pb2.KEMQuery()

# Vector query (it's assumed that a text_query would be converted to an embedding_query
# by KPS or another component, or the client provides the embedding directly)
# For this example, embedding_query is specified directly
kem_query.embedding_query.extend([0.1, 0.2, ..., 0.N]) # Replace with an actual vector of the correct dimension

# Metadata filters (translated to Qdrant payload filter: "md_source" and "md_topic")
kem_query.metadata_filters["source"] = "manual"
kem_query.metadata_filters["topic"] = "AI"

# Filter by creation date (translated to Qdrant payload filter: "created_at_ts")
created_after_ts = Timestamp()
created_after_ts.FromDatetime(datetime(2023, 1, 1, 0, 0, 0)) # Example date
kem_query.created_at_start.CopyFrom(created_after_ts)

# 2. RetrieveKEMsRequest
retrieve_request = glm_service_pb2.RetrieveKEMsRequest(
    query=kem_query,
    page_size=10
)

# Expected GLM behavior:
# 1. Builds a filter for Qdrant based on metadata_filters and created_at_start.
#    The filter will be approximately:
#    Filter(must=[
#        FieldCondition(key="md_source", match=MatchValue(value="manual")),
#        FieldCondition(key="md_topic", match=MatchValue(value="AI")),
#        FieldCondition(key="created_at_ts", range=Range(gte=1672531200)) # 1672531200 is Unix timestamp for 2023-01-01
#    ])
# 2. Performs a vector search in Qdrant with this filter and embedding_query.
# 3. Retrieves full KEM data from SQLite for IDs obtained from Qdrant.
# 4. Returns a list of KEMs.
```

### 7.2. SWM (SharedWorkingMemoryService)

#### 7.2.1. `QuerySWM`: Querying the Cache with Complex Filtering

**Scenario**: Find up to 5 KEMs in the SWM cache that have an `id` from the list `["id1", "id2", "id3"]`, OR have metadata `category="urgent"` (assuming `category` is indexed in SWM), AND were updated within the last hour.

```python
# Example Python-like code for request formation
# import glm_service_pb2 # KEMQuery is also used for SWM
# import swm_service_pb2
# from google.protobuf.timestamp_pb2 import Timestamp
# from datetime import datetime, timedelta

# 1. KEMQuery for SWM
# SWM does not support embedding_query or text_query for its cache.
swm_kem_query = glm_service_pb2.KEMQuery()

# Filter by ID (if specified, SWM will select by these first, or in conjunction with other filters)
swm_kem_query.ids.extend(["id1", "id2", "id3"])

# Filter by metadata (assume "category" is indexed in SWM)
swm_kem_query.metadata_filters["category"] = "urgent"
# If "category" is not indexed, SWM will perform a full scan (after ID filtering, if any).

# Filter by update date (within the last hour)
one_hour_ago_ts = Timestamp()
one_hour_ago_ts.FromDatetime(datetime.utcnow() - timedelta(hours=1))
swm_kem_query.updated_at_start.CopyFrom(one_hour_ago_ts)

# 2. QuerySWMRequest
query_swm_request = swm_service_pb2.QuerySWMRequest(
    query=swm_kem_query,
    page_size=5
)

# Expected SWM behavior:
# The SWM service applies filters in a specific order:
# a) Applies indexed metadata filters (if any).
# b) To the result (or to the whole cache if no indexed filters were used), applies the filter by query.ids (if any).
# c) To that result, applies non-indexed metadata filters.
# d) Finally, applies date filters to the result.
# This example will operate according to this logic. The interaction between ID filters and metadata filters is effectively an AND if both are present and processed sequentially on the filtered set.
# The "OR" logic mentioned in the original Russian scenario ("ИЛИ у которых метаданные category='urgent'") is not directly supported by a single KEMQuery structure in the current SWM implementation; it would require separate queries or a more complex query language.
```

#### 7.2.2. `SubscribeToSWMEvents`: Subscribing to Specific Events

**Scenario**: An agent wants to receive notifications for all KEMs with metadata `project_id="project_alpha"`, and also for all changes to the KEM with `id="important_kem_id"`.

```python
# Example Python-like code for request formation
# import swm_service_pb2

subscribe_request = swm_service_pb2.SubscribeToSWMEventsRequest(
    agent_id="my_smart_agent_007",
    # Optional: Client can suggest a queue size for its events.
    # Server may apply bounds or use its default if 0 or not provided.
    # requested_queue_size=50
)

# Topic 1: KEMs by project_id
topic1 = swm_service_pb2.SubscriptionTopic(
    type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS, # General event type
    filter_criteria="metadata.project_id=project_alpha" # Filter
)
subscribe_request.topics.append(topic1)

# Topic 2: Specific KEM by ID
topic2 = swm_service_pb2.SubscriptionTopic(
    type=swm_service_pb2.SubscriptionTopic.TopicType.KEM_LIFECYCLE_EVENTS,
    filter_criteria="kem_id=important_kem_id"
)
subscribe_request.topics.append(topic2)

# Expected SWM behavior:
# The agent will receive events (KEM_PUBLISHED, KEM_UPDATED, KEM_EVICTED)
# for KEMs satisfying at least one of the filter_criteria.
# For example, if a KEM with metadata={"project_id":"project_alpha", "other":"data"} is published, the agent receives an event.
# If the KEM with id="important_kem_id" is updated, the agent receives an event.
```

These examples illustrate how more complex API requests can be formed. Actual interaction with gRPC clients would require appropriate channel setup and stub method calls.
