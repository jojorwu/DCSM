# DCSM Memory Operations and Modifications Guide

This document provides guidance for developers on how memory, particularly within the Global Ledger Memory (GLM) and its interaction with SQLite, operates within the Dynamic Contextualized Shared Memory (DCSM) system. It also outlines how to approach modifications to this part of the codebase.

## Table of Contents

- [Introduction](#introduction)
- [GLM Memory Architecture](#glm-memory-architecture)
- [Key File Responsibilities](#key-file-responsibilities)
- [How Memory Operations Work (GLM Focus)](#how-memory-operations-work-glm-focus)
- [Modifying the Code](#modifying-the-code)
- [Verifying Changes (Code Work)](#verifying-changes-code-work)

## Introduction

The DCSM system is designed for efficient knowledge management in complex AI agent systems. It features a three-tiered memory architecture:

1.  **GLM (Global Ledger Memory):** The persistent, long-term storage for all Contextualized Memory Units (KEMs).
2.  **SWM (Shared Working Memory):** A high-performance caching layer for "hot" KEMs.
3.  **LAM (Local Agent Memory):** A short-term cache on each agent's side.

This document primarily focuses on the **GLM** component, especially its internal workings with SQLite and Qdrant, to provide context for tasks such as `glm-sqlite-optimizations`. Understanding GLM is crucial for maintaining data integrity and performance of the entire memory system.

## GLM Memory Architecture

GLM serves as the authoritative, persistent store for all KEMs. It employs a dual-database design to balance structured data storage, content storage, and efficient semantic search capabilities:

1.  **SQLite Database:**
    *   **Purpose:** Stores the primary metadata and content of KEMs.
    *   **Data Stored:**
        *   `id`: Unique identifier for the KEM (TEXT, PRIMARY KEY).
        *   `content_type`: MIME type of the KEM content (TEXT).
        *   `content`: The actual binary content of the KEM (BLOB).
        *   `metadata`: A JSON string containing various metadata fields associated with the KEM (TEXT).
        *   `created_at`: Timestamp of KEM creation (TEXT, ISO 8601 format).
        *   `updated_at`: Timestamp of the last KEM update (TEXT, ISO 8601 format).
    *   **Optimizations:** To enhance performance and concurrency, GLM's SQLite implementation typically utilizes:
        *   `PRAGMA journal_mode=WAL` (Write-Ahead Logging).
        *   `PRAGMA synchronous=NORMAL` (Less strict synchronicity for faster writes).
        *   `PRAGMA foreign_keys=ON` (Enforcing foreign key constraints, if applicable).
        *   A `busy_timeout` setting (configurable via `SQLITE_BUSY_TIMEOUT` in `GLMConfig`) to handle concurrent access.
    *   The specifics of SQLite interaction are generally managed within a dedicated repository class (e.g., `SqliteKemRepository`).

2.  **Qdrant Vector Database:**
    *   **Purpose:** Stores vector embeddings of KEM content to enable powerful semantic search and similarity lookups.
    *   **Data Stored:**
        *   Each KEM with embeddings has a corresponding point in Qdrant, identified by the KEM's `id`.
        *   The vector embedding itself.
        *   A payload associated with the vector, containing:
            *   `kem_id_ref`: The KEM ID, for linking back to the SQLite record.
            *   `md_<key>`: Indexed fields derived from the KEM's metadata for efficient filtering during vector searches (e.g., `md_topic`, `md_source`).
            *   `created_at_ts`: Creation timestamp as a Unix epoch integer for range filtering.
            *   `updated_at_ts`: Update timestamp as a Unix epoch integer for range filtering.
    *   Qdrant interaction is managed by a dedicated repository class (e.g., `QdrantKemRepository`).

The orchestration between SQLite and Qdrant, ensuring that data operations are handled consistently across both, is primarily the responsibility of the `DefaultGLMRepository` class found in `dcs_memory/services/glm/app/repositories/default_impl.py`.

## Key File Responsibilities

Understanding the roles of these key files is essential for navigating and modifying the GLM codebase:

*   **`dcs_memory/services/glm/app/repositories/default_impl.py` (`DefaultGLMRepository`):**
    *   The central orchestrator for GLM's storage operations.
    *   It implements the `BasePersistentStorageRepository` interface.
    *   Coordinates calls to `SqliteKemRepository` and `QdrantKemRepository` for storing, retrieving, updating, and deleting KEMs.
    *   Contains critical logic for maintaining data consistency between SQLite and Qdrant, including rollback mechanisms.

*   **`dcs_memory/services/glm/app/repositories/sqlite_repo.py` (`SqliteKemRepository`):**
    *   (File path assumed based on imports in `default_impl.py`)
    *   Handles all direct interactions with the SQLite database.
    *   Responsible for executing CRUD (Create, Read, Update, Delete) operations on the `kems` table.
    *   Manages SQLite connection, schema setup (if applicable), and PRAGMA configurations.

*   **`dcs_memory/services/glm/app/repositories/qdrant_repo.py` (`QdrantKemRepository`):**
    *   (File path assumed based on imports in `default_impl.py`)
    *   Handles all direct interactions with the Qdrant vector database.
    *   Responsible for CRUD operations on vector points (including their payloads).
    *   Manages Qdrant client connection and collection setup (e.g., ensuring the collection exists with the correct configuration).

*   **`dcs_memory/services/glm/app/main.py` (`GlobalLongTermMemoryServicerImpl`):**
    *   The entry point for all gRPC requests to the GLM service.
    *   Implements the gRPC service interface defined in `glm_service.proto`.
    *   Receives incoming requests, validates them, and calls appropriate methods on an instance of `DefaultGLMRepository`.
    *   Transforms data between protobuf messages and internal representations.
    *   Handles gRPC-specific error mapping and responses.
    *   Manages the gRPC server lifecycle and health checks.

*   **`dcs_memory/common/config.py` (`GLMConfig`, `BaseServiceConfig`):**
    *   Defines the configuration structure for the GLM service (and other services) using Pydantic.
    *   `GLMConfig` includes parameters specific to GLM, such as database file paths (`DB_FILENAME`), Qdrant connection details (`QDRANT_HOST`, `QDRANT_PORT`, `QDRANT_COLLECTION`), timeouts (`SQLITE_BUSY_TIMEOUT`), default vector sizes, and page sizes.
    *   `BaseServiceConfig` provides shared configuration options like logging settings, gRPC server parameters, retry logic, circuit breaker settings, and TLS paths.

*   **`config.yml` (typically at the project root):**
    *   The primary YAML file used at runtime to provide actual configuration values for all services.
    *   Values in this file override defaults set in `config.py` and can be overridden by environment variables.

*   **`dcs_memory/ARCHITECTURE_EN.md` (and `ARCHITECTURE.md`):**
    *   These documents provide a high-level overview of the entire DCSM system architecture, data flows, and the role of GLM within the system. They are essential reading for understanding the context of any GLM modifications.

## How Memory Operations Work (GLM Focus)

GLM operations revolve around the lifecycle of KEMs.

### Storing and Updating KEMs

1.  **Request Path:** An agent or service sends a gRPC request (`StoreKEM`, `BatchStoreKEMs`, or `UpdateKEM`) to the GLM service endpoint defined in `dcs_memory/services/glm/app/main.py`.
2.  **Servicer Handling:** The `GlobalLongTermMemoryServicerImpl` receives the request, performs initial validation (e.g., embedding dimensions), and then calls the corresponding method in `DefaultGLMRepository`.
3.  **`DefaultGLMRepository` Logic (`store_kem` / `update_kem`):**
    *   **ID Generation:** For new KEMs (in `StoreKEM`), if no ID is provided, a UUID is generated.
    *   **Timestamp Management:** `created_at` is set for new KEMs (or preserved if updating and KEM exists and client provides it). `updated_at` is always set to the current time of the operation.
    *   **Qdrant Operation (Conditional):** If the KEM includes embeddings and Qdrant is configured:
        *   The repository attempts to upsert (insert or update) the vector point in Qdrant. This includes the vector and a payload containing `kem_id_ref`, filterable metadata (`md_<key>`), and timestamps (`created_at_ts`, `updated_at_ts`).
    *   **SQLite Operation:**
        *   The repository then attempts to store or replace the KEM's data (content, metadata JSON, content_type, timestamps) in the SQLite `kems` table.
    *   **Consistency and Rollback (Crucial):**
        *   If the Qdrant operation succeeds but the subsequent SQLite operation fails, `DefaultGLMRepository` attempts to **roll back** the Qdrant change by deleting the just-upserted point from Qdrant. This is a critical step to prevent orphaned embeddings in Qdrant.
        *   If the Qdrant operation fails, the process usually stops there, and an error is propagated, preventing the SQLite write.
    *   **`BatchStoreKEMs`:** This operation processes a list of KEMs. The `DefaultGLMRepository.batch_store_kems()` method handles this, typically attempting to store each KEM with similar consistency logic. It returns a list of successfully stored KEMs and references to those that failed.

### Retrieving KEMs

1.  **Request Path:** A gRPC `RetrieveKEMs` request is sent to `main.py`.
2.  **Servicer Handling:** The servicer passes the query parameters, page size, and page token to `DefaultGLMRepository.retrieve_kems()`.
3.  **`DefaultGLMRepository` Logic (`retrieve_kems`):**
    *   The repository analyzes the `KEMQuery`.
    *   If a vector search (`embedding_query`) is specified, it queries Qdrant first (applying any metadata or timestamp filters supported by Qdrant's payload indexing).
    *   If filtering by IDs, metadata, or date ranges without a vector query, it primarily queries SQLite.
    *   For vector search results, KEM IDs are retrieved from Qdrant, and then their full data is fetched from SQLite.
    *   The method assembles the KEM protobuf messages and handles pagination.

### Deleting KEMs

1.  **Request Path:** A gRPC `DeleteKEM` request (containing a `kem_id`) is sent to `main.py`.
2.  **Servicer Handling:** The servicer calls `DefaultGLMRepository.delete_kem()` with the `kem_id`.
3.  **`DefaultGLMRepository` Logic (`delete_kem`):**
    *   The repository attempts to delete the KEM from SQLite.
    *   It also attempts to delete the corresponding vector point from Qdrant (if Qdrant is configured).
    *   The operation aims to remove the KEM from both stores.

## Modifying the Code

When making changes to GLM, especially those related to SQLite optimizations or memory handling, consider the following:

### General Guidelines

*   **Identify the Scope:** Determine which part of GLM your changes affect:
    *   gRPC interface or request/response handling: `dcs_memory/services/glm/app/main.py`, relevant `.proto` files.
    *   Core KEM storage orchestration (SQLite + Qdrant): `dcs_memory/services/glm/app/repositories/default_impl.py`.
    *   Direct SQLite database interactions: `dcs_memory/services/glm/app/repositories/sqlite_repo.py` (or actual file name).
    *   Direct Qdrant interactions: `dcs_memory/services/glm/app/repositories/qdrant_repo.py` (or actual file name).
    *   Configuration: `dcs_memory/common/config.py` for Pydantic models, `config.yml` for runtime values.
*   **Read Existing Documentation:** Consult `ARCHITECTURE_EN.md` and `README.md` to understand the broader impact of your changes.

### SQLite Specific Modifications/Optimizations

*   **Target File:** Changes to SQLite table schemas, indexing strategies, query construction, or PRAGMA settings should generally be implemented within the `SqliteKemRepository` class (e.g., in `dcs_memory/services/glm/app/repositories/sqlite_repo.py`).
*   **Parameterized Queries:** Always use parameterized queries (e.g., `cursor.execute("SELECT * FROM kems WHERE id = ?", (kem_id,))`) to prevent SQL injection vulnerabilities.
*   **Performance:**
    *   Analyze query performance using `EXPLAIN QUERY PLAN` if you're writing complex queries.
    *   Ensure appropriate indexes are in place for common query patterns.
    *   Be mindful of the impact of schema changes or new PRAGMA settings on read/write performance.
*   **Concurrency:** Remember that SQLite with WAL mode allows concurrent reads and a single writer. Operations should be efficient to minimize write transaction times. The `SQLITE_BUSY_TIMEOUT` helps manage contention.

### Data Consistency

*   **Dual-Store Integrity:** This is paramount. Any modification to data storage logic in `DefaultGLMRepository` *must* maintain or enhance the consistency between data in SQLite and Qdrant.
*   **Rollback Mechanisms:** If your changes affect the order of operations or introduce new failure points in `store_kem`, `update_kem`, or `batch_store_kems`, ensure that the rollback logic (e.g., deleting from Qdrant if a subsequent SQLite write fails) is correctly adjusted.
*   **Atomicity:** Strive for atomicity where possible. For operations spanning both databases, this is often achieved through careful sequencing and compensating actions.

### Configuration Changes

*   **Pydantic Models:** If new configuration parameters are needed, add them to the appropriate Pydantic model in `dcs_memory/common/config.py` (e.g., `GLMConfig`). Provide clear descriptions and sensible defaults.
*   **`config.yml`:** Document new parameters and provide example usage in the main `config.yml` or developer documentation.
*   **Environment Variables:** Pydantic settings allow overrides via environment variables (e.g., `GLM_NEW_PARAM=value`).

### Logging

*   **Utilize Existing Setup:** The project uses Python's standard `logging` module. Obtain a logger instance at the top of your module: `logger = logging.getLogger(__name__)`.
*   **Informative Messages:** Add clear and informative log messages for new logic, important decision points, and error conditions. Include relevant context (e.g., KEM IDs).
*   **Log Levels:** Use appropriate log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL).
*   **Configuration:** Logging behavior (level, format, output mode like `stdout`, `file`, `json_stdout`) is configured via `BaseServiceConfig` parameters, ultimately set in `config.yml`.

## Verifying Changes (Code Work)

Thorough verification is crucial to ensure your modifications are correct and do not introduce regressions.

1.  **Unit Tests:**
    *   Write unit tests for new functions or classes.
    *   If modifying existing logic, ensure existing unit tests pass or update them accordingly. (Note: The presence and extent of unit tests have not been explicitly verified in this exploration but are a standard best practice).

2.  **Integration Tests:**
    *   **gRPC Client:** The most effective way to test GLM changes is by sending gRPC requests to the service. You can use:
        *   `grpcurl`: A command-line tool for interacting with gRPC services.
        *   Custom Python gRPC clients (similar to how KPS or SWM might interact with GLM, or using the `dcsm_agent_sdk_python`).
    *   **Test Scenarios:**
        *   Verify successful KEM storage, retrieval (by ID, metadata, vector search), updates, and deletions.
        *   Explicitly check that data appears correctly in *both* SQLite and Qdrant.
        *   Test edge cases: empty inputs, invalid data, non-existent KEMs.
        *   **Test error conditions and rollback mechanisms:** For example, simulate a failure during an SQLite write after a Qdrant write to ensure the Qdrant data is rolled back.
        *   Test batch operations and partial failures.

3.  **Health Checks:**
    *   After deploying your changes, always check the GLM service's health endpoint: `/grpc.health.v1.Health/Check`.
    *   The service should report a `SERVING` status. This health check is implemented in `dcs_memory/services/glm/app/main.py` and relies on `DefaultGLMRepository.check_health()` which, in turn, checks SQLite and Qdrant connectivity.

4.  **Logging Analysis:**
    *   Monitor the GLM service logs closely after deploying changes.
    *   Look for any error messages, warnings, or unexpected behavior indicated in the logs.
    *   Ensure your new log messages are appearing as expected and providing useful information.

5.  **Performance Testing (for optimizations):**
    *   If your changes are intended to optimize performance (e.g., SQLite query tuning):
        *   Establish baseline performance metrics before applying the changes.
        *   Conduct load/performance tests after applying changes to measure the impact on request latency, throughput, and resource utilization.
        *   Use profiling tools if necessary to pinpoint bottlenecks.
```
