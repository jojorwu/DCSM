# DCSM: Dynamic Contextualized Shared Memory

**An Intelligent Memory System for Modern AI Agents and Large Language Models (LLMs)**

## Introduction

Dynamic Contextualized Shared Memory (DCSM) is an advanced knowledge management system designed for efficient operation in complex, multi-agent AI systems and for augmenting the capabilities of LLMs. DCSM evolves ideas from approaches like Cache-Augmented Generation (CAG) and Retrieval-Augmented Generation (RAG), focusing on dynamic, distributed, and deeply contextualized access to information.

The primary goal of the system is to provide agents with relevant knowledge precisely when needed, while optimizing computational resource usage and fostering collaborative knowledge accumulation and utilization within a group of agents.

## Core Principles

The DCSM system is based on the following key principles:

*   **Contextualized Memory Units (KEMs)**: Knowledge within the system is not stored as monolithic blocks but is represented as semantically coherent "Contextualized Memory Units" (KEMs). Each KEM is not just a data fragment but a structured object containing the content itself and rich metadata (e.g., topic, source, timestamps, access rights, vector embeddings). This approach enables granular and intelligent knowledge management.

*   **Hierarchical Memory Structure**: To achieve an optimal balance between access speed and storage volume, DCSM employs a multi-level memory architecture:
    *   **GLM (Global/Long-Term Memory)**: The primary, persistent storage for all KEMs in the system. It ensures reliability and long-term knowledge preservation.
    *   **SWM (Shared Working Memory)**: A high-performance, active caching layer. SWM stores "hot" (frequently used or recently requested) KEMs that agents interact with directly.
    *   **LAM (Local Agent Memory)**: A short-term cache on each agent's side, implemented within the Agent SDK. It stores KEMs most relevant to a specific agent, minimizing latency.

*   **Dynamic Management and Knowledge Sharing**:
    *   SWM dynamically manages its content by pre-loading necessary KEMs from GLM and evicting less current ones based on an LRU (Least Recently Used) strategy.
    *   The system supports a Pub/Sub mechanism via SWM, allowing agents to subscribe to event types or KEM changes they are interested in, and to publish new or updated KEMs, making them available to other system participants.

## System Architecture

DCSM is implemented as a set of interacting microservices communicating via gRPC.

*   **GLM (Global Long-Term Memory Service)**:
    *   **Purpose**: Persistent storage for all KEMs.
    *   **Technologies**: Uses SQLite for storing KEM metadata and main content, and Qdrant (a vector database) for storing embeddings and performing semantic searches.

*   **KPS (Knowledge Processor Service)**:
    *   **Purpose**: The entry point for new knowledge into the system.
    *   **Functions**: Accepts raw data (e.g., text), processes it, generates vector embeddings using `sentence-transformers` models, and forms KEMs for subsequent storage in GLM.

*   **SWM (Shared Working Memory Service)**:
    *   **Purpose**: An active caching layer and a hub for knowledge exchange among agents.
    *   **Functions**: Caches KEMs, manages their lifecycle within the cache (LRU eviction with indexing), provides a Pub/Sub mechanism for memory events, and can load data from/persist data to GLM. It also supports distributed locks and counters.

*   **Agent SDK (Python)**:
    *   **Purpose**: Provides a convenient Python interface for agents to interact with DCSM services.
    *   **Functions**: Includes clients for GLM and SWM, implements LAM (Local Agent Memory) with caching and indexing, and simplifies the integration of agent logic with the memory system.

**Simplified Interaction Diagram:**
```
+-------------------+     +---------------------+     +-------------------+
|      Agents       |<--->|     Agent SDK       |<--->|        SWM        |
|  (KEM Consumers)  |     | (LAM, S/G Clients)  |     |  (Cache, Mem Bus) |
+-------------------+     +---------------------+     +-------------------+
                                                        |      ^ (Load/
                                                        |        Persist)
                                                        v      |
                                        +---------------------+
                                        |         GLM         |
                                        | (Qdrant, SQLite)  |
                                        +---------------------+
                                                  ^
                                                  | (KEM Persistence)
                                                  |
                                        +---------------------+
                                        |         KPS         |
                                        | (Processing, Embed.)|
                                        +---------------------+
```
For a more detailed description of the architecture, data formats, and component interactions, please refer to the [dcs_memory/ARCHITECTURE_EN.md](dcs_memory/ARCHITECTURE_EN.md) document.

## Key Advantages

*   **Knowledge Contextualization**: By storing KEMs with rich metadata and vector embeddings, the system enables deep semantic search and provides agents with information precisely tailored to their current context.
*   **Multi-Level Caching**: The memory hierarchy (GLM, SWM, LAM) ensures an optimal balance between fast access to "hot" data and the capacity to store large volumes of information in long-term memory.
*   **Flexibility and Scalability**: The microservice architecture and use of gRPC for component interaction allow the system to be flexible in deployment and to scale individual parts as needed.
*   **Dynamism and Adaptability**: SWM and LAM dynamically adapt their content to the current needs of agents, using LRU strategies and (in SWM) a Pub/Sub mechanism for timely updates with relevant information.
*   **Efficient Search**: Integration with the Qdrant vector database in GLM provides powerful capabilities for semantic search and finding conceptually similar KEMs. `IndexedLRUCache` in SWM and LAM also speeds up metadata-based searches within caches.
*   **Support for Collaborative Work**: SWM acts as a shared workspace where agents can publish new knowledge and subscribe to updates from other agents, fostering synergy in multi-agent systems.
*   **Coordination Primitives**: SWM offers distributed locks and counters, facilitating coordination and synchronization tasks among multiple agents.

## Getting Started

To run the DCSM system locally, you will need Docker and docker-compose.

1.  Clone the repository:
    ```bash
    # Replace <repo_url> with the actual URL of your repository
    git clone <repo_url>
    cd <repository_directory_name>
    ```
2.  Start all services using docker-compose:
    ```bash
    docker-compose up --build
    ```
    This command will build Docker images for all services (if not already built) and start them.

After successful startup, the following services will be available:
*   **Qdrant**: Vector DB (gRPC port 6333, HTTP 6334)
*   **GLM Service**: gRPC on port 50051
*   **KPS Service**: gRPC on port 50052
*   **SWM Service**: gRPC on port 50053

You can start interacting with the system using the [Python Agent SDK](dcsm_agent_sdk_python/README.md) or any other gRPC client. Examples of SDK usage can be found in `dcsm_agent_sdk_python/example.py`.

## Project Status and Future Directions

The DCSM system is under active development. The current implementation includes all major described components and core functionality.

**Key directions for future development:**
*   Full-fledged Pub/Sub mechanism in SWM using `asyncio` or integration with message brokers.
*   Development of more sophisticated caching and eviction policies in SWM.
*   Expansion of filtering and querying capabilities in SWM.
*   Enhancement of security aspects: authentication and authorization.
*   Comprehensive integration and load testing.
*   Improvement of fault tolerance and error handling mechanisms.

We welcome discussion and potential contributions to the project!

---
*(Optionally: Sections like "Contributing" and "License" can be added here if applicable to your project.)*
