# DCSM: Dynamic Contextualized Shared Memory

**An Intelligent Memory System for Modern AI Agents and Large Language Models (LLMs)**

> **Note:** DCSM is currently under active development. While the core components are functional, APIs may evolve, and features are continuously being refined. Users should anticipate potential changes and are encouraged to check back for updates or contribute to the development.

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

## Integrating DCSM with AI Agents and LLMs

DCSM is designed to be a versatile memory backbone for various AI applications, particularly those involving autonomous agents or Large Language Models (LLMs). Here are some conceptual ways to integrate DCSM:

*   **Retrieval Augmented Generation (RAG) for LLMs**:
    *   **How it works**: Before an LLM generates a response, an agent queries DCSM (specifically GLM via SWM or the Agent SDK) using semantic search (vector search on embeddings) or metadata filters. Relevant KEMs are retrieved, and their content is injected into the LLM's prompt as context.
    *   **Benefits**: This approach grounds LLM responses in factual, up-to-date, and verifiable information from DCSM. It allows for customization of the knowledge sources an LLM uses, leading to more accurate, relevant, and less hallucinatory outputs.
    *   **Challenges Addressed**: Combats LLM knowledge cutoffs (by providing current information), reduces the tendency for LLMs to "hallucinate" or invent facts, and helps overcome the inherent context window limitations of LLMs by supplying targeted, external knowledge as needed.
    *   **Role of KPS**: The KPS service is key for populating GLM with processable KEMs, by converting raw documents and data into embedded, queryable knowledge units.

*   **Persistent Memory for Agents**:
    *   **How it works**: Agents store their observations, experiences, learned knowledge, conversation histories, or intermediate results of complex tasks as KEMs within DCSM. These KEMs can be structured with rich metadata (e.g., task ID, observation type, timestamps, source agent) for precise retrieval.
    *   **Benefits**: DCSM endows agents with true long-term memory, enabling them to learn from past interactions, maintain continuity in dialogues or tasks across multiple sessions or restarts, and personalize their behavior over time. The hierarchical memory (LAM/SWM/GLM) ensures efficient access.
    *   **Challenges Addressed**: Solves the "amnesia" problem common in stateless agents or those with limited short-term memory. It facilitates longitudinal learning, adaptation to user preferences, and the execution of complex, multi-step tasks that require recall of prior information or states.

*   **Collaborative Knowledge Building in Multi-Agent Systems**:
    *   **How it works**: Individual agents or specialized KPS instances can publish new KEMs (representing discoveries, processed information, or insights) to SWM. Other agents can subscribe to relevant topics or KEM changes in SWM, allowing them to dynamically receive and incorporate this new knowledge.
    *   **Benefits**: DCSM fosters a shared cognitive environment where multiple agents can contribute to and draw from a common pool of knowledge. This accelerates collective learning, enables agent specialization (where agents contribute unique expertise), and can lead to emergent system-level intelligence and improved overall task performance.
    *   **Challenges Addressed**: Breaks down information silos that can exist between independently operating agents. It reduces redundant knowledge discovery efforts (if one agent learns something, others can benefit) and facilitates more sophisticated, coordinated behaviors based on shared, evolving understanding.

*   **Context-Aware Agent Behavior**:
    *   **How it works**: Agents query DCSM (LAM for immediate needs, SWM for shared operational context, GLM for broader knowledge) for KEMs relevant to their current task, environment, or interaction.
    *   **Benefits**: This allows agents to dynamically adapt their behavior, making more informed and relevant decisions. Instead of relying solely on pre-programmed responses, agents can access and utilize the most pertinent information from the vast knowledge stored in DCSM, leading to more intelligent and flexible actions.
    *   **Challenges Addressed**: Enables agents to operate effectively in dynamic or complex environments where pre-existing knowledge or rules may be insufficient. It allows agents to handle a wider variety of situations and user queries by retrieving specific context on-the-fly, rather than attempting to encode all possible scenarios internally.

*   **Task-Specific Knowledge Bases**:
    *   **How it works**: KEMs within GLM can be organized using metadata tags (e.g., `project_id`, `domain`, `user_id`, `access_level`). Agents can then scope their queries to these specific tags, effectively creating and interacting with logical, task-specific knowledge bases.
    *   **Benefits**: This improves search efficiency by narrowing the search space. It enhances knowledge organization, making it easier to manage diverse information sets. It also allows agents to fluidly switch contexts by targeting different KEM subsets, and can be a basis for implementing data segregation or access control in multi-user or multi-domain applications.
    *   **Challenges Addressed**: Manages information overload in large-scale systems by allowing agents to focus their attention on relevant data subsets. It simplifies knowledge management and maintenance and supports scenarios where agents might need to operate with different sets of knowledge for different tasks or users.

The Python Agent SDK (`dcsm_agent_sdk_python`) provides the primary tools for agents to interact with SWM and GLM, simplifying these integration patterns. It includes functionalities for storing, retrieving, and querying KEMs, as well as interacting with SWM's Pub/Sub and coordination features.

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

### System Configuration

DCSM services are configured through a central YAML file, typically named `config.yml` at the root of the project. This file is mounted into each service container at `/app/config.yml`. Services look for this file based on the `DCSM_CONFIG_FILE` environment variable, which defaults to `config.yml` if not set (but is set to `/app/config.yml` in the provided `docker-compose.yml`).

The `config.yml` allows for defining shared settings and service-specific configurations for GLM, KPS, and SWM. An example `config.yml` is provided in the repository root.

**Configuration Precedence:**
The system loads configurations with the following priority (highest to lowest):
1.  Values passed directly to configuration model constructors (programmatic override).
2.  Environment variables (e.g., `GLM_QDRANT_HOST=my.qdrant.host`). Service-specific variables are prefixed (e.g., `GLM_`, `KPS_`, `SWM_`).
3.  Values from a `.env` file in the service's working directory (if present).
4.  Values from the central `config.yml` file.
5.  Default values defined in the Pydantic configuration models.

This means environment variables can always override settings in `config.yml`.

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
