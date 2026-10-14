# Architectural Decision Records (ADRs)

This document records the critical architectural choices made during the development of **Enterprise Streaming RAG in Java**.

---

## ADR-1: Java 21 & Virtual Threads (Project Loom)

* **Status:** Accepted
* **Context:**
  High-throughput event streaming architectures combined with LLM token streaming require handling thousands of concurrent I/O operations (Kafka partition polling, Apache Lucene vector indexing, and Ollama SSE streaming). In traditional thread-per-request models, native OS threads cause high memory overhead (1MB per thread stack) and thread starvation under load. In asynchronous reactive frameworks (e.g., Project Reactor), code readability degrades through complex callback chains and stack traces become opaque.
* **Decision:**
  Standardize on **Java 21 LTS** and enable **Spring Boot Virtual Threads** (`spring.threads.virtual.enabled=true`).
* **Consequences:**
  - Standard blocking synchronous programming style remains natural and readable while scaling across thousands of lightweight virtual threads.
  - No GIL (Global Interpreter Lock) contention, unlike Python architectures.
  - Native JVM profilers (Async Profiler, JFR) retain full thread visibility.

---

## ADR-2: LangChain4j vs. Spring AI

* **Status:** Accepted
* **Context:**
  The project requires an enterprise Java agentic AI framework supporting declarative ReAct agents, tool execution (`@Tool`), in-process embedding models, and embedded vector store integration.
* **Decision:**
  Adopt **LangChain4j** (`0.35.0+`) instead of Spring AI.
* **Consequences:**
  - LangChain4j provides first-class support for in-process ONNX embeddings (`langchain4j-embeddings-all-minilm-l6-v2`), completely eliminating the Python sidecar container requirement.
  - Declarative `@AiService` interfaces cleanly separate agent prompts, tools, and LLM providers.
  - Clean `EmbeddingStore<TextSegment>` abstraction allows plugging in our native Apache Lucene vector store seamlessly.

---

## ADR-3: In-Process Embeddings via ONNX Runtime

* **Status:** Accepted
* **Context:**
  The Python counterpart uses SentenceTransformers (`all-MiniLM-L6-v2`) to generate 384-dimensional dense vectors. In enterprise Java systems, delegating embeddings to an external Python microservice introduces network latency, HTTP serialization overhead, and another container failure domain.
* **Decision:**
  Execute the `all-MiniLM-L6-v2` embedding model directly inside the JVM using **LangChain4j ONNX Runtime bindings**.
* **Consequences:**
  - Zero network hops: vectorization occurs in memory within the Kafka consumer pipeline.
  - 1:1 vector space compatibility with the Python `all-MiniLM-L6-v2` embeddings.
  - Reduced operational footprint for Kubernetes deployments.

---

## ADR-4: Manual Kafka Offset Commits Tied to Vector Confirmation

* **Status:** Accepted
* **Context:**
  Distributed log ingestion requires strictly reliable delivery. If a consumer crashes after acknowledging Kafka but before persisting the embedding in Apache Lucene, log evidence would be permanently lost.
* **Decision:**
  Configure Spring Kafka with `AckMode.MANUAL_IMMEDIATE` and commit consumer offsets **only after** Apache Lucene confirms vector storage.
* **Consequences:**
  - Guarantees *at-least-once* ingestion semantics.
  - Transient write failures trigger exponential retry and Dead Letter Queue (DLQ) routing rather than data loss.

---

## ADR-5: Model Context Protocol (MCP) as Tool Interoperability Standard

* **Status:** Accepted
* **Context:**
  AI developer environments like Cursor IDE and Claude Desktop require a standardized, secure protocol to discover and execute live diagnostic tools without hardcoding custom REST integrations.
* **Decision:**
  Implement the **Model Context Protocol (MCP)** specification (JSON-RPC 2.0 over SSE and standard I/O) exposing `vector_search` and `log_stats`.
* **Consequences:**
  - Seamless plug-and-play integration with Cursor IDE (`/sse`) and Claude Desktop.
  - Decouples tool execution from the agent framework, allowing both the internal `@AiService` and external MCP hosts to query the same live log store.

---

## ADR-6: Embedded Apache Lucene as In-Process Vector Database (Replacing ChromaDB)

* **Status:** Accepted
* **Context:**
  In the Python sister project (`streaming-rag`), ChromaDB is written in Python and runs embedded in-process directly within the Python application without needing external servers. In Java, however, ChromaDB cannot run embedded; it requires running a separate external ChromaDB Docker container communicating over HTTP on port 8000. In local development and resource-constrained environments (e.g. personal laptops with 8–16 GB RAM), running multiple heavy Docker containers (Kafka, ChromaDB, Ollama, JVM) causes WSL2 memory exhaustion and severe disk swap thrashing.
* **Decision:**
  Replace ChromaDB with **Apache Lucene 9.12.0** as an embedded in-process vector database implementing LangChain4j's `EmbeddingStore<TextSegment>`. Utilize Lucene's native HNSW graph index structures (`KnnFloatVectorField` and `KnnFloatVectorQuery`) with cosine similarity (`VectorSimilarityFunction.COSINE`).
* **Consequences:**
  - **100% in-process execution:** The vector database lives entirely inside the JVM alongside the Kafka consumer and LLM agent, eliminating all external HTTP hops and latency.
  - **Zero ChromaDB container overhead:** The ChromaDB container (`streaming-rag-chroma`) is completely removed from all Docker Compose profiles. Infrastructure profile now requires only Kafka (< 150 MB RAM total).
  - **Near-real-time search visibility:** Searchers use Lucene's `SearcherManager` for near-real-time (NRT) reader refreshes upon document indexing.
  - **Flexible storage backend:** Supports persistent disk storage (`FSDirectory`) for production/Docker and in-memory storage (`ByteBuffersDirectory`) for rapid automated testing.
