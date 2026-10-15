# Architectural Decision Records (ADRs) — Enterprise Streaming RAG in Java

This document records the critical architectural choices made during the development of **Enterprise Streaming RAG in Java**. Each record details the operational context, problem statement, evaluated alternatives, final decision, engineering rationales, trade-offs, and exact code references.

---

## 📋 ADR Index

| ADR ID | Title | Status | Scope |
| :--- | :--- | :--- | :--- |
| [**ADR-01**](#adr-01-java-21--virtual-threads-project-loom) | Java 21 & Virtual Threads (Project Loom) | **Accepted** | Core Runtime |
| [**ADR-02**](#adr-02-langchain4j-over-spring-ai) | LangChain4j over Spring AI | **Accepted** | Agentic Layer |
| [**ADR-03**](#adr-03-in-process-embeddings-via-onnx-runtime) | In-Process Embeddings via ONNX Runtime | **Accepted** | Vector Pipeline |
| [**ADR-04**](#adr-04-manual-kafka-offset-commits-tied-to-vector-confirmation) | Manual Kafka Offset Commits Tied to Vector Confirmation | **Accepted** | Ingestion & Delivery |
| [**ADR-05**](#adr-05-model-context-protocol-mcp-as-tool-interoperability-standard) | Model Context Protocol (MCP) as Tool Interoperability Standard | **Accepted** | Interoperability |
| [**ADR-06**](#adr-06-embedded-apache-lucene-as-in-process-vector-database) | Embedded Apache Lucene as In-Process Vector Database (Replacing ChromaDB) | **Accepted** | Vector Storage |
| [**ADR-07**](#adr-07-kafka-kraft-mode-over-zookeeper) | Kafka KRaft Mode over ZooKeeper | **Accepted** | Ingestion & Streaming |
| [**ADR-08**](#adr-08-kafka-partitioning-key-by-servicename) | Kafka Partitioning Key by `serviceName` | **Accepted** | Ingestion & Ordering |
| [**ADR-09**](#adr-09-single-multi-stage-docker-image-with-distroless-temurin-runtime) | Single Multi-Stage Docker Image with Temurin JRE 21 Runtime | **Accepted** | Infrastructure & Packaging |

---

## ADR-01: Java 21 & Virtual Threads (Project Loom)

### Status
**Accepted**

### Context & Problem Statement
High-throughput event streaming architectures combined with LLM token streaming require handling thousands of concurrent I/O operations (Kafka partition polling, Apache Lucene vector indexing, and Ollama SSE streaming). In traditional thread-per-request models, native OS threads cause high memory overhead (~1 MB per thread stack) and thread starvation under load. In asynchronous reactive frameworks (e.g., Project Reactor), code readability degrades through complex callback chains and stack traces become opaque.

### Considered Alternatives
1. **Platform OS Threads (Default Tomcat Thread Pool):** Limited scalability (~200 concurrent threads before memory and context-switching bottlenecks).
2. **Reactive Programming (Spring WebFlux & Project Reactor):** High throughput, but steep learning curve, viral reactive types (`Mono`/`Flux`), difficult debugging, and impedance mismatches with blocking third-party libraries.
3. **Java 21 Virtual Threads (Project Loom):** Lightweight user-mode threads managed directly by the JVM with negligible memory overhead.

### Decision Outcome
We standardized on **Java 21 LTS** and enabled **Spring Boot Virtual Threads** (`spring.threads.virtual.enabled=true`).

### Decision Rationales & Trade-Offs
* **Simplicity & Readability:** Standard blocking synchronous programming style remains natural and readable while scaling across thousands of lightweight virtual threads.
* **Concurrency Scale:** Near-zero thread overhead allows spawning dedicated virtual threads for long-lived SSE streaming (`Thread.startVirtualThread(...)`).
* **Zero GIL Contention:** Unlike Python's single-threaded event loop, all CPU cores are fully utilized without GIL bottlenecks.
* **Observability:** Native JVM profilers (Async Profiler, JFR) retain clear, chronological thread visibility.

### Code References
* [`src/main/resources/application.yml`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/resources/application.yml#L7-L9) (lines 7–9)
* [`src/main/java/com/streamingrag/api/QueryController.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/api/QueryController.java#L71-L75) (lines 71–75)

---

## ADR-02: LangChain4j over Spring AI

### Status
**Accepted**

### Context & Problem Statement
The project requires an enterprise Java agentic AI framework supporting declarative ReAct agents, tool execution (`@Tool`), in-process embedding models, and embedded vector store integration.

### Considered Alternatives
1. **Spring AI:** Official Spring ecosystem project, but early in its evolution, with limited embedded in-process ONNX model support and immature agentic ReAct loop tooling at the time of development.
2. **Custom LLM Client & Parser:** Building HTTP JSON glue code directly against Ollama; lacks prompt templating, conversational abstractions, and tool-calling machinery.
3. **LangChain4j (0.35.0+):** Mature, production-grade Java AI framework specifically designed for Java enterprise environments.

### Decision Outcome
We adopted **LangChain4j** (`0.35.0+`).

### Decision Rationales & Trade-Offs
* **Native In-Process ONNX Support:** LangChain4j provides first-class support for in-process ONNX embeddings (`langchain4j-embeddings-all-minilm-l6-v2`), completely eliminating the Python sidecar container requirement.
* **Declarative `@AiService`:** Cleanly separates agent prompts, tools, and LLM providers via clean Java interfaces.
* **Pluggable Vector Store Abstraction:** The clean `EmbeddingStore<TextSegment>` abstraction allows plugging in our native Apache Lucene vector store seamlessly.

### Code References
* [`pom.xml`](file:///c:/Users/Utente/Projects/streaming-rag-java/pom.xml#L50-L65) (lines 50–65)
* [`src/main/java/com/streamingrag/config/LangChainConfig.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/config/LangChainConfig.java#L106-L119) (lines 106–119)
* [`src/main/java/com/streamingrag/agent/SreAnalystAgent.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/agent/SreAnalystAgent.java)

---

## ADR-03: In-Process Embeddings via ONNX Runtime

### Status
**Accepted**

### Context & Problem Statement
The Python counterpart uses SentenceTransformers (`all-MiniLM-L6-v2`) to generate 384-dimensional dense vectors. In enterprise Java systems, delegating embeddings to an external Python microservice introduces network latency, HTTP serialization overhead, and another container failure domain.

### Considered Alternatives
1. **External Python Embedding Microservice:** Standard FastAPI sidecar; adds network serialization latency, Docker memory overhead, and inter-service failure points.
2. **Cloud Embedding APIs (OpenAI `text-embedding-3-small`):** High quality, but requires remote HTTP round-trips for every log entry, leaks internal infrastructure telemetry, and incurs usage billing.
3. **In-Process ONNX Runtime (`all-MiniLM-L6-v2`):** Executes the model directly inside the JVM process via native C++ JNI bindings.

### Decision Outcome
We adopted **in-process `all-MiniLM-L6-v2`** using **LangChain4j ONNX Runtime bindings**.

### Decision Rationales & Trade-Offs
* **Zero Network Latency:** Vectorization occurs in-memory within the Kafka consumer pipeline (~5–15 ms per record).
* **1:1 Vector Space Parity:** Produces identical 384-dimensional embeddings compatible with the Python SentenceTransformers model.
* **Security & Privacy:** Sensitive production logs never leave the JVM process boundary.
* **Zero External Dependencies:** No external Python runtime or model microservice needed.

### Code References
* [`src/main/java/com/streamingrag/config/LangChainConfig.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/config/LangChainConfig.java#L46-L54) (lines 46–54)
* [`src/main/java/com/streamingrag/vector/EmbeddingService.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/vector/EmbeddingService.java#L46-L56) (lines 46–56)

---

## ADR-04: Manual Kafka Offset Commits Tied to Vector Confirmation

### Status
**Accepted**

### Context & Problem Statement
Distributed log ingestion requires strictly reliable delivery. If a consumer crashes after acknowledging Kafka but before persisting the embedding in Apache Lucene, log evidence would be permanently lost.

### Considered Alternatives
1. **Auto-Commit (`enable.auto.commit = true`):** Kafka commits the latest polled offset periodically; risks losing messages if the application crashes during vector calculation.
2. **Asynchronous Commit:** Fast, but commit order is not guaranteed and failure recovery is non-deterministic.
3. **Manual Synchronous Commit (`AckMode.MANUAL_IMMEDIATE`):** Explicitly commits the Kafka record offset strictly after the vector database write succeeds.

### Decision Outcome
We configured Spring Kafka with **`AckMode.MANUAL_IMMEDIATE`**, committing consumer offsets **only after** Apache Lucene confirms vector indexing.

### Decision Rationales & Trade-Offs
* **At-Least-Once Delivery Guarantee:** Guarantees zero log loss during consumer restarts or transient failures.
* **Dead Letter Recovery:** If indexing fails permanently, Spring Kafka's `DefaultErrorHandler` retries twice before routing the record to `system-logs-dlq`.

### Code References
* [`src/main/java/com/streamingrag/config/KafkaConfig.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/config/KafkaConfig.java#L103-L113) (lines 103–113)
* [`src/main/java/com/streamingrag/consumer/LogIngestionConsumer.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/consumer/LogIngestionConsumer.java#L52-L71) (lines 52–71)

---

## ADR-05: Model Context Protocol (MCP) as Tool Interoperability Standard

### Status
**Accepted**

### Context & Problem Statement
AI developer environments like Cursor IDE and Claude Desktop require a standardized, secure protocol to discover and execute live diagnostic tools without hardcoding custom REST integrations.

### Considered Alternatives
1. **REST Endpoints Only:** Requires external clients to build custom integration and parsing code.
2. **Proprietary Plugins:** Platform-specific plugins for each IDE/agent environment.
3. **Model Context Protocol (MCP):** Open protocol standard (JSON-RPC 2.0 over SSE and standard I/O) for tool discovery, execution, and contextual grounding.

### Decision Outcome
We implemented the **Model Context Protocol (MCP)** specification via native Spring Boot controllers and runners, exposing `vector_search` and `log_stats`.

### Decision Rationales & Trade-Offs
* **Native IDE Integration:** Engineers can query live system logs directly from Cursor IDE (`/sse`) or Claude Desktop.
* **Dual Transports:** Supports HTTP Server-Sent Events (`/sse`, `/messages`) for networked deployment and `stdio` (`McpServerRunner`) for desktop process execution.
* **Shared Tool Implementation:** Both internal `@AiService` and external MCP hosts query the same underlying `SreTools` and Lucene vector store.

### Code References
* [`src/main/java/com/streamingrag/mcp/McpProtocol.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/mcp/McpProtocol.java)
* [`src/main/java/com/streamingrag/mcp/McpService.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/mcp/McpService.java)
* [`src/main/java/com/streamingrag/mcp/McpSseController.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/mcp/McpSseController.java)

---

## ADR-06: Embedded Apache Lucene as In-Process Vector Database (Replacing ChromaDB)

### Status
**Accepted**

### Context & Problem Statement
In the Python sister project (`streaming-rag`), ChromaDB is written in Python and runs embedded in-process directly within the Python application without needing external servers. In Java, however, ChromaDB cannot run embedded; it requires running a separate external ChromaDB Docker container communicating over HTTP on port 8000. In local development and resource-constrained environments (e.g. personal laptops with 8–16 GB RAM), running multiple heavy Docker containers (Kafka, ChromaDB, Ollama, JVM) causes WSL2 memory exhaustion and severe disk swap thrashing.

### Considered Alternatives
1. **External ChromaDB HTTP Container:** Requires running a dedicated Python container alongside Java; introduces HTTP serialization overhead and container memory pressure.
2. **External Enterprise Vector DB (Qdrant, Milvus, Weaviate):** High operational overhead for single-developer and edge environments.
3. **Apache Lucene 9.12.0 Embedded:** Native JVM search engine with built-in HNSW k-NN vector indexing (`KnnFloatVectorField` / `KnnFloatVectorQuery`) with cosine similarity.

### Decision Outcome
We replaced ChromaDB with **Apache Lucene 9.12.0** as an embedded in-process vector database implementing LangChain4j's `EmbeddingStore<TextSegment>`.

### Decision Rationales & Trade-Offs
* **100% In-Process Execution:** The vector database lives entirely inside the JVM alongside the Kafka consumer and LLM agent, eliminating all external HTTP hops and latency.
* **Zero ChromaDB Container Overhead:** The ChromaDB container is completely eliminated from all Docker Compose profiles. The infrastructure profile requires only Kafka (< 150 MB RAM total).
* **Near-Real-Time Search Visibility:** Searchers use Lucene's `SearcherManager` for near-real-time (NRT) reader refreshes upon document indexing.
* **Dual-Mode Backend:** Supports persistent disk storage (`FSDirectory`) for production/Docker and in-memory storage (`ByteBuffersDirectory`) for rapid automated testing.

### Code References
* [`src/main/java/com/streamingrag/vector/LuceneEmbeddingStore.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/vector/LuceneEmbeddingStore.java)
* [`src/main/java/com/streamingrag/config/LangChainConfig.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/config/LangChainConfig.java#L61-L75) (lines 61–75)

---

## ADR-07: Kafka KRaft Mode over ZooKeeper

### Status
**Accepted**

### Context & Problem Statement
Microservice architectures generate continuous, bursty streams of logs and diagnostic traces. We needed an event streaming broker to buffer and decouple log emission from vector indexing. Traditionally, Apache Kafka required a separate Apache ZooKeeper ensemble to manage broker metadata, topic configurations, and partition leader elections. Running ZooKeeper alongside Kafka in local developer environments adds significant memory overhead and startup complexity.

### Considered Alternatives
1. **Kafka with ZooKeeper ensemble:** Traditional multi-container setup (`zookeeper` + `kafka`).
2. **RabbitMQ / Redis Streams:** Lightweight message brokers, but lack Kafka's robust replayability, distributed partition log model, and ecosystem-standard consumer group semantics.
3. **Kafka in KRaft Mode (KIP-500):** Modern single-node Kafka operating without ZooKeeper, utilizing Raft metadata quorum internally.

### Decision Outcome
We adopted **Kafka KRaft Mode** using `apache/kafka:3.7.0` with combined broker and controller roles (`KAFKA_PROCESS_ROLES=broker,controller`).

### Decision Rationales & Trade-Offs
* **Reduced Memory Footprint:** Eliminates the ZooKeeper JVM container entirely, saving ~1 GB of host RAM in Docker Desktop.
* **Faster Recovery & Zero Sync Lag:** Metadata changes are committed to an internal Raft partition rather than synchronized asynchronously across an external ZooKeeper cluster.
* **Modern Standards:** Prepares the architecture for modern production Kafka topologies (ZooKeeper has been officially deprecated by the Apache Kafka project).

### Code References
* [`docker-compose.yml`](file:///c:/Users/Utente/Projects/streaming-rag-java/docker-compose.yml#L8-L31) (lines 8–31)
* [`docker-compose.infra.yml`](file:///c:/Users/Utente/Projects/streaming-rag-java/docker-compose.infra.yml#L14-L37) (lines 14–37)

---

## ADR-08: Kafka Partitioning Key by `serviceName`

### Status
**Accepted**

### Context & Problem Statement
Kafka topics are divided into partitions for horizontal scale. However, Kafka only guarantees **strict message ordering within a single partition**, not across different partitions. In site reliability engineering (SRE) incident investigation, diagnosing root cause requires preserving the chronological sequence of operations emitted by a specific service (e.g., `user-auth` logs *CredentialCheck $\rightarrow$ RateLimitExceeded $\rightarrow$ 401 Unauthorized*).

### Considered Alternatives
1. **Null Partition Key (Round-Robin):** Distributes messages evenly across all partitions, losing service-level ordering.
2. **Keying by `eventId` (Random UUID):** Uniformly hashes random UUIDs across partitions, losing ordering.
3. **Keying by `serviceName`:** Uses `serviceName` as the Kafka message key.

### Decision Outcome
We chose to **partition explicitly by `serviceName`**.

### Decision Rationales & Trade-Offs
* **Chronological Causality:** Kafka computes `hash(serviceName) % num_partitions`. Consequently, all logs generated by a specific microservice land on the exact same partition in strict chronological order.
* **Consumer Group Scalability:** Multiple consumer replicas within the same consumer group can process different services in parallel without race conditions or timestamp inversions.
* **Trade-Off (Partition Hotspotting):** If one service logs orders of magnitude more events than others, its assigned partition receives disproportionate traffic. Mitigated in production by partition balancing and log sampling.

### Code References
* [`src/main/java/com/streamingrag/producer/LogProducerService.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/producer/LogProducerService.java#L46-L51) (lines 46–51)
* [`src/main/java/com/streamingrag/model/SystemLog.java`](file:///c:/Users/Utente/Projects/streaming-rag-java/src/main/java/com/streamingrag/model/SystemLog.java)

---

## ADR-09: Single Multi-Stage Docker Image with Temurin JRE 21 Runtime

### Status
**Accepted**

### Context & Problem Statement
Enterprise container images must be minimal, secure, and fast to build. Bundling a full JDK in the production image inflates container size by several hundred megabytes and introduces unnecessary compilers and development tools into production.

### Considered Alternatives
1. **Single-Stage Build with JDK 21:** Simple to write, but results in a bloated image (> 800 MB) containing build tools and compilers.
2. **Multi-Stage Build with Temurin JRE 21:** Builder stage compiles the fat JAR using Maven; runtime stage runs the JAR inside a lightweight JRE with a non-root user.

### Decision Outcome
We designed a **multi-stage `Dockerfile`** using `maven:3.9.9-eclipse-temurin-21` for building and `eclipse-temurin:21-jre` for the runtime stage, running as a non-privileged user (`appuser:appgroup`).

### Decision Rationales & Trade-Offs
* **Minimal Image Size:** The builder stage is discarded; runtime contains only the JRE and compiled application JAR.
* **Container Security:** Runs as non-root `appuser:appgroup` with optimal container JVM flags (`-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0`).
* **Fast Layer Caching:** `pom.xml` dependencies are fetched offline prior to source compilation, accelerating incremental builds.

### Code References
* [`Dockerfile`](file:///c:/Users/Utente/Projects/streaming-rag-java/Dockerfile#L6-L43) (lines 6–43)
* [`.github/workflows/ci.yml`](file:///c:/Users/Utente/Projects/streaming-rag-java/.github/workflows/ci.yml#L45-L66) (lines 45–66)
