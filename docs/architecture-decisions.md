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
| [**ADR-10**](#adr-10-interactive-openapi-30--swagger-ui-via-springdoc) | Interactive OpenAPI 3.0 & Swagger UI via Springdoc | **Accepted** | API & Developer Experience |
| [**ADR-11**](#adr-11-sliding-time-windowed-statistics-via-lucene-longpoint-range-queries) | Sliding Time-Windowed Statistics via Lucene `LongPoint` Range Queries | **Accepted** | Vector Storage & Analytics |
| [**ADR-12**](#adr-12-dynamic-external-mcp-client--unified-tool-registry) | Dynamic External MCP Client & Unified Tool Registry | **Accepted** | Interoperability & Agentic Layer |
| [**ADR-13**](#adr-13-cloud-native-kubernetes-packaging-with-helm-3--automated-kind-provisioning) | Cloud-Native Kubernetes Packaging with Helm 3 & Automated Kind Provisioning | **Accepted** | Infrastructure & Packaging |
| [**ADR-14**](#adr-14-llm-observability--langsmith-trace-forwarding-via-chatmodellistener) | LLM Observability & LangSmith Trace Forwarding via `ChatModelListener` | **Accepted** | Observability & LLMOps |

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
* [`src/main/resources/application.yml`](../src/main/resources/application.yml#L7-L9)
* [`src/main/java/com/streamingrag/api/QueryController.java`](../src/main/java/com/streamingrag/api/QueryController.java#L85-L90)

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
* [`pom.xml`](../pom.xml#L58-L73)
* [`src/main/java/com/streamingrag/config/LangChainConfig.java`](../src/main/java/com/streamingrag/config/LangChainConfig.java)
* [`src/main/java/com/streamingrag/agent/SreAnalystAgent.java`](../src/main/java/com/streamingrag/agent/SreAnalystAgent.java)

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
* [`src/main/java/com/streamingrag/config/LangChainConfig.java`](../src/main/java/com/streamingrag/config/LangChainConfig.java#L52-L60)
* [`src/main/java/com/streamingrag/vector/EmbeddingService.java`](../src/main/java/com/streamingrag/vector/EmbeddingService.java#L46-L56)

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
* [`src/main/java/com/streamingrag/config/KafkaConfig.java`](../src/main/java/com/streamingrag/config/KafkaConfig.java#L103-L113)
* [`src/main/java/com/streamingrag/consumer/LogIngestionConsumer.java`](../src/main/java/com/streamingrag/consumer/LogIngestionConsumer.java#L52-L71)

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
* [`src/main/java/com/streamingrag/mcp/McpProtocol.java`](../src/main/java/com/streamingrag/mcp/McpProtocol.java)
* [`src/main/java/com/streamingrag/mcp/McpService.java`](../src/main/java/com/streamingrag/mcp/McpService.java)
* [`src/main/java/com/streamingrag/mcp/McpSseController.java`](../src/main/java/com/streamingrag/mcp/McpSseController.java)

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
* [`src/main/java/com/streamingrag/vector/LuceneEmbeddingStore.java`](../src/main/java/com/streamingrag/vector/LuceneEmbeddingStore.java)
* [`src/main/java/com/streamingrag/config/LangChainConfig.java`](../src/main/java/com/streamingrag/config/LangChainConfig.java#L65-L81)

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
* [`docker-compose.yml`](../docker-compose.yml#L8-L31)
* [`docker-compose.infra.yml`](../docker-compose.infra.yml#L14-L37)

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
* [`src/main/java/com/streamingrag/producer/LogProducerService.java`](../src/main/java/com/streamingrag/producer/LogProducerService.java#L46-L51)
* [`src/main/java/com/streamingrag/model/SystemLog.java`](../src/main/java/com/streamingrag/model/SystemLog.java)

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
* [`Dockerfile`](../Dockerfile#L6-L43)
* [`.github/workflows/ci.yml`](../.github/workflows/ci.yml#L45-L66)

---

## ADR-10: Interactive OpenAPI 3.0 & Swagger UI via Springdoc

### Status
**Accepted**

### Context & Problem Statement
In the Python sister project (`streaming-rag`), FastAPI automatically generates OpenAPI 3.0 specifications and serves an interactive Swagger UI playground at `/docs`. Spring Boot does not expose OpenAPI documentation out of the box, requiring engineers and evaluators to rely solely on external `curl` commands.

### Considered Alternatives
1. **Static Markdown API Documentation Only:** Prone to drift and lacks an interactive browser playground.
2. **Springfox Swagger:** Unmaintained and incompatible with Spring Boot 3 / Jakarta EE 10.
3. **Springdoc OpenAPI (`springdoc-openapi-starter-webmvc-ui` 2.6.0):** Official community standard for Spring Boot 3.x supporting Jakarta validation, Java 21 records, and OpenAPI 3.0 annotations.

### Decision Outcome
We integrated **`springdoc-openapi-starter-webmvc-ui:2.6.0`**, configured `OpenApiConfig`, and annotated controllers and Java 21 DTO records with `@Tag`, `@Operation`, and `@Schema`.

### Decision Rationales & Trade-Offs
* **Interactive Playground:** Exposes `/swagger-ui.html` and `/v3/api-docs` out of the box, matching FastAPI's developer ergonomics.
* **Java 21 Record Introspection:** Automatically reflects Jakarta Bean Validation constraints (`@NotBlank`) and `@Schema` metadata on `QueryRequest`, `QueryResponse`, and `LogSource`.

### Code References
* [`src/main/java/com/streamingrag/config/OpenApiConfig.java`](../src/main/java/com/streamingrag/config/OpenApiConfig.java)
* [`src/main/java/com/streamingrag/api/QueryController.java`](../src/main/java/com/streamingrag/api/QueryController.java)

---

## ADR-11: Sliding Time-Windowed Statistics via Lucene `LongPoint` Range Queries

### Status
**Accepted**

### Context & Problem Statement
Initially, `SreTools.logStats(logLevel, serviceName, minutes)` returned cumulative in-memory counters accumulated since process startup, ignoring the `minutes` time-window parameter. In contrast, the Python sister project filters vector store metadata against a sliding UTC timestamp cutoff (`now - timedelta(minutes=minutes)`).

### Considered Alternatives
1. **Cumulative In-Memory Counters Only:** Fast, but cannot filter by arbitrary sliding time windows (`minutes=5` vs `minutes=60`).
2. **Full Index Scan & String Parsing:** Scanning all stored documents and parsing ISO-8601 strings on every query; $O(N)$ overhead as the index grows.
3. **Lucene `LongPoint` BKD-Tree Numeric Range Index (`timestamp_ms`):** Indexing each log's epoch millisecond timestamp in a Lucene `LongPoint` dimensional index alongside stored metadata, and executing `LongPoint.newRangeQuery("timestamp_ms", cutoff.toEpochMilli(), Long.MAX_VALUE)`.

### Decision Outcome
We enhanced `LuceneEmbeddingStore` to index `FIELD_TIMESTAMP_MS` (`LongPoint` + `StoredField`) and added `findMetadataSince(Instant cutoff, String logLevel, String serviceName)` backed by `LongPoint.newRangeQuery(...)`.

### Decision Rationales & Trade-Offs
* **Sub-Millisecond Range Filtering:** Lucene's BKD-tree (`LongPoint`) executes numeric timestamp range queries in $O(\log N)$ time.
* **Accurate Sliding Window Analytics:** `SreTools.logStats` now computes accurate severity and service distributions strictly within `Instant.now().minus(Duration.ofMinutes(effectiveMinutes))`.

### Code References
* [`src/main/java/com/streamingrag/vector/LuceneEmbeddingStore.java`](../src/main/java/com/streamingrag/vector/LuceneEmbeddingStore.java#L204-L261)
* [`src/main/java/com/streamingrag/agent/SreTools.java`](../src/main/java/com/streamingrag/agent/SreTools.java#L105-L165)

---

## ADR-12: Dynamic External MCP Client & Unified Tool Registry

### Status
**Accepted**

### Context & Problem Statement
While the application acted as an MCP server exposing `vector_search` and `log_stats`, the internal `SreAnalystAgent` could not dynamically discover and invoke tools hosted on external MCP servers (configured via `MCP_SERVER_URL`), unlike the Python project's `MultiServerMCPClient`.

### Considered Alternatives
1. **Static Hardcoded Java `@Tool` Classes Only:** Requires recompiling the application whenever a new external diagnostic tool is added.
2. **Dynamic JSON-RPC 2.0 MCP Client (`McpClientService`) + LangChain4j `ToolSpecification` / `ToolExecutor` Map:** Discovers tools from `MCP_SERVER_URL` via `tools/list`, converts their JSON Schema into LangChain4j `ToolSpecification` instances, and routes executions over `tools/call`, merging them with native `SreTools`.

### Decision Outcome
We implemented **`McpClientService`** and wired `LangChainConfig.sreAnalystAgent` to register the unified map of native `SreTools` and dynamically discovered external MCP tools, with graceful fallback to native tools when `MCP_SERVER_URL` is unconfigured or unreachable.

### Decision Rationales & Trade-Offs
* **Zero-Code Extensibility:** External MCP servers (e.g., PagerDuty, GitHub, Prometheus MCP) can be attached at runtime via `MCP_SERVER_URL`.
* **Resilient Fallback:** Network errors or unavailable external MCP servers never prevent the agent from starting or using native `vectorSearch` and `logStats` tools.

### Code References
* [`src/main/java/com/streamingrag/mcp/McpClientService.java`](../src/main/java/com/streamingrag/mcp/McpClientService.java)
* [`src/main/java/com/streamingrag/config/LangChainConfig.java`](../src/main/java/com/streamingrag/config/LangChainConfig.java#L118-L138)

---

## ADR-13: Cloud-Native Kubernetes Packaging with Helm 3 & Automated Kind Provisioning

### Status
**Accepted**

### Context & Problem Statement
Deploying only the Spring Boot Deployment in Kubernetes leaves evaluators to manually wire Kafka KRaft, Ollama model initialization jobs, persistent volumes, and secrets. We needed a self-contained cloud-native Helm chart and local Kind cluster automation matching the Python sister repository.

### Considered Alternatives
1. **App-Only Kubernetes Manifests:** Incomplete; fails unless external Kafka and Ollama clusters already exist.
2. **Full-Stack Parameterized Helm 3 Chart + `scripts/kind-deploy.sh`:** Packages Kafka KRaft (`kafka.yaml`), Ollama + `ollama-init` hook Job (`ollama.yaml`), PVC-backed Spring Boot app with embedded Lucene (`deployment.yaml`), ConfigMap, and Secret templates.

### Decision Outcome
We upgraded `charts/streaming-rag/` to a full-stack Helm 3 chart and created `scripts/kind-deploy.sh` for single-command local Kubernetes validation on Kind.

### Decision Rationales & Trade-Offs
* **Eliminates ChromaDB Pod in Kubernetes:** Because Apache Lucene and ONNX Runtime run embedded inside the Spring Boot container, the Kubernetes topology requires fewer pods and lower cluster memory than the Python stack.
* **Automated Model Readiness:** The `ollama-init` Helm hook Job and `wait-for-ollama-model` initContainer guarantee `llama3.2:3b` is pulled before traffic is routed.

### Code References
* [`charts/streaming-rag/`](../charts/streaming-rag/Chart.yaml)
* [`scripts/kind-deploy.sh`](../scripts/kind-deploy.sh)

---

## ADR-14: LLM Observability & LangSmith Trace Forwarding via `ChatModelListener`

### Status
**Accepted**

### Context & Problem Statement
Production agentic systems require visibility into LLM prompt/response latencies, token consumption (`inputTokens`, `outputTokens`, `totalTokens`), tool execution requests, and error rates, as well as compatibility with LangSmith tracing (`LANGCHAIN_TRACING_V2=true`).

### Considered Alternatives
1. **Uninstrumented LLM Calls:** Opaque latency bottlenecks and unmonitored token usage.
2. **LangChain4j `ChatModelListener` (`LlmObservabilityListener`) with Async LangSmith Run Exporter:** Hooks directly into `OllamaChatModel` and `OllamaStreamingChatModel` lifecycle events (`onRequest`, `onResponse`, `onError`), recording atomic metrics, structured logs, and non-blocking HTTP trace dispatches to LangSmith's `/runs` API.

### Decision Outcome
We implemented **`LlmObservabilityListener`** implementing `ChatModelListener` and registered it on both `OllamaChatModel` and `OllamaStreamingChatModel`.

### Decision Rationales & Trade-Offs
* **Zero Latency Penalty:** LangSmith trace payloads are dispatched asynchronously via `HttpClient.sendAsync(...)`, never blocking the LLM response path.
* **Environment Variable Parity:** Honors the exact same environment variables (`LANGCHAIN_TRACING_V2`, `LANGCHAIN_API_KEY`, `LANGCHAIN_PROJECT`, `LANGCHAIN_ENDPOINT`) as the Python sister project.

### Code References
* [`src/main/java/com/streamingrag/config/LlmObservabilityListener.java`](../src/main/java/com/streamingrag/config/LlmObservabilityListener.java)
* [`src/main/java/com/streamingrag/config/LangChainConfig.java`](../src/main/java/com/streamingrag/config/LangChainConfig.java#L83-L116)
