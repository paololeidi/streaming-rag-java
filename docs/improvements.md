# Engineering Roadmap & Production Improvements

This document outlines the detailed technical roadmap designed to transition the **Enterprise Streaming RAG in Java** project from a functional proof-of-concept into a production-grade, enterprise-ready system. It mirrors the strategic improvements established in the Python sister project ([`streaming-rag`](https://github.com/paololeidi/streaming-rag)), tailored to the enterprise Java 21, Spring Boot, and JVM cloud-native ecosystem.

---

## 🔄 Cross-Repository Parity & Alignment Backlog (from Python Sister Project)

The following architectural and functional capabilities from the Python implementation ([`streaming-rag`](https://github.com/paololeidi/streaming-rag)) have been implemented for full cross-repository parity:

- [x] **J0.1 Interactive OpenAPI / Swagger UI:**
  - Integrated `org.springdoc:springdoc-openapi-starter-webmvc-ui:2.6.0` in `pom.xml` and `OpenApiConfig.java`.
  - Annotated `QueryController`, `HealthController`, `QueryRequest`, `QueryResponse`, and `LogSource` with `@Tag`, `@Operation`, and `@Schema`.
  - Exposed interactive Swagger UI at `/swagger-ui.html` and OpenAPI 3.0 JSON specification at `/v3/api-docs`.
- [x] **J0.2 Sliding Time Window Filtering in `logStats`:**
  - Indexed `LongPoint` and `StoredField` (`timestamp_ms`) in `LuceneEmbeddingStore` and exposed `findMetadataSince(Instant cutoff, String logLevel, String serviceName)` via `LongPoint.newRangeQuery(...)`.
  - Updated `SreTools.logStats()` to compute a sliding UTC timestamp cutoff (`Instant.now().minus(Duration.ofMinutes(effectiveMinutes))`) and aggregate severity/service distributions over the requested window (with fallback to consumer counters when the vector store is empty in isolated mocks).
- [x] **J0.3 Dynamic MCP Client Capabilities:**
  - Implemented `McpClientService` configured via `app.mcp.server-url` (`MCP_SERVER_URL`).
  - Dynamically discovers external MCP tools via JSON-RPC 2.0 `tools/list`, maps them to LangChain4j `ToolSpecification` + `ToolExecutor` instances, and executes them via `tools/call` alongside native `SreTools` with graceful fallback when unconfigured or unreachable.
- [x] **J0.4 Kubernetes Kind Deployment Script & Helpers:**
  - Upgraded `charts/streaming-rag/` with parameterized Kafka KRaft (`kafka.yaml`), Ollama + `ollama-init` model-pull hook (`ollama.yaml`), LangSmith secret management (`secret.yaml`), `_helpers.tpl`, `NOTES.txt`, `.helmignore`, and `README.md`.
  - Added `scripts/kind-deploy.sh` for single-command Kind cluster creation, Docker image loading, Helm linting, and rollout verification.
- [x] **J0.5 Observability & LLM Tracing (LangSmith / OpenTelemetry):**
  - Implemented `LlmObservabilityListener` (`ChatModelListener`) wired into `OllamaChatModel` and `OllamaStreamingChatModel`.
  - Captures structured request/response/error telemetry (latency ms, input/output/total token counts, tool call counts) and asynchronously exports run traces to LangSmith (`/runs`) when `LANGCHAIN_TRACING_V2=true` and `LANGCHAIN_API_KEY` are configured.

---

## 🎯 Strategic Priorities Overview

```
Phase 1: Reliability, Data Integrity & Contract Remediation (Idempotency, Durability, API/Tool Contracts)
   │
   ▼
Phase 2: Software Hardening (Tests, CI/CD, Micro-batching, Concurrency)
   │
   ▼
Phase 3: Advanced Agent Capabilities (Memory, Structured Tools, Multi-MCP)
   │
   ▼
Phase 4: LLMOps & Evaluation (Golden Dataset, Ragas/DeepEval, Guardrails)
   │
   ▼
Phase 5: Streaming & Vector Engine Optimization (Hybrid Search, TTL, Chunking)
   │
   ▼
Phase 6: Interactive Demo & Showcase (Web UI, Live Incident Demo Script)
```

---

## 🛡️ Phase 1: Reliability, Data Integrity & Contract Remediation

**Goal:** Prioritize end-to-end data integrity, crash durability, and strict API/tool contract enforcement so every advertised reliability guarantee is backed by deterministic verification.

- [ ] **1.1 Idempotent Ingestion & Event Identity Guarantees:**
  - *Current limitation:* `VectorStoreRepository.store()` calls `embeddingStore.add(embedding, segment)` which generates a random UUID, and `LuceneEmbeddingStore.add()` appends a document via `indexWriter.addDocument(doc)` even if supplied an existing ID. Kafka redelivery can therefore duplicate indexed evidence.
  - *Target remediation:*
    - Define event identity using `SystemLog.eventId` (with deterministic fallback `kafka-{partition}-{offset}`), including the conflict policy for repeated IDs.
    - Pass the stable ID through `VectorStoreRepository.store()` and implement an atomic upsert in `LuceneEmbeddingStore` using `indexWriter.updateDocument(new Term(FIELD_ID, id), doc)`.
    - Keep operational processing-attempt counters in `LogIngestionConsumer` distinct from counts of unique indexed events.
    - Test repeated writes, concurrent duplicate delivery, replay, and crash failure after indexing but before Kafka acknowledgment.
- [ ] **1.2 Durable Lucene Storage Aligned with Kafka Offset Commits:**
  - *Current limitation:* `LuceneEmbeddingStore` calls `searcherManager.maybeRefresh()` for near-real-time search visibility, but does not explicitly call `indexWriter.commit()` before `LogIngestionConsumer` acknowledges Kafka. Search visibility is not crash durability.
  - *Target remediation:*
    - Establish a durable-write boundary (`indexWriter.commit()`) before committing the corresponding Kafka offsets (`acknowledgment.acknowledge()`).
    - Propagate indexing and commit errors so failed writes are never acknowledged to Kafka.
    - Preserve durability-before-commit ordering when micro-batching is introduced, and add an abrupt-process-termination recovery test against a disk-backed `FSDirectory` index.
- [ ] **1.3 Enforceable Request `k` Parameter Contract:**
  - *Current limitation:* `QueryController` logs `request.getEffectiveK()` but passes only `request.prompt()` to `sreAnalystAgent.query(...)`. The HTTP request's `k` value does not govern tool retrieval or returned citations.
  - *Target remediation:*
    - Enforce `QueryRequest.k` (defaults, validation bounds `1 <= k <= 20`) in the retrieval execution path (`SreTools.vectorSearch`) and final `QueryResponse.sources` list for both synchronous and streaming requests; prompt instructions alone are not enforcement.
    - Return explicit HTTP 400 validation errors for invalid inputs and apply consistent bounds to MCP tool calls.
- [ ] **1.4 Truthful Time-Windowed Statistics & Dynamic Service Discovery:**
  - Index filterable metadata (`service_name`, `log_level`, `timestamp_ms`) as queryable Lucene fields rather than relying solely on stored metadata JSON or hardcoded service lists.
  - Compute totals, severity breakdowns, and service breakdowns over the exact same jointly filtered event set within the UTC cutoff window.
  - Discover service names dynamically from indexed data instead of a fixed list (`payment-gateway`, `user-auth`, `inventory-service`, `frontend-bff`), define UTC cutoff boundaries and event-time vs. ingestion-time semantics, and inject a `java.time.Clock` for deterministic boundary tests.
- [ ] **1.5 Request-Scoped Evidence Preservation Across Multi-Tool Calls:**
  - *Current limitation:* A successful `vectorSearch` overwrites `currentSources` (`ThreadLocal`), an empty search leaves prior sources unchanged, and streaming callbacks execute asynchronously without `ThreadLocal` affinity or cleanup.
  - *Target remediation:*
    - Accumulate and deduplicate `LogSource` evidence across multiple searches within a single investigation, bounded consistently with the `k` contract.
    - Replace `ThreadLocal` with an explicit per-investigation context that remains isolated across concurrent and asynchronous streaming execution and cleans up on completion, failure, timeout, or cancellation.
    - Report missing or malformed metadata explicitly rather than fabricating random UUIDs or `Instant.now()` timestamps.
- [ ] **1.6 Genuine Token Streaming & Client Disconnect Lifecycle:**
  - *Current limitation:* `QueryController.queryStream` falls back on any `IllegalArgumentException` to synchronous `query()`, splits the full answer by whitespace, and emits words with `Thread.sleep(15)` delays.
  - *Target remediation:*
    - Use genuine model token streaming (or explicitly document buffered delivery without artificial delays if a model lacks streaming tool support), restricting any fallback to a specific recognized capability exception.
    - Handle `SseEmitter` `onTimeout`, `onCompletion`, and `onError` lifecycle callbacks and cancel outstanding model work on client disconnect.
- [ ] **1.7 Real Pipeline Integration & Supported Runtime Baseline:**
  - Separate mocked unit tests from real pipeline integration tests in `LogPipelineIntegrationTest` (enable Kafka listener startup, use Testcontainers Kafka + real in-process ONNX `AllMiniLmL6V2EmbeddingModel` + isolated Lucene `FSDirectory`, and verify retrieval of the exact produced `eventId`).
  - Add `@WebMvcTest` / `MockMvc` controller contract tests covering validation, JSON serialization, and asynchronous SSE streaming.
  - Require the real integration suite in Docker-enabled CI (never silently skipping in CI) and explicitly enforce the supported JDK 21 runtime baseline.

---

## 🛠️ Phase 2: Software Engineering Hardening & Reliability

**Goal:** Eliminate single points of failure, ensure code correctness through automated testing, and optimize ingestion throughput.

- [x] **2.1 Baseline Test Suite (JUnit 5 + Mockito + Testcontainers):**
  - **Unit Tests:**
    - Test `SystemLog` Java 21 Record validation (valid logs, missing required fields, ISO-8601 parsing, metadata consistency).
    - Test `toEmbeddingText()` chunk transformation and normalization rules.
    - Test `Prompts` template formatting and system guardrail assembly.
    - Mock Apache Lucene repository and LangChain4j embedding store to unit-test `vectorSearch` and `logStats` tools in isolation.
  - **Integration Tests:**
    - Existing controller tests exercise endpoint methods and agent construction; they do not establish full MVC request/streaming coverage.
    - `LogPipelineIntegrationTest` exercises producer/consumer behavior with Testcontainers Kafka, but mocks `VectorStoreRepository`; it does not verify ONNX embedding or Lucene persistence.
  - **Remaining verification gaps:** Add real Kafka-to-ONNX-to-Lucene integration coverage, MVC contract tests, and question-to-evidence agent evaluations in items 1.7, 2.5, and 4.1–4.2.
- [x] **2.2 CI/CD Pipeline (GitHub Actions for Java):**
  - Created `.github/workflows/ci.yml` executing on every pull request, push to `main`/`master`, and `workflow_dispatch`.
  - Configured Maven Wrapper (`mvnw` / `mvnw.cmd`) pinned to Apache Maven 3.9.9 and JDK 21 (Eclipse Temurin).
  - Automated test execution with `./mvnw -B clean verify` (including Testcontainers Kafka integration tests and Surefire artifact archiving).
  - Multi-stage Docker build validation step verifying container assembly and security layering.
- [ ] **2.3 Kafka Consumer Micro-Batching & Throughput Optimization:**
  - *Current limitation:* Consuming and vectorizing logs individually ($O(1)$ calls) bottlenecks ingestion throughput to ~20–40 logs/sec.
  - *Target architecture:*
    - Buffer incoming logs into micro-batches using Spring Kafka's `BatchAcknowledgingMessageListener` (e.g., maximum batch size of 50–100 messages or a 200ms time window).
    - Vectorize the entire batch using `EmbeddingModel.embedAll(List<TextSegment>)` leveraging vectorized ONNX tensor operations.
    - Execute a single bulk `LuceneEmbeddingStore.addAll(embeddings, segments)` batch insert to Apache Lucene.
    - Commit Kafka offsets asynchronously per batch instead of synchronously per message.
- [ ] **2.4 Error Handling & Resilience (Dead Letter Queue Replay):**
  - Implement a dedicated Dead Letter Queue consumer for `system-logs-dlq` to inspect and quarantine poisoned messages.
  - Add an automated exponential backoff retry mechanism with dead-letter replay endpoints (`POST /api/v1/admin/dlq/replay`).
  - Add structured Spring Boot lifecycle logging with dependency readiness health indicators (Kafka KRaft broker and Apache Lucene readiness polling).
- [ ] **2.5 Question-to-Evidence End-to-End Tests:**
  - After adding real pipeline integration coverage, seed a small, controlled incident and submit a representative question through the query API.
  - Assert that retrieval returns the expected event IDs and that the answer reflects required evidence and key facts; do not compare generated prose to an exact answer string.
  - Include ambiguous and contradictory evidence cases, such as a log message saying “insufficient funds” whose exception stack trace reports a socket timeout. Require the answer to distinguish the message from the exception, acknowledge conflicts, and avoid asserting an unproven single root cause.
  - Verify citation IDs resolve to retrieved records and that unsupported details are not presented as established facts. Keep deterministic retrieval/API assertions in CI and place live-model variability in the evaluation suite.
- [ ] **2.6 Concurrency Hardening & JNI ONNX Carrier Thread Isolation:**
  - **JNI Pinning & CPU-Bound Inference Caveat (Interview Deep-Dive):** While Virtual Threads excel at I/O-bound waiting, executing in-process **ONNX Runtime via JNI** enters a native C++ frame, which **pins** the Virtual Thread to its underlying OS **Carrier Thread** (preventing unmount) while performing **CPU-bound** matrix math. Under burst traffic, if dozens of Virtual Threads invoke JNI ONNX inference simultaneously, they can pin and saturate all Carrier Threads in the `ForkJoinPool`, causing **Carrier Thread starvation** and delaying lightweight HTTP/SSE endpoints. Mitigation: keep Virtual Threads for I/O orchestration, gate concurrent JNI ONNX calls behind a `Semaphore` (bulkhead) or a dedicated bounded platform thread pool, and batch embeddings during ingestion.

---

## 🧠 Phase 3: Advanced Agentic Architecture & Orchestration

**Goal:** Upgrade the LangChain4j agent from a stateless ReAct loop into a stateful, robust investigative assistant integrated with multiple external systems.

- [ ] **3.1 Multi-Turn Conversational Memory & State Checkpointing:**
  - Introduce session identification (`thread_id` / `session_id`) in `QueryRequest`.
  - Wire LangChain4j `ChatMemory` backed by a pluggable store (`MessageWindowChatMemory` in-memory, or persistent Redis / PostgreSQL state store).
  - Enable conversational investigations (e.g., User: *"What failed?"* -> Agent: *"Payment service timed out connecting to Stripe"* -> User: *"Can you show me the stack trace and impacted customer IDs for the third occurrence?"*).
- [ ] **3.2 Structured Tool Outputs & Type-Safe Citations:**
  - *Current limitation:* Tool output is formatted as structured text strings, which the API parses to construct `LogSource` models.
  - *Target architecture:* Return strongly-typed Java records directly from tools, allowing LangChain4j and the REST API to cleanly map results to typed `LogSource` DTOs without string manipulation.
- [ ] **3.3 Multi-Server MCP Integration (Autonomous Root-Cause Analysis):**
  - Connect the agent to external MCP servers alongside native log tools:
    - **GitHub MCP Server:** Inspect recent git commits, pull requests, and active deployment tags in service repositories to correlate log spikes with recent code deployments.
    - **Prometheus / Metrics MCP Server:** Correlate application error spikes with JVM and infrastructure metrics (CPU load, memory pressure, HikariCP connection pool exhaustion).
    - **Slack / Teams MCP Server:** Enable the agent to broadcast incident summaries directly to on-call incident channels.
  - *Demo Scenario:* Agent detects database connection timeouts -> queries Prometheus MCP to see HikariCP pool saturation -> queries GitHub MCP to discover a recent PR that reduced pool size -> synthesizes root cause and suggests immediate rollback.
- [ ] **3.4 Query Routing & Fallbacks:**
  - Implement query preprocessing (classifying whether a query is purely statistical, purely semantic, or conversational).
  - Add fallback logic when the local LLM struggles with tool calling (retry mechanism with self-correction prompt and structured error feedback).

---

## 📊 Phase 4: LLMOps, Evaluation & Quality Assurance

**Goal:** Establish systematic quantitative metrics to measure retrieval accuracy, hallucination prevention, and agent answer quality.

- [ ] **4.1 Incident Golden Benchmark Dataset:**
  - Create a curated dataset (`src/test/resources/evals/golden_dataset.json`) containing 20+ realistic incident scenarios across microservices.
  - Fields per scenario:
    - `query`: The user question.
    - `relevant_log_ids`: Ground truth log entries that must be retrieved.
    - `expected_facts`: Required evidence-backed facts and acceptable root-cause conclusions, rather than one exact answer string.
    - `unsupported_claims`: Conclusions the evidence does not support and the answer must not assert.
    - `conflicting_evidence`: Optional competing log fields or records the answer should distinguish or qualify.
    - `expected_tools`: The tools that should have been invoked (`vectorSearch`, `logStats`, etc.).
- [ ] **4.2 Deterministic Agent & RAG Evaluation Pipeline:**
  - Add reproducible tests with a stubbed LLM to verify query routing, expected tool calls, evidence propagation, citation mapping, and API response contracts against controlled fixtures. These tests validate orchestration and evidence handling, not the semantic quality of a live model's generated answer.
  - Score retrieval independently with Hit Rate @ K, Recall @ K, and Mean Reciprocal Rank so retrieval regressions are separable from generation variability.
  - Use fixed fixture answers where deterministic answer assembly needs coverage; compare against semantic criteria or structured results, never exact prose from a generative model.
  - Expose an automated evaluation runner that generates a machine-readable result and concise summary report.
- [ ] **4.3 Optional Live-Model Evaluation & Quality Gates:**
  - Run a small curated question set against the configured Ollama model as a separate, opt-in or scheduled evaluation, not as a timing-sensitive unit test on every CI run.
  - Record model name/version, prompt/configuration, retrieved evidence, answer, latency, and per-criterion scores to make regressions diagnosable and comparisons reproducible.
  - Evaluate whether required facts are present, claims are grounded in retrieved evidence, conflicts and uncertainty are represented, citations point to relevant records, and unsupported claims are avoided. Use semantic criteria or a calibrated evaluator; do not require exact answer wording.
  - Consider an evaluation framework (e.g., Ragas, DeepEval, or LangChain4j evaluation support) for context precision/recall, faithfulness, and answer relevance, while keeping evaluator variability visible in results.
  - Define explicit acceptance thresholds for evidence coverage, citation relevance, unsupported-claim rate, and correct handling of ambiguous evidence before using results as a release gate.
- [x] **4.4 Observability & OpenTelemetry / LangSmith Tracing:**
  - Configured LangChain4j `LlmObservabilityListener` (`ChatModelListener`) for token consumption, latency tracking, tool-call telemetry, and asynchronous LangSmith trace export (`LANGCHAIN_TRACING_V2=true`).

---

## ⚡ Phase 5: Streaming, Data Ingestion & Vector Optimization

**Goal:** Enhance log generation realism, optimize data retention, and improve search relevance across metadata dimensions.

- [ ] **5.1 Scenario-Driven Log Generation & Multi-Incident Simulation:**
  - *Current limitation:* The mock producer alternates across standard log templates with static error types, constraining agent demo queries.
  - *Target architecture:*
    - **Enterprise Schema Enrichment:** Add `environment` (`prod`/`staging`), `region` (`us-east-1`/`eu-west-1`), and HTTP context (`status_code`, `http_method`, `endpoint`, `duration_ms`, `customer_id`).
    - **Multi-Archetype Incident Simulator:** Upgrade `MockLogGenerator` with selectable incident profiles:
      - *Database & Connection Pool Exhaustion:* `HikariPool-1 - Connection is not available, request timed out`, deadlock alerts, lock wait timeouts on specific tables.
      - *Security & Access Anomalies:* Spikes of `401 Unauthorized` / `403 Forbidden`, expired JWT claims, credential-stuffing patterns on `/api/v1/auth/login`.
      - *Third-Party & Downstream Degradation:* External provider rate limiting (`StripeAPIError: 429 Too Many Requests`), webhook delivery timeouts, payment gateway `502 Bad Gateway`.
      - *Resource & Memory Exhaustion:* `java.lang.OutOfMemoryError: Java heap space`, excessive GC pauses (>3000ms), container cgroup OOM kills.
      - *Cascading Microservice Outages:* Upstream gateway 504 timeouts triggering retry storms, circuit breakers tripping (`CLOSED` -> `OPEN`) on dependent services.
    - **Expanded Demo Questions Unlocked for the Agent:**
      - *"Which third-party integration is currently rate-limiting us or returning 429s?"*
      - *"Are there any signs of authentication anomalies or brute-force attempts on the auth service in the last hour?"*
      - *"Trace customer transaction `cust_9412` across services to explain why checkout failed."*
      - *"Which endpoints are seeing elevated 5xx error rates, and what are their p95 latency percentiles?"*
      - *"Did any service experience memory leaks or GC pauses prior to terminating?"*
      - *"What was the root cause and downstream blast radius of the database connection pool saturation?"*
- [ ] **5.2 Advanced Chunking Strategies:**
  - Experiment with dynamic chunking:
    - Window-based aggregation: group consecutive logs from the same service/trace within a 10-second window into a single chunk.
    - Stack trace segmentation: split extensive Java stack traces into structured chunks while preserving the top-level exception header.
- [ ] **5.3 Vector Store Lifecycle & Retention Policy (TTL):**
  - Implement a background retention worker to prune logs older than a configurable retention period (e.g., 24 hours), avoiding unbounded vector index growth in Apache Lucene.
- [ ] **5.4 Hybrid Search (Dense Embeddings + Sparse BM25):**
  - Log analysis frequently requires exact keyword lookups (error codes like `ERR_503_CONN_RESET`, specific IP addresses, or exact UUIDs) which dense embeddings can miss.
  - Implement hybrid search combining dense semantic similarity with sparse BM25 / lexical retrieval.

---

## 🖥️ Phase 6: Interactive Demo & Showcase Experience

**Goal:** Deliver an intuitive, visually striking interface and reproducible presentation materials tailored for live demos and technical interviews.

- [ ] **6.1 Interactive Web UI (Split-View Dashboard):**
  - **Split-View Dashboard (React / Vite or Streamlit):**
    - *Left Pane:* Live real-time log ingestion stream with filtering by log level and visual alerts on `ERROR`.
    - *Right Pane:* Interactive chat interface featuring:
      - Real-time token streaming via SSE (`/api/v1/query/stream`).
      - Visual expander widgets revealing the agent's thought process, tool invocations, and raw source documents.
      - Service health statistics widgets (error rate breakdown).
- [ ] **6.2 End-to-End Incident Demo Script ("The Golden Path"):**
  - Document a guided 3-minute walkthrough script:
    1. Trigger a synthetic cascading failure via an admin API or script command.
    2. Watch real-time logs flood the Kafka stream and Lucene collection.
    3. Query the agent: *"Why did payment-gateway fail around 14:00?"*.
    4. Observe the agent execute `logStats` to detect the error spike, followed by `vectorSearch` to pinpoint the database timeout.
    5. Follow up with conversational memory: *"Which specific customer transactions were impacted?"*.
- [ ] **6.3 Presentation & Visual Portfolio Assets:**
  - Record a 90-second high-resolution demo video / GIF showcasing the interactive UI, SSE streaming, and MCP interaction.
  - Include an interactive system architecture diagram and benchmark results in presentation decks.
