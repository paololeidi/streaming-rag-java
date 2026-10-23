# Enterprise Streaming RAG in Java: Build, Run & Test Guide

This comprehensive guide walks you through interacting with and verifying every layer of the **Enterprise Streaming RAG in Java** architecture:
- **Event-Driven Log Ingestion:** Datafaker -> Apache Kafka -> In-process ONNX Embedding -> Embedded Apache Lucene.
- **Embedded Vector Database & Sliding-Window Analytics:** Native JVM-level dense vector indexing (`KnnFloatVectorField` HNSW cosine similarity) and sliding UTC time-window range queries (`LongPoint` `timestamp_ms`) powered by Apache Lucene.
- **Interactive OpenAPI / Swagger UI:** Browser API playground at `/swagger-ui.html` and OpenAPI 3.0 spec at `/v3/api-docs`.
- **Agentic REST API:** Synchronous incident investigation with citations (`POST /api/v1/query`).
- **Real-Time Token Streaming:** Server-Sent Events (SSE) streaming (`POST /api/v1/query/stream`).
- **Model Context Protocol (MCP) Server & Client:** Live JSON-RPC 2.0 tool execution (`vector_search`, `log_stats`) for Cursor IDE and Claude Desktop (`/sse`, `/messages`), plus dynamic external MCP tool discovery via `MCP_SERVER_URL`.
- **Cloud-Native Kubernetes (Kind & Helm) & LangSmith Observability:** Single-command Kind cluster deployment (`./scripts/kind-deploy.sh`) and LLM tracing (`LANGCHAIN_TRACING_V2=true`).

---

## 📋 Table of Contents
1. [Prerequisites & Deployment Modes](#1-prerequisites--deployment-modes)
   - [Option A: Full Self-Contained Stack in Docker (Evaluators & Recruiters)](#option-a-full-self-contained-stack-in-docker-evaluators--recruiters)
   - [Option B: Lean Local Dev Mode (Recommended for Laptops & Daily Dev)](#option-b-lean-local-dev-mode-recommended-for-laptops--daily-dev)
   - [Option C: Kubernetes (Kind + Helm 3)](#option-c-kubernetes-kind--helm-3)
2. [Step 1: Healthcheck & Interactive Swagger UI](#step-1-healthcheck--interactive-swagger-ui)
3. [Step 2: Monitoring Log Ingestion & In-Process Vectorization](#step-2-monitoring-log-ingestion--in-process-vectorization)
4. [Step 3: Interacting with the SRE Analyst Agent (REST API)](#step-3-interacting-with-the-sre-analyst-agent-rest-api)
5. [Step 4: Real-Time Token-by-Token SSE Streaming](#step-4-real-time-token-by-token-sse-streaming)
6. [Step 5: Querying via Model Context Protocol (MCP)](#step-5-querying-via-model-context-protocol-mcp)
7. [Step 6: Cursor IDE, Claude Desktop & External MCP Client Integration](#step-6-cursor-ide-claude-desktop--external-mcp-client-integration)
8. [Step 7: LLM Observability & LangSmith Tracing](#step-7-llm-observability--langsmith-tracing)
9. [Step 8: Automated Test Suite Verification](#step-8-automated-test-suite-verification)

---

## 1. Prerequisites & Deployment Modes

The repository provides **streamlined deployment options**, mirroring the battle-tested setup of the Python sister project ([`streaming-rag`](https://github.com/paololeidi/streaming-rag)):

| Deployment Mode | Who it's for | What runs in Docker / K8s | What runs on Host (Native OS) | Total Memory Footprint |
| :--- | :--- | :--- | :--- | :--- |
| **Option A: Full Stack** | **Recruiters, Evaluators, CI/CD** | Kafka + Ollama + Java App (Embedded Lucene) | *Nothing* (100% Hermetic) | ~5–6 GB (requires 16GB+ RAM) |
| **Option B: Lean Local Dev** | **Personal Laptops & Daily Dev** *(Recommended!)* | **Kafka only** (`docker-compose.infra.yml`) | **Java App (Embedded Lucene) + Native Ollama** | **< 150 MB in Docker, ~3 GB Host** |
| **Option C: Kind + Helm** | **Kubernetes & Cloud-Native Validation** | Kind Cluster (Kafka + Ollama + Java App) | `kubectl` + `helm` | ~6–8 GB |

> 💡 **Why Apache Lucene replaces ChromaDB:**  
> In Python projects, ChromaDB can run embedded in-process. However, in Java, ChromaDB required running a separate, heavy external container over HTTP. By replacing ChromaDB with **Apache Lucene 9 (HNSW k-NN vector search + BKD-tree `LongPoint`)**, the vector database now runs **100% in-process within the JVM**. This completely eliminates the ChromaDB container, frees up substantial RAM and CPU, and removes external network hops for vector indexing and queries!

---

### Option A: Full Self-Contained Stack in Docker (Evaluators & Recruiters)

> 🎯 **Best for:** Reviewers, recruiters, or dedicated servers/workstations who want to clone the repo and run everything in Docker without installing local Java, Maven, or Ollama CLI tools.

#### 1. Launch the Stack
From the project root:
```bash
docker compose up -d --build
```
This boots all 3 containers:
- **`streaming-rag-kafka`** (KRaft broker on port `9092` / `29092`)
- **`streaming-rag-ollama`** (Local LLM engine on port `11434`)
- **`streaming-rag-app`** (Java 21 Spring Boot app on port `8080` with embedded Apache Lucene vector storage, pre-wired to `http://ollama:11434`)

#### 2. Pull the LLM Model into the Container
Once Ollama is running:
```bash
docker exec -it streaming-rag-ollama ollama pull llama3.2:3b
```

---

### Option B: Lean Local Dev Mode (Recommended for Laptops & Daily Dev)

> 🚀 **Recommended for Personal Laptops (e.g. 8–16 GB RAM with integrated GPUs like Intel Iris Xe):**  
> Matches the leanest development setup possible, eliminating WSL2 memory exhaustion and disk swap thrashing.

#### Step 1: Stop any existing Docker containers
```bash
docker compose down
```

#### Step 2: Start Minimal Infrastructure (Kafka only)
```bash
docker compose -f docker-compose.infra.yml up -d
```
Verify the container is healthy:
```bash
docker ps
```
*(You will see only `streaming-rag-kafka` on port 9092).*

#### Step 3: Ensure Native Ollama is Running
Make sure Ollama is running on your host:
```powershell
ollama run llama3.2:3b "say ready in one word"
```

#### Step 4: Launch Spring Boot Natively on Host
```powershell
# Set JAVA_HOME to your local JDK 21 installation if not already on PATH
# $env:JAVA_HOME = "C:\path\to\jdk-21"
mvn spring-boot:run
```
*(All defaults in `application.yml` already point to `localhost:9092` for Kafka, embedded `./data/lucene-index` for Apache Lucene, and `localhost:11434` for Ollama!)*

---

### Option C: Kubernetes (Kind + Helm 3)

Deploy the entire stack (Kafka KRaft, Ollama with `ollama-init` model pull Job, and the Spring Boot application with PVC-backed Lucene storage) to a local Kind Kubernetes cluster:

```bash
./scripts/kind-deploy.sh
```

Or manually with Helm:
```bash
helm lint ./charts/streaming-rag
helm upgrade --install streaming-rag ./charts/streaming-rag -n streaming-rag --create-namespace
kubectl port-forward -n streaming-rag svc/streaming-rag 8080:8080
```
See [`charts/streaming-rag/README.md`](../charts/streaming-rag/README.md) for full Helm configuration values.

---

## Step 1: Healthcheck & Interactive Swagger UI

Verify that the Spring Boot application is healthy and explore the interactive OpenAPI documentation.

### cURL
```bash
curl -X GET http://localhost:8080/health
```

### PowerShell
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/health" -Method Get
```

### Expected Response
```json
{
  "status": "ok"
}
```

### Interactive OpenAPI / Swagger UI
- **Swagger UI Playground:** Open [`http://localhost:8080/swagger-ui.html`](http://localhost:8080/swagger-ui.html) in your browser.
- **OpenAPI 3.0 JSON Specification:** [`http://localhost:8080/v3/api-docs`](http://localhost:8080/v3/api-docs)

---

## Step 2: Monitoring Log Ingestion & In-Process Vectorization

With synthetic producer enabled (`APP_PRODUCER_ENABLED=true` or `-Dapp.producer.enabled=true`), the `MockLogGenerator` continuously produces realistic logs across 4 microservices:
- `payment-gateway`
- `user-auth`
- `inventory-service`
- `frontend-bff`

Inspect real-time log ingestion (in terminal or Docker):
```text
[PRODUCE] Sent log eventId=2c952f6d-... service=user-auth level=WARN to partition=0 offset=104
[CONSUME] Indexed log eventId=2c952f6d-... service=user-auth level=WARN partition=0 offset=104 into vector store id=...
```

**Key Architectural Guarantees Observed:**
1. **Partition-Level Ordering:** Events are keyed by `serviceName`, guaranteeing sequential processing per microservice.
2. **In-Process ONNX Embeddings:** All-MiniLM-L6-v2 runs directly inside the JVM, producing 384-dimensional dense vectors with **zero network hops** to Python.
3. **In-Process Apache Lucene Vector & Timestamp Indexing:** Embeddings (`KnnFloatVectorField`) and epoch timestamps (`LongPoint` `timestamp_ms`) are indexed natively into Lucene in real time.
4. **At-Least-Once Delivery:** Kafka offsets are acknowledged (`AckMode.MANUAL_IMMEDIATE`) **only after** successful persistence in Apache Lucene.

---

## Step 3: Interacting with the SRE Analyst Agent (REST API)

Send an incident investigation query to the SRE Analyst Agent. The agent executes an autonomous ReAct loop:
1. Identifies the issue and invokes `vectorSearch` over Apache Lucene.
2. Retrieves and analyzes relevant log excerpts.
3. Calls `logStats` (backed by Lucene `LongPoint` sliding time-window range queries) to evaluate blast radius and error distributions.
4. Synthesizes a structured incident report citing exact log sources.

### cURL
```bash
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Investigate why checkout is failing and analyze errors in payment-gateway or inventory-service",
    "k": 5
  }'
```

### PowerShell
```powershell
$body = @{
    prompt = "Investigate why checkout is failing and analyze errors in payment-gateway or inventory-service"
    k = 5
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8080/api/v1/query" `
  -Method Post `
  -ContentType "application/json" `
  -Body $body | ConvertTo-Json -Depth 5
```

### Example Structured Response
```json
{
  "answer": "### Executive Summary\nThe checkout workflow experienced multiple failures caused by timeout exceptions when communicating with the Stripe API in payment-gateway...\n\n### Timeline & Evidence\n- [ord_83921049]: payment-gateway reported 'Read timed out connecting to https://api.stripe.com/v1/charges'...\n\n### Root Cause Analysis (RCA)\nUpstream payment gateway saturation.\n\n### Actionable Remediation\n1. Increase gateway timeout threshold.\n2. Enable exponential backoff retries.",
  "sources": [
    {
      "eventId": "214a1f3c-59bf-4da2-8561-12c1b979a4de",
      "timestamp": "2026-10-12T14:22:10Z",
      "serviceName": "payment-gateway",
      "logLevel": "ERROR",
      "excerpt": "[ERROR] payment-gateway @ 2026-10-12T14:22:10Z\nStripe API unreachable after 3 retries...",
      "score": 0.892
    }
  ],
  "generatedAt": "2026-10-12T15:10:00Z"
}
```

---

## Step 4: Real-Time Token-by-Token SSE Streaming

Experience low-latency streaming inference powered by Java 21 Virtual Threads and LangChain4j `TokenStream`.

### cURL
```bash
curl -N -X POST http://localhost:8080/api/v1/query/stream \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "prompt": "Provide a quick root cause summary of recent database errors",
    "k": 3
  }'
```

### Expected Streaming Output
Tokens are streamed sequentially into your console terminated with `[DONE]`:
```text
data: Based

data:  on

data:  the

data:  retrieved

data:  database

data:  logs

...

data: [DONE]
```

---

## Step 5: Querying via Model Context Protocol (MCP)

The application embeds an official Model Context Protocol (MCP) server (JSON-RPC 2.0 / Protocol `2024-11-05`). You can test it via HTTP:

### 5.1 Initialize MCP Handshake
```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {}
  }'
```
**Expected Output:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "serverInfo": {
      "name": "streaming-rag-mcp-server",
      "version": "1.0.0"
    }
  }
}
```

### 5.2 Discover Available Tools (`tools/list`)
```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list",
    "params": {}
  }'
```
Returns definitions and JSON schemas for `vector_search` (backed by Apache Lucene) and `log_stats` (sliding time-window statistics).

### 5.3 Execute `vector_search` Tool via MCP
```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "vector_search",
      "arguments": {
        "query": "LDAP connection failure",
        "k": 3
      }
    }
  }'
```

### 5.4 Execute `log_stats` Tool via MCP
```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 4,
    "method": "tools/call",
    "params": {
      "name": "log_stats",
      "arguments": {
        "log_level": "ERROR",
        "service_name": "ALL",
        "minutes": 15
      }
    }
  }'
```

---

## Step 6: Cursor IDE, Claude Desktop & External MCP Client Integration

### Cursor IDE Setup (Embedded MCP Server)
1. In Cursor, open **Settings** (`Ctrl + ,` or `Cmd + ,`).
2. Select **Features** -> **MCP Servers**.
3. Click **Add New MCP Server**:
   - **Name:** `streaming-rag-java`
   - **Type:** `sse`
   - **URL:** `http://localhost:8080/sse`
4. Confirm green status lights for `vector_search` and `log_stats`.
5. Open Cursor Agent Chat and type:
   > *"@streaming-rag-java find recent ERROR logs in user-auth and tell me if there was an LDAP timeout."*

### Claude Desktop Setup
In your Claude Desktop configuration file (`%APPDATA%\Claude\claude_desktop_config.json` on Windows):
```json
{
  "mcpServers": {
    "streaming-rag-java": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

### Connecting `SreAnalystAgent` to External MCP Servers (`MCP_SERVER_URL`)
To have the Java `SreAnalystAgent` dynamically discover and invoke tools from an external MCP server (via `McpClientService`), set `MCP_SERVER_URL`:
```bash
export MCP_SERVER_URL=http://localhost:8001/sse
mvn spring-boot:run
```
If the external MCP server is unconfigured or unreachable, the agent logs a warning and gracefully falls back to native `SreTools`.

---

## Step 7: LLM Observability & LangSmith Tracing

Every Ollama request and response is instrumented by `LlmObservabilityListener` (`ChatModelListener`), logging structured telemetry:
```text
[LLM TRACE: REQUEST] runId=... model=llama3.2:3b messages=2 tools=2
[LLM TRACE: RESPONSE] runId=... model=llama3.2:3b latencyMs=1420 inputTokens=185 outputTokens=64 totalTokens=249 toolCalls=1
```

To enable asynchronous trace export to **LangSmith**, set the standard environment variables:
```bash
export LANGCHAIN_TRACING_V2=true
export LANGCHAIN_API_KEY=lsv2_pt_...
export LANGCHAIN_PROJECT=streaming-rag-java
```

---

## Step 8: Automated Test Suite Verification

Run the full automated test suite (covering unit logic, in-process ONNX embeddings, embedded Apache Lucene vector & range indexing, OpenAPI configuration, REST controllers, dynamic MCP client & server protocol, LLM observability listener, and Testcontainers Kafka integration):

```bash
mvn clean test
```
*(or using the included wrapper: `./mvnw clean test` on Linux / `.\mvnw.cmd clean test` on Windows)*

**Expected Result:**
```text
[INFO] Results:
[INFO] Tests run: ..., Failures: 0, Errors: 0
[INFO] BUILD SUCCESS
```
*(If Docker Desktop is offline on your host, Testcontainers integration tests are conditionally bypassed, ensuring smooth compilation in isolated environments).*
