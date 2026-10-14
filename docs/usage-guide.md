# Enterprise Streaming RAG in Java: End-to-End Usage & Verification Guide

This comprehensive guide walks you through interacting with and verifying every layer of the **Enterprise Streaming RAG in Java** architecture:
- **Event-Driven Log Ingestion:** Datafaker -> Apache Kafka -> In-process ONNX Embedding -> Embedded Apache Lucene.
- **Embedded Vector Database:** Native JVM-level dense vector indexing and cosine k-NN similarity search powered by Apache Lucene (zero external vector database container!).
- **Agentic REST API:** Synchronous incident investigation with citations (`POST /api/v1/query`).
- **Real-Time Token Streaming:** Server-Sent Events (SSE) streaming (`POST /api/v1/query/stream`).
- **Model Context Protocol (MCP):** Live JSON-RPC 2.0 tool execution (`vector_search`, `log_stats`) for Cursor IDE and Claude Desktop.

---

## 📋 Table of Contents
1. [Prerequisites & Deployment Modes](#1-prerequisites--deployment-modes)
   - [Option A: Full Self-Contained Stack in Docker (Evaluators & Recruiters)](#option-a-full-self-contained-stack-in-docker-evaluators--recruiters)
   - [Option B: Lean Local Dev Mode (Recommended for Laptops & Daily Dev)](#option-b-lean-local-dev-mode-recommended-for-laptops--daily-dev)
2. [Step 1: Healthcheck & Liveness Verification](#step-1-healthcheck--liveness-verification)
3. [Step 2: Monitoring Log Ingestion & In-Process Vectorization](#step-2-monitoring-log-ingestion--in-process-vectorization)
4. [Step 3: Interacting with the SRE Analyst Agent (REST API)](#step-3-interacting-with-the-sre-analyst-agent-rest-api)
5. [Step 4: Real-Time Token-by-Token SSE Streaming](#step-4-real-time-token-by-token-sse-streaming)
6. [Step 5: Querying via Model Context Protocol (MCP)](#step-5-querying-via-model-context-protocol-mcp)
7. [Step 6: Cursor IDE & Claude Desktop Integration](#step-6-cursor-ide--claude-desktop-integration)
8. [Step 7: Automated Test Suite Verification](#step-7-automated-test-suite-verification)

---

## 1. Prerequisites & Deployment Modes

The repository provides **two streamlined deployment options**, mirroring the battle-tested setup of the Python sister project ([`streaming-rag`](https://github.com/paololeidi/streaming-rag)):

| Deployment Mode | Who it's for | What runs in Docker | What runs on Host (Native OS) | Total Memory Footprint |
| :--- | :--- | :--- | :--- | :--- |
| **Option A: Full Stack** | **Recruiters, Evaluators, CI/CD** | Kafka + Ollama + Java App (Embedded Lucene) | *Nothing* (100% Hermetic) | ~5–6 GB (requires 16GB+ RAM) |
| **Option B: Lean Local Dev** | **Personal Laptops & Daily Dev** *(Recommended!)* | **Kafka only** (`docker-compose.infra.yml`) | **Java App (Embedded Lucene) + Native Ollama** | **< 150 MB in Docker, ~3 GB Host** |

> 💡 **Why Apache Lucene replaces ChromaDB:**  
> In Python projects, ChromaDB can run embedded in-process. However, in Java, ChromaDB required running a separate, heavy external container over HTTP. By replacing ChromaDB with **Apache Lucene 9 (HNSW k-NN vector search)**, the vector database now runs **100% in-process within the JVM**. This completely eliminates the ChromaDB container, frees up substantial RAM and CPU, and removes external network hops for vector indexing and queries!

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

#### Why this mode is optimal for development:
When running heavy containers inside Docker Desktop on Windows, WSL2 (`vmmem`) can hit memory thresholds and trigger disk swap thrashing against the SSD.

**The Solution:**
1. **Docker runs ONLY the ultra-lightweight infrastructure** (Apache Kafka in KRaft mode) via `docker-compose.infra.yml`. It consumes **under 150 MB of RAM** and **0% disk swap**.
2. **Vector Database runs natively embedded in the JVM** via Apache Lucene with zero external containers or HTTP latency.
3. **Ollama runs native on the Host OS**, utilizing your machine's CPU and GPU acceleration directly.
4. **Spring Boot runs native on the Host OS** (via IntelliJ IDEA or `mvn spring-boot:run`), enabling instant hot-reloading, step-through debugging, and zero Docker build waiting times.

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
$env:JAVA_HOME = "C:\Users\Utente\.jdks\ms-21.0.12.1"
mvn spring-boot:run
```
*(All defaults in `application.yml` already point to `localhost:9092` for Kafka, embedded `./data/lucene-index` for Apache Lucene, and `localhost:11434` for Ollama!)*

---

## Step 1: Healthcheck & Liveness Verification

Verify that the Spring Boot application is healthy.

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
3. **In-Process Apache Lucene Vector Indexing:** Embeddings and metadata are indexed natively into Lucene's HNSW vector index in real time.
4. **At-Least-Once Delivery:** Kafka offsets are acknowledged (`AckMode.MANUAL_IMMEDIATE`) **only after** successful persistence in Apache Lucene.

---

## Step 3: Interacting with the SRE Analyst Agent (REST API)

Send an incident investigation query to the SRE Analyst Agent. The agent executes an autonomous ReAct loop:
1. Identifies the issue and invokes `vectorSearch` over Apache Lucene.
2. Retrieves and analyzes relevant log excerpts.
3. Optionally calls `logStats` to evaluate blast radius and error distributions.
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
Returns definitions and JSON schemas for `vector_search` (backed by Apache Lucene) and `log_stats`.

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

## Step 6: Cursor IDE & Claude Desktop Integration

Connect Cursor IDE or Claude Desktop directly to the live streaming log store without writing code:

### Cursor IDE Setup
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
Restart Claude Desktop to start diagnosing live logs directly inside the chat interface.

---

## Step 7: Automated Test Suite Verification

Run the full automated test suite (covering unit logic, in-process ONNX embeddings, embedded Apache Lucene vector indexing, REST controllers, Testcontainers Kafka integration, and MCP protocol):

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
