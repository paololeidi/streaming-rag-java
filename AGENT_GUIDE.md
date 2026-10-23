# AGENT RUNBOOK & ONBOARDING GUIDE: Enterprise Streaming RAG in Java

This document is an operational guide designed for **AI Coding Assistants, LLM Agents, and Technical Navigators** (e.g., Cursor, Claude Code, Copilot, Antigravity, Aider).
When a user asks to run, test, inspect, or troubleshoot the **Enterprise Streaming RAG in Java** project, follow the rules, workflows, and diagnostic recipes described here.

---

## 1. Agent Persona & Guiding Principles

1. **Ask Path Preference Upfront, Then Execute Autonomously (Minimum User Effort)**:
    - When the user asks to run, setup, or test the project, ask **one single upfront question**:
      > *"Would you like to run the project via **Option A (Full Self-Contained Stack in Docker - zero local setup)** or **Option B (Lean Local Dev Mode - Kafka in Docker, Java 21 & Ollama native - recommended for laptops)**?"*
    - As soon as the user selects an option, **take complete autonomous control**: do NOT pause or ask for confirmation at every intermediate step.
    - Execute all prerequisite checks, service launches, model pulls, and healthchecks in the background until the system reaches a **verified, working state**.
    - Present the finished result directly to the user with live endpoints and test queries.
    - Intervene or ask further questions ONLY if hitting an external, unresolvable blocker (e.g. Docker daemon is not running, host out of memory, or Java 21 is missing for Option B).
2. **Respect Long-Running vs. One-Shot Processes**:
    - The Kafka Broker, Ollama LLM engine, and the Spring Boot Java Application (which embeds the Datafaker Producer, Log Consumer, ONNX Embedder, Apache Lucene Vector Store, and MCP Server) are **daemon/long-running services**.
    - Always run them in detached/background mode (e.g., `docker compose up -d` or managed background terminal tasks) so the agent turn is never blocked.
3. **OS-Aware Execution (Windows vs. Unix)**:
    - On Windows PowerShell:
        - Verify/Set `JAVA_HOME` if needed (e.g., `$env:JAVA_HOME = "C:\path\to\jdk-21"` or check with `java -version` and `mvn -version`).
        - Run Maven commands with `mvn` or `.\mvnw.cmd`.
        - When using `curl`, be aware that PowerShell aliases `curl` to `Invoke-WebRequest` unless calling `curl.exe` or using `Invoke-RestMethod`.
    - On POSIX (Linux / macOS / Git Bash):
        - Verify Java 21+ (`java -version`).
        - Run with `mvn` or `./mvnw`.
        - Standard `curl` works out of the box.
4. **Step-by-Step Progress Tracking & Final Status Report**:
    - While executing setup steps autonomously, output compact, transparent progress checkpoints so the user can follow along:
      - `[1/4] Environment Config & Prerequisites: OK`
      - `[2/4] Service Startup: OK`
      - `[3/4] Ingestion Pipeline & Lucene Vectorization: OK`
      - `[4/4] End-to-End Validation (/health OK & query test verified): OK`
    - As soon as the stack is running and healthy (in Option A or Option B), **always compile and deliver the Final Setup Status Report** (defined in Section 6) containing the component status table, data pipeline check, live URLs, and sample queries ready to execute.
5. **Mandatory End-to-End Validation & User Notice on LLM Latency**:
    - The agent MUST NOT stop at merely launching processes. It must automatically verify the `/health` endpoint (`{"status": "ok"}`) AND execute a live verification query against `POST /api/v1/query` to confirm the entire agentic loop (Spring Boot + LangChain4j + Ollama + In-process Apache Lucene) works properly.
    - **Inform the User Upfront**: Before/during running the test query, the agent MUST explicitly inform the user that this verification query can take noticeable time (typically 30–90+ seconds, especially on CPU hardware without a dedicated GPU) because Ollama may need to load model weights into RAM (cold start) and the LangChain4j ReAct agent executes multiple inference passes (tool selection, retrieval, synthesis). This sets expectations and prevents the user from assuming the application is stuck.

---

## 2. System Architecture & Port Allocation

```
 ┌──────────────────────────────── Data Pipeline ────────────────────────────────┐
 │                                                                               │
 │  Datafaker Producer ──► Kafka ──► Consumer ──► ONNX Embedder ──► Apache Lucene│
 │   (In-Process Bean)   (KRaft)  (spring-kafka)(All-MiniLM-L6-v2)  (In-Process) │
 │                                                                               │
 └───────────────────────────────────────┬───────────────────────────────────────┘
                                         │
 ┌──────────────────────────────── Agentic Layer ────────────────────────────────┐
 │                                                                               │
 │  POST /api/v1/query & /api/v1/query/stream (SSE)  |  GET /swagger-ui.html     │
 │         │                                                                     │
 │         ▼                                                                     │
 │  LangChain4j @AiService (Ollama · llama3.2:3b + LlmObservabilityListener)     │
 │         ├── @Tool vectorSearch ──► LuceneEmbeddingStore (k-NN Cosine)         │
 │         ├── @Tool logStats    ──► Lucene LongPoint Sliding-Window Stats       │
 │         └── McpClientService  ──► External MCP Servers (MCP_SERVER_URL)       │
 └───────────────────────────────────────┬───────────────────────────────────────┘
                                         │
 ┌─────────────────────────────────── MCP Server ────────────────────────────────┐
 │  Model Context Protocol (JSON-RPC 2.0 / Protocol 2024-11-05)                  │
 │  Embedded inside Spring Boot on Port 8080:                                    │
 │  ├── SSE Transport:         GET /sse  (for Cursor IDE & Claude Desktop)       │
 │  ├── JSON-RPC Messages:     POST /messages                                    │
 │  └── Streamable HTTP:       POST /sse                                         │
 └───────────────────────────────────────────────────────────────────────────────┘
```

| Service | Port | Protocol | Purpose |
|---|---|---|---|
| **Spring Boot Application** | `8080` | HTTP / SSE | REST API (`/api/v1/query`), Stream (`/api/v1/query/stream`), Swagger UI (`/swagger-ui.html`, `/v3/api-docs`), Health (`/health`), MCP SSE (`/sse`), MCP Messages (`/messages`) |
| **Kafka Broker** | `9092` (host) / `29092` (Docker) | TCP (KRaft) | Event-driven log streaming pipeline (topic: `system-logs`) |
| **Ollama Runtime** | `11434` | HTTP | Local LLM inference engine (`llama3.2:3b`) |
| **Apache Lucene 9** | In-Process (Embedded JVM) | File / RAM | High-speed dense vector store (`./data/lucene-index`) + `LongPoint` sliding time-window range index, zero external container |
| **Mock Producer & Consumer** | In-Process (Embedded JVM) | Spring Beans | Continuous synthetic log generation and Kafka vectorization consumer |

---

## 3. Decision Matrix: Initial User Selection

When the user asks to run or test the project, prompt them with the single decision question, then immediately execute that option autonomously:

```
Step 0: Ask User Preference
 ├── Option A: "Option A (Full Self-Contained Stack in Docker - Zero local setup, 100% Hermetic)"
 │    └──► Agent executes OPTION A autonomously to completion -> Delivers working endpoints
 │
 └── Option B: "Option B (Lean Local Dev Mode - Kafka in Docker, Java & Ollama native - Recommended for Laptops)"
      └──► Agent executes OPTION B autonomously to completion -> Delivers working endpoints
```

---

## 4. OPTION A: Full Self-Contained Stack in Docker (Zero Friction)

Best for evaluators, reviewers, recruiters, or CI environments who want to run the entire system in Docker without installing a local Java 21 JDK or Maven.

### Step A.1: Start All Services Detached
From the project root:
```bash
docker compose up -d --build
```
This starts 3 containers:
- `streaming-rag-kafka` (KRaft broker on port `9092` / `29092`)
- `streaming-rag-ollama` (Local LLM engine on port `11434`)
- `streaming-rag-app` (Java 21 Spring Boot app on port `8080` with embedded Lucene)

### Step A.2: Pull the LLM Model into the Ollama Container
Ensure the required model is pulled inside the Ollama container:
```bash
docker exec streaming-rag-ollama ollama pull llama3.2:3b
```

### Step A.3: Health & Readiness Verification
Poll until the Spring Boot application reports healthy:
```bash
# cURL
curl -sf http://localhost:8080/health

# PowerShell
(Invoke-RestMethod -Uri http://localhost:8080/health).status
```
Expected output: `{"status": "ok"}`.

### Step A.4: Check Streaming Pipeline Logs
Verify that synthetic logs are flowing through Kafka, ONNX embeddings, and Lucene:
```bash
docker compose logs -n 30 app
```
Verify log entries resembling:
```text
[PRODUCE] Sent log eventId=... service=user-auth level=WARN to partition=0 offset=...
[CONSUME] Indexed log eventId=... service=user-auth level=WARN partition=0 offset=... into vector store
```

### Step A.5: End-to-End Query Verification & Delivery (Manage Latency Expectations)
As soon as `/health` returns `ok` and log ingestion is confirmed:
1. **Inform User**: Notify the user that an end-to-end test query is being executed to verify the full stack:
   > *"Testing `/api/v1/query`. Please note that this may take 30–90 seconds while Ollama loads the model weights into RAM and the LangChain4j ReAct agent runs its reasoning loop."*
2. **Execute Test Query**: Run a verification query against `POST /api/v1/query` and verify a valid JSON answer with citations is returned.
3. **Deliver Status Report**: Compile and deliver the structured **Final Setup Status Report** (Section 6).
4. **Offer Interactive Testing**: Suggest opening `/swagger-ui.html`, trying the streaming endpoint (`/api/v1/query/stream`), or testing MCP tools.

### Step A.6: Teardown
When the user finishes testing:
```bash
docker compose down -v
```

---

## 5. OPTION B: Lean Local Dev Mode (Recommended for Laptops & Daily Dev)

Best for active code editing, fast iteration, unit testing, and running on machines with limited RAM (e.g. 8–16 GB laptops).

### Why this mode is optimal for development:
- **Zero WSL2 Memory Thrashing**: Heavy Docker containers inside WSL2 on Windows can consume significant RAM and cause disk swap thrashing.
- **Minimal Docker Footprint**: Runs **only Apache Kafka** in Docker via `docker-compose.infra.yml` consuming **under 150 MB RAM**.
- **In-Process Vector Store**: Apache Lucene 9 runs embedded directly inside the JVM (zero ChromaDB container, zero external network hops).
- **Native Ollama Performance**: Ollama runs directly on the Host OS, utilizing local CPU and GPU acceleration.
- **Instant Hot-Reload & Debugging**: Spring Boot runs natively via Maven or IDE with instant startup.

### Step B.1: Prerequisites & Host Environment Check
Ensure the host has:
- **Java 21+** (`java -version`)
- **Maven 3.9+** (`mvn -version`)
- **Ollama CLI** (`ollama --version`)

On Windows PowerShell, set `JAVA_HOME` if needed:
```powershell
$env:JAVA_HOME = "C:\path\to\jdk-21"
```

### Step B.2: Stop Any Existing Heavy Docker Containers
```bash
docker compose down
```

### Step B.3: Start Minimal Infrastructure (Kafka Only)
```bash
docker compose -f docker-compose.infra.yml up -d
```
Verify port `9092` is accessible and container is running:
```bash
docker ps
```
*(You should see only `streaming-rag-kafka` running).*

### Step B.4: Ensure Native Host Ollama is Running & Model Pulled
Ensure the Ollama service is active and the model is downloaded:
```bash
ollama list
# If llama3.2:3b is missing:
ollama pull llama3.2:3b
# Quick sanity check:
ollama run llama3.2:3b "say ready in one word"
```

### Step B.5: Launch Spring Boot Natively on Host
Run the application using Maven:
```powershell
# Windows PowerShell
$env:JAVA_HOME = "C:\path\to\jdk-21"
mvn spring-boot:run
```
```bash
# POSIX / Bash
mvn spring-boot:run
```
*Note: All defaults in `application.yml` already point to `localhost:9092` for Kafka, `./data/lucene-index` for embedded Lucene, and `http://localhost:11434` for Ollama.*
*Both the `MockLogGenerator` (`app.producer.enabled=true`) and `LogIngestionConsumer` run automatically inside the Spring Boot process.*

### Step B.6: Health Check, Ingestion Check, End-to-End Query Verification & Delivery
Once Spring Boot outputs `Started StreamingRagApplication in ... seconds`:
1. **Health Verification**: Check that `GET http://localhost:8080/health` returns `{"status": "ok"}`.
2. **Verify Ingestion**: Confirm in the Spring Boot console that `[PRODUCE]` and `[CONSUME]` messages appear every second.
3. **Inform User on Latency**: Inform the user:
   > *"Executing an end-to-end verification query against `/api/v1/query`. Note: This can take 30–90 seconds (longer on CPU systems without dedicated GPU) while Ollama loads model weights into RAM and the LangChain4j ReAct agent runs multi-pass reasoning."*
4. **Run Test Query**: Execute a test query against `POST http://localhost:8080/api/v1/query` and verify a valid response with citations is returned.
5. **Compile & Deliver Report**: Compile and deliver the **Final Setup Status Report** (Section 6).

---

## 6. Setup Progress Tracking & Final Status Report Specification

Whenever the agent sets up the environment (Option A or Option B), it must:
1. **Provide live progress checkpoints** during execution:
   ```text
   ⏳ [1/4] Environment Configuration (.env/JAVA_HOME, Kafka infra)... OK
   ⏳ [2/4] Service Startup (Kafka, Ollama, Spring Boot app)... OK
   ⏳ [3/4] Ingestion Pipeline Verification (Kafka -> Consumer -> In-process ONNX -> Lucene)... OK
   ⏳ [4/4] End-to-End Validation (/health OK & query test verified)... OK
   ```
   *(Note: While executing checkpoint 4, always alert the user that running the test query may take 30–90 seconds due to Ollama cold start and CPU reasoning cycles).*
2. **Deliver the structured Final Setup Status Report** upon completion:

```markdown
## 🏁 Setup Status Report: Enterprise Streaming RAG in Java

### 🟢 System Status: READY & OPERATIONAL

| Component | Status | Port / Target | Health / Access URL |
|---|---|---|---|
| **Spring Boot Backend** | 🟢 Healthy | `:8080` | [http://localhost:8080/health](http://localhost:8080/health) |
| **Swagger UI / OpenAPI** | 🟢 Ready | `:8080/swagger-ui.html` | [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html) |
| **Agent REST Query** | 🟢 Ready | `:8080/api/v1/query` | `POST /api/v1/query` |
| **Real-time SSE Stream** | 🟢 Ready | `:8080/api/v1/query/stream` | `POST /api/v1/query/stream` |
| **Embedded MCP Server** | 🟢 Active | `:8080` (SSE / JSON-RPC) | [http://localhost:8080/sse](http://localhost:8080/sse) / `POST /messages` |
| **Kafka Broker** | 🟢 Healthy | `:9092` | KRaft Cluster (Topic: `system-logs`) |
| **Apache Lucene 9** | 🟢 Ready | In-Process JVM | Dense Vector Store (`./data/lucene-index`) |
| **Ollama LLM** | 🟢 Ready | `:11434` | Model `llama3.2:3b` active |
| **Datafaker Producer** | 🟢 Emitting | In-Process Bean | Streaming synthetic logs to Kafka |
| **Ingestion Consumer** | 🟢 Ingesting | In-Process Bean | Embedding via ONNX & indexing into Lucene |

### 📊 Data Pipeline Verification
- **Kafka Stream**: Confirmed messages published to topic `system-logs` keyed by `serviceName`.
- **In-Process ONNX Embeddings**: Generated 384-dimensional dense vectors with `All-MiniLM-L6-v2` in JVM.
- **Embedded Lucene Index**: Vectors indexed in real-time with HNSW cosine similarity + `LongPoint` timestamp range index.

### 🚀 Immediate Next Steps for Testing
1. **Interactive Swagger UI**: Open `http://localhost:8080/swagger-ui.html` in your browser.
2. **Instant Semantic Test Query**:
   ```bash
   curl -s -X POST http://localhost:8080/api/v1/query \
     -H "Content-Type: application/json" \
     -d '{"prompt": "Investigate why checkout is failing and analyze errors in payment-gateway", "k": 5}'
   ```
3. **Instant Token-by-Token Streaming Query**:
   ```bash
   curl -N -X POST http://localhost:8080/api/v1/query/stream \
     -H "Content-Type: application/json" \
     -H "Accept: text/event-stream" \
     -d '{"prompt": "Summarize recent error trends across services", "k": 3}'
   ```
4. **Model Context Protocol (MCP) Inspection**: Connect Cursor IDE or Claude Desktop to `http://localhost:8080/sse`.
5. **Shutdown**: Run `docker compose down -v` (Option A) or stop Spring Boot and run `docker compose -f docker-compose.infra.yml down` (Option B).
```

---

## 7. Testing & Validation Playbook

Guide the user through these test scenarios to demonstrate system capabilities:

### Test 1: Liveness Health Check & OpenAPI Specs
```bash
# cURL
curl -X GET http://localhost:8080/health
curl -X GET http://localhost:8080/v3/api-docs

# PowerShell
Invoke-RestMethod -Uri "http://localhost:8080/health" -Method Get
```
**Expected Response:** `{"status": "ok"}`

### Test 2: Semantic Root-Cause Query (POST `/api/v1/query`)
Ask a question that forces the LangChain4j agent to trigger the `vectorSearch` tool over Apache Lucene:
```bash
# cURL
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Investigate why checkout is failing and analyze errors in payment-gateway or inventory-service",
    "k": 5
  }'
```
```powershell
# PowerShell
$body = @{
    prompt = "Investigate why checkout is failing and analyze errors in payment-gateway or inventory-service"
    k = 5
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8080/api/v1/query" `
  -Method Post `
  -ContentType "application/json" `
  -Body $body | ConvertTo-Json -Depth 5
```
**Evaluation Check:**
- Verify that `answer` contains a natural-language diagnostic report (Executive Summary, Timeline, Root Cause, Actionable Remediation).
- Verify that `sources` contains citations with `serviceName`, `logLevel`, `timestamp`, `score`, and `excerpt`.

### Test 3: Statistical Query (Sliding Time Window Tool Call)
Ask a question that triggers the `logStats` tool (backed by Lucene `LongPoint` sliding time-window queries):
```bash
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "How many errors were logged across all services in the last 15 minutes?",
    "k": 5
  }'
```
**Evaluation Check:**
- Verify that the agent calls `logStats` and summarizes the error count and distribution across microservices within the requested 15-minute window.

### Test 4: Real-Time SSE Token Streaming (POST `/api/v1/query/stream`)
Test low-latency token-by-token streaming powered by Virtual Threads:
```bash
curl -N -X POST http://localhost:8080/api/v1/query/stream \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "prompt": "Provide a quick root cause summary of recent database errors",
    "k": 3
  }'
```
**Evaluation Check:**
- Tokens stream incrementally as `data: <token>` and cleanly terminate with `data: [DONE]`.

### Test 5: Querying via Model Context Protocol (MCP)
The application embeds an official Model Context Protocol (MCP) server (JSON-RPC 2.0 / Protocol `2024-11-05`):

#### 5.1 Initialize MCP Handshake
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

#### 5.2 Discover Available Tools (`tools/list`)
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
Returns JSON schemas for `vector_search` (backed by Apache Lucene) and `log_stats`.

#### 5.3 Execute `vector_search` Tool via MCP
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

#### 5.4 Execute `log_stats` Tool via MCP
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

### Test 6: Cursor IDE & Claude Desktop Integration

#### Cursor IDE Setup
1. In Cursor, open **Settings** (`Ctrl + ,` or `Cmd + ,`).
2. Select **Features** -> **MCP Servers**.
3. Click **Add New MCP Server**:
   - **Name:** `streaming-rag-java`
   - **Type:** `sse`
   - **URL:** `http://localhost:8080/sse`
4. Confirm green status lights for `vector_search` and `log_stats`.
5. In Cursor Agent Chat, type:
   > *"@streaming-rag-java find recent ERROR logs in user-auth and tell me if there was an LDAP timeout."*

#### Claude Desktop Setup
In Claude Desktop configuration (`%APPDATA%\Claude\claude_desktop_config.json` on Windows):
```json
{
  "mcpServers": {
    "streaming-rag-java": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

### Test 7: Automated Test Suite & Quality Gates
Run the full automated test suite (covering unit logic, in-process ONNX embeddings, embedded Lucene indexing + sliding time-window queries, OpenAPI config, REST controllers, MCP server & dynamic MCP client, and LLM observability listener):
```bash
mvn clean test
```
**Expected Result:**
```text
[INFO] Results:
[INFO] Tests run: ..., Failures: 0, Errors: 0
[INFO] BUILD SUCCESS
```
*(1 test is skipped automatically if Docker Desktop is offline on your host, ensuring smooth local compilation without external container dependencies).*

---

## 8. Agent Troubleshooting Matrix

| Symptom | Root Cause | Solution for the Agent |
|---|---|---|
| `UnsupportedClassVersionError` or `Java version mismatch` | System is using Java < 21 | Ensure Java 21+ is active. On Windows PowerShell: `$env:JAVA_HOME = "C:\path\to\jdk-21"` |
| `Connection refused: localhost:9092` | Kafka broker is not up or still initializing KRaft | Check `docker ps`. Verify `streaming-rag-kafka` container is healthy or run `docker compose -f docker-compose.infra.yml up -d` |
| `Ollama connection error` or timeout (`port 11434`) | Ollama daemon not running or blocked | In Option A: check `docker compose logs ollama`. In Option B: run `ollama serve` or check `ollama list`. Verify model `llama3.2:3b` is pulled |
| `Model llama3.2:3b not found` in Option A | Ollama container initialized without model pull | Run `docker exec streaming-rag-ollama ollama pull llama3.2:3b` |
| `LockObtainFailedException: Lock held by another program` | Previous application instance holds Lucene lock | Terminate previous Java process or check for orphan locks in `./data/lucene-index/write.lock` |
| `Port 8080 already in use` | Another instance of Spring Boot or another service running on 8080 | On Windows: `netstat -ano \| findstr :8080`, kill PID with `Stop-Process -Id <PID> -Force`. On Linux: `lsof -i :8080` |
| `PowerShell curl parameter error` | PowerShell aliases `curl` to `Invoke-WebRequest` | Call `curl.exe` directly or use `Invoke-RestMethod` as documented |
| `POST /api/v1/query` takes 30–90s or appears frozen | Ollama runs on CPU; model cold start loading into RAM; ReAct agent runs multi-pass reasoning (tool calling + synthesis) | Inform user upfront of expected latency (~3.5 words/s on CPU). Recommend using the streaming endpoint (`/api/v1/query/stream` with `curl -N`) or lowering `k` (e.g. 2–3) |

---

## 9. Rules for Modifying Code

When assisting the user with code modifications:
1. **Preserve Data Contract**: Any changes to log fields must be updated in `com.streamingrag.model.SystemLog` and mirrored in `docs/data-contract.md`. Key partitioning by `serviceName` must be maintained.
2. **Virtual Threads Compliance**: Concurrency uses Java 21 Virtual Threads (`spring.threads.virtual.enabled: true`). Never introduce thread-pinning blocking synchronized blocks on virtual thread carrier threads.
3. **In-Process Vector Store Integrity**: Maintain embedded Apache Lucene vector indexing (`LuceneEmbeddingStore`) and in-process ONNX embeddings (`All-MiniLM-L6-v2`). Do not re-introduce external vector database containers.
4. **Strict Test Coverage**: Every modification to business logic, controllers, or tools must pass the test suite: run `mvn clean test` and verify that the test suite compiles and all tests pass.
5. **Manual Kafka Offset Commits**: Offset management in `LogIngestionConsumer` must commit offsets (`AckMode.MANUAL_IMMEDIATE`) *only after* vectors are durably indexed into Lucene to maintain at-least-once processing guarantees.
