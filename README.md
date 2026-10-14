# Enterprise Streaming RAG in Java

[![Java](https://img.shields.io/badge/Java-21%2B%20LTS-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.4-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![LangChain4j](https://img.shields.io/badge/LangChain4j-0.35.0-blue.svg)](https://github.com/langchain4j/langchain4j)
[![Apache Lucene](https://img.shields.io/badge/Apache%20Lucene-9.12.0%20(Vectors)-darkblue.svg)](https://lucene.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-red.svg)](https://kafka.apache.org/)
[![MCP](https://img.shields.io/badge/MCP-Protocol%202024--11--05-purple.svg)](https://modelcontextprotocol.io/)

> 💡 **Sister Repository:** This is the Java version of the [`streaming-rag`](https://github.com/paololeidi/streaming-rag) project in Python.

---

## 📌 Project Overview

**Enterprise Streaming RAG in Java** is an event-driven platform designed for real-time log stream processing, instant vectorization, and autonomous incident Root Cause Analysis (RCA).

Unlike conventional RAG setups that depend on batch jobs or external standalone vector databases, this project implements a **complete end-to-end streaming intelligence pipeline natively inside the JVM**:
- **Continuous Stream Ingestion**: Produces and consumes system log events across **Apache Kafka** partitions using `spring-kafka` with manual offset commits and enterprise-grade resilience.
- **In-Process AI Vectorization & Search**: Converts streaming logs into dense semantic embeddings using ONNX Runtime (`all-MiniLM-L6-v2`) in-memory, immediately indexing them in an embedded **Apache Lucene 9** vector store (HNSW k-NN cosine similarity search). Zero Python sidecars, external containers, or external vector network hops required.
- **Agentic RAG & Incident RCA**: An autonomous AI agent built with **LangChain4j** and local **Ollama** (`llama3.2:3b`) dynamically inspects vector store entries and ingestion metrics to correlate errors across microservices, discover root causes, and suggest mitigations.
- **Modern Developer & Agent Interfaces**: Exposes synchronous REST endpoints, Server-Sent Events (SSE) token streaming, and a compliant **Model Context Protocol (MCP)** server for seamless integration with AI environments like Cursor IDE and Claude Desktop.

---

## 🏛️ High-Level Architecture

```
┌──────────────────────────────── Data Pipeline ────────────────────────────────┐
│                                                                               │
│  Datafaker Producer ──► Kafka ──► Consumer ──► ONNX Embedder ──► Apache Lucene│
│                                  (spring-kafka)(All-MiniLM-L6-v2) (In-Process)│
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
┌──────────────────────────────── Agentic Layer ────────────────────────────────┐
│                                                                               │
│  POST /api/v1/query & /query/stream (SSE)                                     │
│         │                                                                     │
│         ▼                                                                     │
│  LangChain4j @AiService (Ollama · llama3.2:3b)                                │
│         ├─── @Tool vectorSearch ──► LuceneEmbeddingStore (k-NN Cosine)        │
│         ├─── @Tool logStats    ──► Ingestion & Severity Distribution          │
│         └─── [MCP Tools]       ──► External MCP Servers                       │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
┌────────────────────────────────── MCP Server ─────────────────────────────────┐
│  Model Context Protocol (JSON-RPC 2.0 / Protocol 2024-11-05)                  │
│  Exposes vector_search & log_stats via SSE (Port 8080/8001) / stdio           │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## 💡 Why Java for Streaming RAG? (Enterprise Rationale)

In enterprise IT landscapes, core transactional and streaming backbones are predominantly engineered in **Java / Spring Boot**. While Python excels at quick research prototypes, operating Python services inside latency-sensitive streaming data pipelines creates notable operational constraints:

1. **Native Kafka Ecosystem:** Apache Kafka was built on the JVM. `spring-kafka` provides first-class partition rebalancing, manual acknowledgment control, zero-copy serialization, and battle-tested fault tolerance without wrapper overhead.
2. **High-Throughput Concurrency (Java 21 Virtual Threads / Project Loom):** Non-blocking stream processing and concurrent LLM token streaming scale across tens of thousands of lightweight virtual threads without Global Interpreter Lock (GIL) bottlenecks or single-threaded event loop complexity.
3. **In-Process AI Vectorization & Storage (Zero Python Sidecar or External Vector DB):** Embeddings (`all-MiniLM-L6-v2`) execute directly in-process via JNI through the native ONNX Runtime, and vectors are indexed in-memory using **Apache Lucene 9 (k-NN HNSW)**. This matches the embedded ergonomics of Python ChromaDB without requiring external sidecars or HTTP serialization penalties.
4. **Enterprise Reality:** Demonstrates that modern agentic RAG and real-time AI workflows can be embedded natively into existing enterprise Java infrastructure without forcing organizations to re-platform to Python in production.

---

## ⚡ Quick start (Recommended: Agent-Assisted)

The easiest and fastest way to set up, launch, and verify the project is to let your **AI Coding Assistant / Agent** (Cursor, Claude Code, Copilot, Antigravity, Aider, etc.) run it autonomously using the **[AGENT_GUIDE.md](AGENT_GUIDE.md)** runbook.

Simply prompt your assistant:
> *"Start the application following the agent guide"*

The agent will ask you whether you prefer **Path A (Full Docker Compose - zero local setup)** or **Path B (Local Java 21 / Ollama hybrid development)**, and will then autonomously verify prerequisites, launch services, seed initial logs, execute health checks and test queries, and deliver working endpoints.

### Prerequisites
Make sure the following tools are available on your system before startup:
- **Docker & Docker Compose** (running daemon, required for containers / Kafka KRaft broker).
- **Ollama** running with the model pulled: `ollama pull llama3.2:3b` (required for local LLM inference in hybrid mode).
- **Java 21 LTS** & **Maven 3.9+** (or use the included `./mvnw` / `mvnw.cmd` wrapper; required only if choosing local hybrid development).

---

## 🛠️ Manual Setup Guide

If you prefer to configure, build, and run the stack manually step-by-step (Docker Compose, individual services, Kafka KRaft, Kubernetes/Helm, or automated test suites):

👉 **[docs/usage-guide.md](docs/usage-guide.md)**

---

## ⚖️ Dual-Implementation Architectural Comparison

| Dimension | Python Implementation ([`streaming-rag`](https://github.com/paololeidi/streaming-rag)) | Java Implementation (`streaming-rag-java`) |
|---|---|---|
| **Runtime & Language** | Python 3.11+ / FastAPI / Uvicorn | Java 21 LTS / Spring Boot 3.3.4 (Virtual Threads) |
| **Concurrency Model** | `asyncio` Single-threaded Event Loop | Lightweight Virtual Threads (Project Loom) |
| **Kafka Integration** | `confluent-kafka` (C-wrapper) | Native `spring-kafka` with KRaft & Manual Acks |
| **Agentic Framework** | LangGraph (StateGraph ReAct) | LangChain4j (`@AiService` + `@Tool`) |
| **Embeddings Execution** | SentenceTransformers (Python process) | **In-Process ONNX Runtime** (inside JVM) |
| **Vector Store** | ChromaDB (In-Process / Embedded in Python) | **Apache Lucene 9 (In-Process / Embedded in Java)** |
| **MCP Integration** | FastMCP Python server | Spring SSE & Stdio JSON-RPC 2.0 MCP Server |
| **Packaging & Deploy** | Docker Compose / Helm / Kind | Multi-Stage Dockerfile / Compose / Helm |

---

## 📚 Project Documentation

All execution instructions, technical specifications, and architectural details are organized in the `docs/` directory:

- 📖 **[Usage & Verification Guide](docs/usage-guide.md)**  
  Step-by-step instructions for running the environment, verifying health, testing synchronous and streaming REST APIs (cURL & PowerShell), executing Model Context Protocol (MCP) tools, and configuring Cursor IDE & Claude Desktop.

- 📐 **[Technical Architecture & System Guide](docs/technical-guide.md)**  
  In-depth technical specification detailing the project purpose, end-to-end data pipeline, in-process ONNX vectorization, embedded Apache Lucene vector search, agentic ReAct loop, guardrails, and enterprise fault tolerance mechanisms.

- 🏛️ **[Architecture Decision Records (ADRs)](docs/architecture-decisions.md)**  
  Formal ADRs covering Java 21 Virtual Threads, LangChain4j, Apache Lucene embedded vector storage, in-process ONNX embeddings, manual Kafka offset commits, and MCP tool boundaries.

- 📋 **[Data Contract Specification](docs/data-contract.md)**  
  Formal schema definitions for `SystemLog` records, partition routing by `serviceName`, serialization formats, and embedding text normalization.


- 🚀 **[Improvements Roadmap](docs/improvements.md)**  
  Strategic production evolution roadmap matching the Python sister project (multi-tenancy, distributed vector scaling, and advanced evaluation).
