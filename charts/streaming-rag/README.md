# streaming-rag-java Helm Chart

Deploys the full **Enterprise Streaming RAG in Java** stack to Kubernetes:
- **Apache Kafka** (single-node KRaft mode)
- **Ollama** (+ `ollama-init` Job to pull `llama3.2:3b`)
- **Spring Boot 3.3 Application** (embedding in-process Datafaker Producer, Kafka Consumer, ONNX `all-MiniLM-L6-v2` Embedder, Apache Lucene 9 HNSW Vector Store, REST API + Swagger UI, and MCP Server)

> 💡 Because Apache Lucene 9 and ONNX Runtime execute 100% in-process inside the JVM, no external ChromaDB container is needed in the Kubernetes cluster.

## Prerequisites

- Docker
- [Kind](https://kind.sigs.k8s.io/) (local Kubernetes validation target)
- `kubectl`
- Helm 3

Host RAM: prefer **~8–16 GB+** for Kind + Ollama + Kafka + JVM.

## Kind: Build, Load, Install

From the **project root**:

```bash
# 1. Cluster (skip if you already have one)
kind create cluster --name streaming-rag-java

# 2. Build the app image and load it into Kind
docker build -t streaming-rag-java:local .
kind load docker-image streaming-rag-java:local --name streaming-rag-java

# 3. Install / upgrade
helm upgrade --install streaming-rag ./charts/streaming-rag \
  -n streaming-rag --create-namespace

# 4. Watch rollout (ollama-init Job pulls the model)
kubectl get pods,jobs -n streaming-rag -w
```

Or use the automated helper script:

```bash
./scripts/kind-deploy.sh
```

## Validate

```bash
kubectl port-forward -n streaming-rag svc/streaming-rag 8080:8080

# Health check
curl http://localhost:8080/health

# OpenAPI / Swagger UI
# Open http://localhost:8080/swagger-ui.html in your browser

# Agentic RAG query
curl -s -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"prompt": "what errors occurred recently?", "k": 5}'

# MCP SSE (Cursor IDE / Claude Desktop)
# URL: http://localhost:8080/sse
```

Check ingestion logs:

```bash
kubectl logs -n streaming-rag deploy/streaming-rag --tail=50
```

## Useful Values

| Key | Default | Notes |
| --- | ------- | ----- |
| `image.repository` / `tag` | `streaming-rag-java` / `local` | Must match `kind load` |
| `image.pullPolicy` | `IfNotPresent` | Required for locally loaded Kind images |
| `ollama.model` | `llama3.2:3b` | Pulled by `ollama-init` Job |
| `service.type` | `ClusterIP` | Set `NodePort` if you prefer not to port-forward |
| `config.mcpServerUrl` | `""` | Optional external MCP server URL for dynamic tool discovery |
| `langsmith.enabled` | `false` | Set `true` + `langsmith.apiKey` to enable LangSmith LLM tracing |

Example with LangSmith tracing:

```bash
helm upgrade --install streaming-rag ./charts/streaming-rag \
  -n streaming-rag --create-namespace \
  --set langsmith.enabled=true \
  --set-string langsmith.apiKey=lsv2_...
```

## Uninstall

```bash
helm uninstall streaming-rag -n streaming-rag
kubectl delete namespace streaming-rag
# optional: kind delete cluster --name streaming-rag-java
```
