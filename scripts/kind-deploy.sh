#!/usr/bin/env bash
# Deploy Enterprise Streaming RAG in Java to a Kind cluster (work-laptop validation path).
# Run from the project root: ./scripts/kind-deploy.sh
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-streaming-rag-java}"
RELEASE="${RELEASE:-streaming-rag}"
NAMESPACE="${NAMESPACE:-streaming-rag}"
IMAGE="${IMAGE:-streaming-rag-java:local}"

echo "==> Checking tools"
command -v docker >/dev/null
command -v kind >/dev/null
command -v kubectl >/dev/null
command -v helm >/dev/null

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "==> Creating Kind cluster: $CLUSTER_NAME"
  kind create cluster --name "$CLUSTER_NAME"
else
  echo "==> Kind cluster already exists: $CLUSTER_NAME"
fi

echo "==> Building app image: $IMAGE"
docker build -t "$IMAGE" .

echo "==> Loading image into Kind"
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

echo "==> Helm lint"
helm lint ./charts/streaming-rag

echo "==> Helm upgrade --install (blocks until ollama-init Job finishes pulling the model)"
helm upgrade --install "$RELEASE" ./charts/streaming-rag \
  -n "$NAMESPACE" --create-namespace

echo "==> Waiting for Deployments"
kubectl rollout status deployment/kafka -n "$NAMESPACE" --timeout=180s
kubectl rollout status deployment/ollama -n "$NAMESPACE" --timeout=180s
kubectl rollout status deployment/"$RELEASE" -n "$NAMESPACE" --timeout=300s

echo ""
echo "Done. Port-forward and test:"
echo "  kubectl port-forward -n $NAMESPACE svc/$RELEASE 8080:8080"
echo "  curl http://localhost:8080/health"
echo "  Open Swagger UI: http://localhost:8080/swagger-ui.html"
echo "  MCP SSE endpoint: http://localhost:8080/sse"
echo ""
kubectl get pods,jobs,svc -n "$NAMESPACE"
