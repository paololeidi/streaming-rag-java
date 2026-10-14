# syntax=docker/dockerfile:1

# -------------------------------------------------------------
# Stage 1: Build Application with Maven & Eclipse Temurin JDK 21
# -------------------------------------------------------------
FROM maven:3.9.9-eclipse-temurin-21 AS builder

WORKDIR /build

# Pre-fetch dependencies to leverage Docker layer caching
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source code and build production fat JAR
COPY src ./src
RUN mvn clean package -DskipTests -B

# -------------------------------------------------------------
# Stage 2: Production Runtime with Temurin JRE 21 (glibc for ONNX Runtime)
# -------------------------------------------------------------
FROM eclipse-temurin:21-jre AS runtime

LABEL maintainer="Streaming RAG Team"
LABEL description="Enterprise Streaming RAG in Java (Kafka + LangChain4j + Apache Lucene + MCP)"

WORKDIR /app

# Run as non-privileged user for enterprise container security
RUN groupadd -r appgroup && useradd -r -g appgroup appuser \
    && mkdir -p /app/data && chown -R appuser:appgroup /app
USER appuser:appgroup

# Copy compiled JAR from builder stage
COPY --from=builder --chown=appuser:appgroup /build/target/streaming-rag-java-*.jar app.jar

# Expose HTTP REST / SSE stream / MCP endpoints
EXPOSE 8080

# Configure JVM flags optimized for containers and Virtual Threads
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
