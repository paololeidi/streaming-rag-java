package com.streamingrag.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI 3.0 and Swagger UI configuration for the Enterprise Streaming RAG API.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI streamingRagOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Enterprise Streaming RAG in Java API")
                        .version("1.0.0")
                        .description("Event-driven Agentic RAG platform for real-time log stream ingestion, "
                                + "in-process ONNX vectorization, Apache Lucene HNSW k-NN search, "
                                + "and autonomous incident Root Cause Analysis (RCA) with Model Context Protocol (MCP).")
                        .contact(new Contact()
                                .name("Enterprise Streaming RAG Team")
                                .url("https://github.com/paololeidi/streaming-rag-java"))
                        .license(new License()
                                .name("MIT License")
                                .url("https://opensource.org/licenses/MIT")));
    }
}
