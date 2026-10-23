package com.streamingrag.config;

import com.streamingrag.api.HealthController;
import com.streamingrag.api.QueryController;
import com.streamingrag.api.dto.LogSource;
import com.streamingrag.api.dto.QueryRequest;
import com.streamingrag.api.dto.QueryResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.models.OpenAPI;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class OpenApiConfigTest {

    @Test
    @DisplayName("Should create OpenAPI specification with title, version, description, contact and license")
    void shouldConfigureOpenApiBean() {
        OpenApiConfig config = new OpenApiConfig();
        OpenAPI openAPI = config.streamingRagOpenAPI();

        assertThat(openAPI).isNotNull();
        assertThat(openAPI.getInfo()).isNotNull();
        assertThat(openAPI.getInfo().getTitle()).isEqualTo("Enterprise Streaming RAG in Java API");
        assertThat(openAPI.getInfo().getVersion()).isEqualTo("1.0.0");
        assertThat(openAPI.getInfo().getDescription()).contains("Apache Lucene HNSW");
        assertThat(openAPI.getInfo().getContact()).isNotNull();
        assertThat(openAPI.getInfo().getLicense().getName()).isEqualTo("MIT License");
    }

    @Test
    @DisplayName("Should annotate controllers and DTO records with OpenAPI annotations")
    void shouldAnnotateControllersAndDtos() throws NoSuchMethodException {
        assertThat(QueryController.class.isAnnotationPresent(Tag.class)).isTrue();
        assertThat(HealthController.class.isAnnotationPresent(Tag.class)).isTrue();

        Method queryMethod = QueryController.class.getMethod("query", QueryRequest.class);
        Method streamMethod = QueryController.class.getMethod("streamQuery", QueryRequest.class);
        Method healthMethod = HealthController.class.getMethod("health");

        assertThat(queryMethod.isAnnotationPresent(Operation.class)).isTrue();
        assertThat(streamMethod.isAnnotationPresent(Operation.class)).isTrue();
        assertThat(healthMethod.isAnnotationPresent(Operation.class)).isTrue();

        assertThat(QueryRequest.class.isAnnotationPresent(Schema.class)).isTrue();
        assertThat(QueryResponse.class.isAnnotationPresent(Schema.class)).isTrue();
        assertThat(LogSource.class.isAnnotationPresent(Schema.class)).isTrue();
    }
}
