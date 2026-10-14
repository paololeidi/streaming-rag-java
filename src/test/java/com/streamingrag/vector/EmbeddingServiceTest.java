package com.streamingrag.vector;

import com.streamingrag.model.LogLevel;
import com.streamingrag.model.SystemLog;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EmbeddingServiceTest {

    @Mock
    private EmbeddingModel embeddingModel;

    private EmbeddingService embeddingService;

    @BeforeEach
    void setUp() {
        embeddingService = new EmbeddingService(embeddingModel);
    }

    @Test
    @DisplayName("Should convert SystemLog to TextSegment with metadata")
    void shouldConvertToTextSegmentWithMetadata() {
        UUID eventId = UUID.randomUUID();
        Instant timestamp = Instant.parse("2026-10-12T14:00:00Z");
        SystemLog log = new SystemLog(
                eventId,
                timestamp,
                "inventory-service",
                LogLevel.WARN,
                "Stock low for SKU-1234",
                null
        );

        TextSegment segment = embeddingService.toTextSegment(log, 2, 45L);

        assertThat(segment).isNotNull();
        assertThat(segment.text()).isEqualTo("[WARN] inventory-service @ 2026-10-12T14:00:00Z\nStock low for SKU-1234");
        assertThat(segment.metadata().getString("event_id")).isEqualTo(eventId.toString());
        assertThat(segment.metadata().getString("service_name")).isEqualTo("inventory-service");
        assertThat(segment.metadata().getString("log_level")).isEqualTo("WARN");
        assertThat(segment.metadata().getString("timestamp")).isEqualTo("2026-10-12T14:00:00Z");
        assertThat(segment.metadata().getString("partition")).isEqualTo("2");
        assertThat(segment.metadata().getString("offset")).isEqualTo("45");
    }

    @Test
    @DisplayName("Should generate embedding for TextSegment")
    void shouldEmbedTextSegment() {
        TextSegment segment = TextSegment.from("Sample log text");
        float[] vector = new float[]{0.1f, 0.2f, 0.3f};
        Embedding embedding = Embedding.from(vector);

        when(embeddingModel.embed(any(TextSegment.class))).thenReturn(Response.from(embedding));

        Embedding result = embeddingService.embed(segment);

        assertThat(result).isNotNull();
        assertThat(result.vector()).isEqualTo(vector);
    }

    @Test
    @DisplayName("Should generate embedding for raw text query")
    void shouldEmbedTextQuery() {
        float[] vector = new float[]{0.5f, -0.2f, 0.8f};
        Embedding embedding = Embedding.from(vector);

        when(embeddingModel.embed(anyString())).thenReturn(Response.from(embedding));

        Embedding result = embeddingService.embedText("search query");

        assertThat(result).isNotNull();
        assertThat(result.vector()).isEqualTo(vector);
    }
}
