package com.streamingrag.agent;

import com.streamingrag.api.dto.LogSource;
import com.streamingrag.consumer.LogIngestionConsumer;
import com.streamingrag.model.LogLevel;
import com.streamingrag.vector.VectorStoreRepository;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SreToolsTest {

    @Mock
    private VectorStoreRepository vectorStoreRepository;

    @Mock
    private LogIngestionConsumer logIngestionConsumer;

    private SreTools sreTools;

    @BeforeEach
    void setUp() {
        sreTools = new SreTools(vectorStoreRepository, logIngestionConsumer);
        sreTools.clearCapturedSources();
    }

    @Test
    @DisplayName("Should execute vectorSearch and format results with citations")
    void shouldExecuteVectorSearchWithMatches() {
        UUID eventId = UUID.randomUUID();
        Metadata metadata = Metadata.from(Map.of(
                "event_id", eventId.toString(),
                "service_name", "user-auth",
                "log_level", "ERROR",
                "timestamp", "2026-10-12T14:00:00Z"
        ));
        TextSegment segment = TextSegment.from("LDAP authentication timeout contacting domain controller", metadata);
        EmbeddingMatch<TextSegment> match = new EmbeddingMatch<>(0.89, "id-1", null, segment);

        when(vectorStoreRepository.search(eq("ldap timeout"), eq(5), anyDouble()))
                .thenReturn(List.of(match));

        String result = sreTools.vectorSearch("ldap timeout", 5);

        assertThat(result).contains("Found 1 matching log entries");
        assertThat(result).contains("user-auth");
        assertThat(result).contains("ERROR");
        assertThat(result).contains("LDAP authentication timeout");
        assertThat(result).contains(eventId.toString());

        List<LogSource> captured = sreTools.getCapturedSources();
        assertThat(captured).hasSize(1);
        assertThat(captured.get(0).eventId()).isEqualTo(eventId);
        assertThat(captured.get(0).serviceName()).isEqualTo("user-auth");
        assertThat(captured.get(0).score()).isEqualTo(0.89);
    }

    @Test
    @DisplayName("Should return friendly message when vectorSearch has no matches")
    void shouldHandleEmptyVectorSearch() {
        when(vectorStoreRepository.search(anyString(), anyInt(), anyDouble()))
                .thenReturn(List.of());

        String result = sreTools.vectorSearch("unknown error", 3);

        assertThat(result).contains("No matching log entries found in Apache Lucene");
        assertThat(sreTools.getCapturedSources()).isEmpty();
    }

    @Test
    @DisplayName("Should execute logStats and summarize ingestion metrics")
    void shouldExecuteLogStats() {
        when(logIngestionConsumer.getTotalIngestedCount()).thenReturn(150L);
        when(logIngestionConsumer.getCountForLevel(LogLevel.ERROR)).thenReturn(15L);
        when(logIngestionConsumer.getCountForLevel(LogLevel.WARN)).thenReturn(30L);
        when(logIngestionConsumer.getCountForLevel(LogLevel.INFO)).thenReturn(105L);
        when(logIngestionConsumer.getCountForService("payment-gateway")).thenReturn(40L);

        String result = sreTools.logStats("ALL", "ALL", 15);

        assertThat(result).contains("Total Ingested Logs: 150");
        assertThat(result).contains("ERROR: 15");
        assertThat(result).contains("payment-gateway: 40");
    }
}
