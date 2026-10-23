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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
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
    @DisplayName("Should execute logStats over sliding time window from Lucene metadata")
    void shouldExecuteWindowedLogStatsFromLucene() {
        Instant now = Instant.now();
        Metadata m1 = Metadata.from(Map.of(
                "service_name", "payment-gateway",
                "log_level", "ERROR",
                "timestamp", now.minusSeconds(60).toString()
        ));
        Metadata m2 = Metadata.from(Map.of(
                "service_name", "payment-gateway",
                "log_level", "WARN",
                "timestamp", now.minusSeconds(120).toString()
        ));
        Metadata m3 = Metadata.from(Map.of(
                "service_name", "user-auth",
                "log_level", "ERROR",
                "timestamp", now.minusSeconds(180).toString()
        ));

        when(vectorStoreRepository.findMetadataSince(any(Instant.class), eq("ALL"), eq("ALL")))
                .thenReturn(List.of(m1, m2, m3));

        Instant beforeCall = Instant.now();
        String result = sreTools.logStats("ALL", "ALL", 15);

        ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(vectorStoreRepository).findMetadataSince(cutoffCaptor.capture(), eq("ALL"), eq("ALL"));

        Instant expectedCutoff = beforeCall.minus(Duration.ofMinutes(15));
        assertThat(cutoffCaptor.getValue()).isBetween(expectedCutoff.minusSeconds(5), expectedCutoff.plusSeconds(5));

        assertThat(result).contains("Window: 15 min");
        assertThat(result).contains("Total Ingested Logs: 3");
        assertThat(result).contains("ERROR: 2");
        assertThat(result).contains("WARN: 1");
        assertThat(result).contains("INFO: 0");
        assertThat(result).contains("payment-gateway: 2");
        assertThat(result).contains("user-auth: 1");
    }

    @Test
    @DisplayName("Should execute logStats with fallback to consumer metrics when vector store window is empty")
    void shouldExecuteLogStats() {
        when(vectorStoreRepository.findMetadataSince(any(Instant.class), anyString(), anyString()))
                .thenReturn(List.of());
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
