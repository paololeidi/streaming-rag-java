package com.streamingrag.vector;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LuceneEmbeddingStoreTest {

    private ByteBuffersDirectory directory;
    private LuceneEmbeddingStore store;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws IOException {
        directory = new ByteBuffersDirectory();
        objectMapper = new ObjectMapper();
        store = new LuceneEmbeddingStore(directory, objectMapper);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (store != null) {
            store.close();
        }
    }

    @Test
    @DisplayName("Should index embedding and retrieve nearest neighbors using Lucene KnnFloatVectorQuery")
    void shouldIndexAndSearchVector() {
        Embedding emb1 = Embedding.from(new float[]{1.0f, 0.0f, 0.0f});
        TextSegment seg1 = TextSegment.from("Database connection timeout", Metadata.from(Map.of("service", "db-service")));

        Embedding emb2 = Embedding.from(new float[]{0.0f, 1.0f, 0.0f});
        TextSegment seg2 = TextSegment.from("User authentication successful", Metadata.from(Map.of("service", "auth-service")));

        String id1 = store.add(emb1, seg1);
        String id2 = store.add(emb2, seg2);

        assertThat(id1).isNotNull();
        assertThat(id2).isNotNull();

        // Query vector close to emb1
        Embedding queryEmb = Embedding.from(new float[]{0.95f, 0.05f, 0.0f});
        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(queryEmb)
                .maxResults(2)
                .minScore(0.5)
                .build();

        EmbeddingSearchResult<TextSegment> result = store.search(request);
        List<EmbeddingMatch<TextSegment>> matches = result.matches();

        assertThat(matches).isNotEmpty();
        assertThat(matches.get(0).embeddingId()).isEqualTo(id1);
        assertThat(matches.get(0).embedded().text()).isEqualTo("Database connection timeout");
        assertThat(matches.get(0).embedded().metadata().getString("service")).isEqualTo("db-service");
        assertThat(matches.get(0).score()).isGreaterThan(0.9);
    }

    @Test
    @DisplayName("Should filter metadata by sliding UTC time window, log level, and service name")
    void shouldFilterMetadataBySlidingTimeWindow() {
        Instant now = Instant.now();
        Embedding emb = Embedding.from(new float[]{1.0f, 0.0f});

        // Recent ERROR in payment-gateway (5 minutes ago)
        TextSegment recentError = TextSegment.from("Recent error", Metadata.from(Map.of(
                "service_name", "payment-gateway",
                "log_level", "ERROR",
                "timestamp", now.minus(Duration.ofMinutes(5)).toString()
        )));

        // Recent WARN in user-auth (10 minutes ago)
        TextSegment recentWarn = TextSegment.from("Recent warn", Metadata.from(Map.of(
                "service_name", "user-auth",
                "log_level", "WARN",
                "timestamp", now.minus(Duration.ofMinutes(10)).toString()
        )));

        // Old ERROR in payment-gateway (45 minutes ago)
        TextSegment oldError = TextSegment.from("Old error", Metadata.from(Map.of(
                "service_name", "payment-gateway",
                "log_level", "ERROR",
                "timestamp", now.minus(Duration.ofMinutes(45)).toString()
        )));

        store.add(emb, recentError);
        store.add(emb, recentWarn);
        store.add(emb, oldError);

        // 15-minute window: should return only the 2 recent entries
        Instant cutoff15m = now.minus(Duration.ofMinutes(15));
        List<Metadata> recentAll = store.findMetadataSince(cutoff15m, "ALL", "ALL");
        assertThat(recentAll).hasSize(2);

        // 15-minute window + ERROR filter: should return only recentError
        List<Metadata> recentErrorsOnly = store.findMetadataSince(cutoff15m, "ERROR", "ALL");
        assertThat(recentErrorsOnly).hasSize(1);
        assertThat(recentErrorsOnly.get(0).getString("service_name")).isEqualTo("payment-gateway");

        // 60-minute window + payment-gateway filter: should return both recentError and oldError
        Instant cutoff60m = now.minus(Duration.ofMinutes(60));
        List<Metadata> payment60m = store.findMetadataSince(cutoff60m, "ALL", "payment-gateway");
        assertThat(payment60m).hasSize(2);
    }

    @Test
    @DisplayName("Should batch index and delete documents by id")
    void shouldBatchIndexAndDelete() {
        Embedding emb1 = Embedding.from(new float[]{1.0f, 0.0f});
        TextSegment seg1 = TextSegment.from("Error log 1");

        Embedding emb2 = Embedding.from(new float[]{0.0f, 1.0f});
        TextSegment seg2 = TextSegment.from("Error log 2");

        List<String> ids = store.addAll(List.of(emb1, emb2), List.of(seg1, seg2));
        assertThat(ids).hasSize(2);

        // Delete emb1
        store.remove(ids.get(0));

        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(emb1)
                .maxResults(5)
                .minScore(0.0)
                .build();

        EmbeddingSearchResult<TextSegment> result = store.search(request);
        assertThat(result.matches())
                .extracting(EmbeddingMatch::embeddingId)
                .doesNotContain(ids.get(0));

        // Clear all
        store.removeAll();
        EmbeddingSearchResult<TextSegment> emptyResult = store.search(request);
        assertThat(emptyResult.matches()).isEmpty();
    }
}
