package com.streamingrag.vector;

import com.streamingrag.model.LogLevel;
import com.streamingrag.model.SystemLog;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class VectorStoreRepositoryTest {

    @Mock
    private EmbeddingStore<TextSegment> embeddingStore;

    @Mock
    private EmbeddingService embeddingService;

    private VectorStoreRepository repository;

    @BeforeEach
    void setUp() {
        repository = new VectorStoreRepository(embeddingStore, embeddingService);
    }

    @Test
    @DisplayName("Should store log event by generating segment, embedding, and saving in store")
    void shouldStoreLogEvent() {
        SystemLog log = new SystemLog(
                UUID.randomUUID(),
                Instant.now(),
                "frontend-bff",
                LogLevel.INFO,
                "Request handled",
                null
        );

        TextSegment segment = TextSegment.from("text");
        Embedding embedding = Embedding.from(new float[]{0.1f, 0.2f});

        when(embeddingService.toTextSegment(log, 0, 10L)).thenReturn(segment);
        when(embeddingService.embed(segment)).thenReturn(embedding);
        when(embeddingStore.add(embedding, segment)).thenReturn("doc-id-123");

        String resultId = repository.store(log, 0, 10L);

        assertThat(resultId).isEqualTo("doc-id-123");
        verify(embeddingStore, times(1)).add(embedding, segment);
    }

    @Test
    @DisplayName("Should search vector store using query embedding")
    void shouldSearchVectorStore() {
        Embedding queryEmbedding = Embedding.from(new float[]{0.1f, 0.2f});
        TextSegment matchSegment = TextSegment.from("match");
        EmbeddingMatch<TextSegment> match = new EmbeddingMatch<>(0.92, "doc-id-123", queryEmbedding, matchSegment);
        EmbeddingSearchResult<TextSegment> searchResult = new EmbeddingSearchResult<>(List.of(match));

        when(embeddingService.embedText("database timeout")).thenReturn(queryEmbedding);
        when(embeddingStore.search(any(EmbeddingSearchRequest.class))).thenReturn(searchResult);

        List<EmbeddingMatch<TextSegment>> results = repository.search("database timeout", 5, 0.7);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).score()).isEqualTo(0.92);
        assertThat(results.get(0).embedded().text()).isEqualTo("match");
    }
}
