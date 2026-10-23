package com.streamingrag.vector;

import com.streamingrag.model.SystemLog;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Repository wrapping the LangChain4j LuceneEmbeddingStore for vector indexing, semantic retrieval,
 * and sliding time-window metadata aggregation.
 */
@Repository
public class VectorStoreRepository {

    private static final Logger log = LoggerFactory.getLogger(VectorStoreRepository.class);

    private final EmbeddingStore<TextSegment> embeddingStore;
    private final EmbeddingService embeddingService;

    public VectorStoreRepository(
            EmbeddingStore<TextSegment> embeddingStore,
            EmbeddingService embeddingService) {
        this.embeddingStore = embeddingStore;
        this.embeddingService = embeddingService;
    }

    /**
     * Vectorizes and stores a SystemLog in Apache Lucene vector index.
     *
     * @param systemLog the log to store
     * @param partition partition index
     * @param offset    record offset
     * @return the generated embedding store ID
     */
    public String store(SystemLog systemLog, int partition, long offset) {
        TextSegment segment = embeddingService.toTextSegment(systemLog, partition, offset);
        Embedding embedding = embeddingService.embed(segment);
        String id = embeddingStore.add(embedding, segment);

        log.debug("[VECTOR STORE] Stored log eventId={} in Apache Lucene with id={}", systemLog.eventId(), id);
        return id;
    }

    /**
     * Performs cosine similarity search over stored log embeddings.
     *
     * @param query      natural language or error query
     * @param maxResults maximum matches to return
     * @param minScore   minimum similarity threshold (0.0 - 1.0)
     * @return list of matched text segments with similarity scores
     */
    public List<EmbeddingMatch<TextSegment>> search(String query, int maxResults, double minScore) {
        Embedding queryEmbedding = embeddingService.embedText(query);
        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(queryEmbedding)
                .maxResults(maxResults)
                .minScore(minScore)
                .build();
        EmbeddingSearchResult<TextSegment> result = embeddingStore.search(request);
        return result.matches();
    }

    /**
     * Retrieves log metadata entries stored since the specified UTC cutoff timestamp,
     * optionally filtered by severity level and microservice name.
     *
     * @param cutoff      sliding UTC cutoff timestamp
     * @param logLevel    optional log level filter ('INFO', 'WARN', 'ERROR', or 'ALL')
     * @param serviceName optional microservice name filter or 'ALL'
     * @return list of matching {@link Metadata} records
     */
    public List<Metadata> findMetadataSince(Instant cutoff, String logLevel, String serviceName) {
        if (embeddingStore instanceof LuceneEmbeddingStore luceneStore) {
            return luceneStore.findMetadataSince(cutoff, logLevel, serviceName);
        }
        return Collections.emptyList();
    }
}
