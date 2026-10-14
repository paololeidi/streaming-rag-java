package com.streamingrag.vector;

import com.streamingrag.model.SystemLog;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Service encapsulating vector embedding operations and conversion from SystemLog to LangChain4j TextSegment.
 */
@Service
public class EmbeddingService {

    private final EmbeddingModel embeddingModel;

    public EmbeddingService(EmbeddingModel embeddingModel) {
        this.embeddingModel = embeddingModel;
    }

    /**
     * Converts a SystemLog into a LangChain4j TextSegment enriched with metadata.
     *
     * @param log       the domain log event
     * @param partition Kafka partition where the event was received
     * @param offset    Kafka offset of the event
     * @return TextSegment ready for vectorization and indexing
     */
    public TextSegment toTextSegment(SystemLog log, int partition, long offset) {
        Map<String, Object> metadataMap = new HashMap<>();
        metadataMap.put("event_id", log.eventId().toString());
        metadataMap.put("service_name", log.serviceName());
        metadataMap.put("log_level", log.logLevel().name());
        metadataMap.put("timestamp", log.timestamp().toString());
        metadataMap.put("partition", String.valueOf(partition));
        metadataMap.put("offset", String.valueOf(offset));

        return TextSegment.from(log.toEmbeddingText(), Metadata.from(metadataMap));
    }

    /**
     * Generates a vector embedding for a TextSegment using the in-process ONNX model.
     *
     * @param segment TextSegment containing text and metadata
     * @return dense vector embedding
     */
    public Embedding embed(TextSegment segment) {
        Response<Embedding> response = embeddingModel.embed(segment);
        return response.content();
    }

    /**
     * Generates a vector embedding for a raw query string.
     *
     * @param text raw search text
     * @return dense vector embedding
     */
    public Embedding embedText(String text) {
        Response<Embedding> response = embeddingModel.embed(text);
        return response.content();
    }
}
