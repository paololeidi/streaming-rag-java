package com.streamingrag.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Strongly typed application configuration properties.
 */
@ConfigurationProperties(prefix = "app")
public record AppProperties(
    KafkaProperties kafka,
    ProducerProperties producer,
    LuceneProperties lucene,
    OllamaProperties ollama
) {

    public record KafkaProperties(
        @DefaultValue("system-logs") String topic,
        @DefaultValue("system-logs-dlq") String dlqTopic,
        @DefaultValue("streaming-rag-consumer-group") String groupId,
        @DefaultValue("localhost:9092") String bootstrapServers,
        @DefaultValue("4") int partitions,
        @DefaultValue("1") short replicationFactor
    ) {}

    public record ProducerProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("1000") long fixedRateMs
    ) {}

    public record LuceneProperties(
        @DefaultValue("./data/lucene-index") String indexPath,
        @DefaultValue("false") boolean inMemory,
        @DefaultValue("10") int timeoutSeconds
    ) {}

    public record OllamaProperties(
        @DefaultValue("http://localhost:11434") String baseUrl,
        @DefaultValue("llama3.2:3b") String modelName,
        @DefaultValue("0.0") double temperature,
        @DefaultValue("60") int timeoutSeconds
    ) {}
}
