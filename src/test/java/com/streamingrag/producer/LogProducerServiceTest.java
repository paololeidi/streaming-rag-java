package com.streamingrag.producer;

import com.streamingrag.config.AppProperties;
import com.streamingrag.model.LogLevel;
import com.streamingrag.model.SystemLog;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LogProducerServiceTest {

    @Mock
    private KafkaTemplate<String, SystemLog> kafkaTemplate;

    @Mock
    private MockLogGenerator mockLogGenerator;

    private AppProperties appProperties;
    private LogProducerService producerService;

    @BeforeEach
    void setUp() {
        appProperties = new AppProperties(
                new AppProperties.KafkaProperties(
                        "system-logs",
                        "system-logs-dlq",
                        "test-group",
                        "localhost:9092",
                        4,
                        (short) 1
                ),
                new AppProperties.ProducerProperties(true, 1000),
                new AppProperties.LuceneProperties("./data/lucene-index", false, 10),
                new AppProperties.OllamaProperties("http://localhost:11434", "llama3.2:3b", 0.0, 60)
        );

        producerService = new LogProducerService(kafkaTemplate, mockLogGenerator, appProperties);
    }

    @Test
    @DisplayName("Should send log to Kafka keyed by serviceName")
    void shouldSendLogKeyedByServiceName() {
        SystemLog log = new SystemLog(
                UUID.randomUUID(),
                Instant.now(),
                "inventory-service",
                LogLevel.INFO,
                "Stock updated",
                null
        );

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("system-logs", 1), 0L, 0, 0L, 0, 0
        );
        SendResult<String, SystemLog> sendResult = new SendResult<>(null, metadata);
        CompletableFuture<SendResult<String, SystemLog>> future = CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(eq("system-logs"), eq("inventory-service"), eq(log)))
                .thenReturn(future);

        CompletableFuture<SendResult<String, SystemLog>> resultFuture = producerService.sendLog(log);

        assertThat(resultFuture).isNotNull();
        verify(kafkaTemplate, times(1)).send("system-logs", "inventory-service", log);
        assertThat(producerService.getProducedCount()).isEqualTo(1L);
    }

    @Test
    @DisplayName("Should generate and send log using MockLogGenerator")
    void shouldGenerateAndSendLog() {
        SystemLog generatedLog = new SystemLog(
                UUID.randomUUID(),
                Instant.now(),
                "user-auth",
                LogLevel.INFO,
                "User login",
                null
        );

        when(mockLogGenerator.generateLog()).thenReturn(generatedLog);

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("system-logs", 0), 0L, 0, 0L, 0, 0
        );
        CompletableFuture<SendResult<String, SystemLog>> future =
                CompletableFuture.completedFuture(new SendResult<>(null, metadata));
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(future);

        SystemLog result = producerService.generateAndSend();

        assertThat(result).isEqualTo(generatedLog);
        verify(kafkaTemplate).send("system-logs", "user-auth", generatedLog);
    }

    @Test
    @DisplayName("Should generate and send burst of logs")
    void shouldGenerateAndSendBurst() {
        SystemLog sampleLog = new SystemLog(
                UUID.randomUUID(),
                Instant.now(),
                "frontend-bff",
                LogLevel.INFO,
                "GET /status",
                null
        );

        when(mockLogGenerator.generateLog()).thenReturn(sampleLog);

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("system-logs", 2), 0L, 0, 0L, 0, 0
        );
        CompletableFuture<SendResult<String, SystemLog>> future =
                CompletableFuture.completedFuture(new SendResult<>(null, metadata));
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(future);

        List<SystemLog> logs = producerService.generateAndSendBurst(3);

        assertThat(logs).hasSize(3);
        verify(kafkaTemplate, times(3)).send("system-logs", "frontend-bff", sampleLog);
    }
}
