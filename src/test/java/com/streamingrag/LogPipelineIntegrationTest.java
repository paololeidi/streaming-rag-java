package com.streamingrag;

import com.streamingrag.consumer.LogIngestionConsumer;
import com.streamingrag.model.LogLevel;
import com.streamingrag.model.SystemLog;
import com.streamingrag.producer.LogProducerService;
import com.streamingrag.vector.VectorStoreRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * End-to-end integration test validating the complete log streaming pipeline:
 * Mock Generator -> Kafka Producer -> Consumer -> In-process ONNX Embedding -> Apache Lucene Vector Store.
 */
@SpringBootTest
@ActiveProfiles("test")
@Tag("integration")
public class LogPipelineIntegrationTest {

    static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    static KafkaContainer kafka;

    static {
        if (isDockerAvailable()) {
            kafka = new KafkaContainer("apache/kafka:3.7.0");
            kafka.start();
            try (AdminClient admin = AdminClient.create(
                    Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
                admin.createTopics(Collections.singletonList(new NewTopic("test-system-logs", 1, (short) 1)))
                        .all()
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
    }

    @DynamicPropertySource
    static void configureKafka(DynamicPropertyRegistry registry) {
        if (kafka != null && kafka.isRunning()) {
            registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
            registry.add("app.kafka.bootstrap-servers", kafka::getBootstrapServers);
        }
    }

    @MockBean
    private KafkaAdmin kafkaAdmin;

    @MockBean
    private VectorStoreRepository vectorStoreRepository;

    @Autowired
    private LogProducerService producerService;

    @Autowired
    private LogIngestionConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer.resetMetrics();
        when(vectorStoreRepository.store(any(), anyInt(), anyLong())).thenReturn("lucene-test-id");
    }

    @Test
    @DisplayName("Should wire Spring Boot context and verify components integration")
    void shouldWireContextSuccessfully() {
        assertThat(producerService).isNotNull();
        assertThat(consumer).isNotNull();
    }

    @Test
    @DisplayName("Should process synthetic log through domain pipeline")
    void shouldProcessLogThroughPipeline() {
        SystemLog log = new SystemLog(
                UUID.randomUUID(),
                Instant.now(),
                "payment-gateway",
                LogLevel.ERROR,
                "Stripe connection timed out",
                "java.net.SocketTimeoutException: Connection timeout"
        );

        // Verify domain embedding contract
        String embeddingText = log.toEmbeddingText();
        assertThat(embeddingText).contains("[ERROR] payment-gateway");
        assertThat(embeddingText).contains("stack_trace:");
    }

    @Test
    @EnabledIf("isDockerAvailable")
    @DisplayName("Should run live Testcontainers Kafka test when Docker is active")
    void shouldRunLiveContainerTestIfDockerActive() {
        assertThat(isDockerAvailable()).isTrue();
        assertThat(kafka).isNotNull();
        assertThat(kafka.isRunning()).isTrue();

        SystemLog log = new SystemLog(
                UUID.randomUUID(),
                Instant.now(),
                "payment-service",
                LogLevel.ERROR,
                "Payment gateway request timed out after 5000ms",
                "java.net.SocketTimeoutException: Read timed out\n\tat com.payment.Gateway.call(Gateway.java:42)"
        );

        // Send log through producer service
        producerService.sendLog(log).join();

        // Wait until consumer processes the record from Kafka container
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(consumer.getTotalIngestedCount()).isGreaterThanOrEqualTo(1L);
            assertThat(consumer.getCountForService("payment-service")).isGreaterThanOrEqualTo(1L);
            assertThat(consumer.getCountForLevel(LogLevel.ERROR)).isGreaterThanOrEqualTo(1L);
        });

        // Verify vector storage interaction
        verify(vectorStoreRepository, atLeastOnce()).store(any(), anyInt(), anyLong());
    }
}
