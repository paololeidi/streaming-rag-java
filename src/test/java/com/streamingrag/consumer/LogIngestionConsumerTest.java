package com.streamingrag.consumer;

import com.streamingrag.model.LogLevel;
import com.streamingrag.model.SystemLog;
import com.streamingrag.vector.VectorStoreRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LogIngestionConsumerTest {

    @Mock
    private VectorStoreRepository vectorStoreRepository;

    @Mock
    private Acknowledgment acknowledgment;

    private LogIngestionConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new LogIngestionConsumer(vectorStoreRepository);
        consumer.resetMetrics();
    }

    @Test
    @DisplayName("Should successfully consume SystemLog, store in vector repository, increment metrics and acknowledge")
    void shouldConsumeStoreAndAcknowledge() {
        SystemLog log = new SystemLog(
                UUID.randomUUID(),
                Instant.now(),
                "payment-gateway",
                LogLevel.INFO,
                "Payment received",
                null
        );

        ConsumerRecord<String, SystemLog> record = new ConsumerRecord<>(
                "system-logs", 0, 100L, "payment-gateway", log
        );

        when(vectorStoreRepository.store(eq(log), eq(0), eq(100L))).thenReturn("lucene-id-123");

        consumer.consume(record, acknowledgment);

        assertThat(consumer.getTotalIngestedCount()).isEqualTo(1L);
        assertThat(consumer.getCountForService("payment-gateway")).isEqualTo(1L);
        assertThat(consumer.getCountForLevel(LogLevel.INFO)).isEqualTo(1L);
        verify(vectorStoreRepository, times(1)).store(eq(log), eq(0), eq(100L));
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Should acknowledge and not fail when record value is null")
    void shouldHandleNullRecordValue() {
        ConsumerRecord<String, SystemLog> record = new ConsumerRecord<>(
                "system-logs", 0, 101L, "payment-gateway", null
        );

        consumer.consume(record, acknowledgment);

        assertThat(consumer.getTotalIngestedCount()).isEqualTo(0L);
        verify(vectorStoreRepository, never()).store(any(), anyInt(), anyLong());
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Should not acknowledge and rethrow when vector storage fails")
    void shouldNotAcknowledgeWhenVectorStoreFails() {
        SystemLog log = new SystemLog(
                UUID.randomUUID(),
                Instant.now(),
                "user-auth",
                LogLevel.ERROR,
                "Auth failed",
                "stacktrace"
        );

        ConsumerRecord<String, SystemLog> record = new ConsumerRecord<>(
                "system-logs", 1, 200L, "user-auth", log
        );

        when(vectorStoreRepository.store(any(), anyInt(), anyLong()))
                .thenThrow(new RuntimeException("Lucene vector indexing failed"));

        assertThatThrownBy(() -> consumer.consume(record, acknowledgment))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Lucene vector indexing failed");

        verify(acknowledgment, never()).acknowledge();
        assertThat(consumer.getTotalIngestedCount()).isEqualTo(0L);
    }
}
