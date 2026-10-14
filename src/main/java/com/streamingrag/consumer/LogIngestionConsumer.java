package com.streamingrag.consumer;

import com.streamingrag.model.LogLevel;
import com.streamingrag.model.SystemLog;
import com.streamingrag.vector.VectorStoreRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enterprise Kafka Consumer ingesting SystemLog messages.
 * Uses manual acknowledgment (MANUAL_IMMEDIATE) to ensure at-least-once processing semantics:
 * messages are acknowledged ONLY after successful embedding and storage in Apache Lucene.
 */
@Service
public class LogIngestionConsumer {

    private static final Logger log = LoggerFactory.getLogger(LogIngestionConsumer.class);

    private final VectorStoreRepository vectorStoreRepository;
    private final AtomicLong totalIngestedCount = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> serviceCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<LogLevel, AtomicLong> levelCounts = new ConcurrentHashMap<>();

    public LogIngestionConsumer(VectorStoreRepository vectorStoreRepository) {
        this.vectorStoreRepository = vectorStoreRepository;
    }

    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${app.kafka.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, SystemLog> record, Acknowledgment acknowledgment) {
        SystemLog systemLog = record.value();

        try {
            if (systemLog == null) {
                log.warn("[CONSUME] Received null payload at partition={} offset={}", record.partition(), record.offset());
                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
                return;
            }

            // Phase 2: In-process ONNX vectorization and persistence to Apache Lucene
            String storeId = vectorStoreRepository.store(systemLog, record.partition(), record.offset());

            // Track ingestion metrics
            totalIngestedCount.incrementAndGet();
            serviceCounts.computeIfAbsent(systemLog.serviceName(), k -> new AtomicLong(0)).incrementAndGet();
            levelCounts.computeIfAbsent(systemLog.logLevel(), k -> new AtomicLong(0)).incrementAndGet();

            log.info("[CONSUME] Indexed log eventId={} service={} level={} partition={} offset={} into vector store id={}",
                    systemLog.eventId(),
                    systemLog.serviceName(),
                    systemLog.logLevel(),
                    record.partition(),
                    record.offset(),
                    storeId);

            // Acknowledge Kafka message only after successful write to Apache Lucene
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } catch (Exception e) {
            log.error("[CONSUME ERROR] Failed to process/index record partition={} offset={}: {}",
                    record.partition(), record.offset(), e.getMessage(), e);
            throw e; // Rethrow to trigger DefaultErrorHandler retry and DLQ publishing
        }
    }

    public long getTotalIngestedCount() {
        return totalIngestedCount.get();
    }

    public long getCountForService(String serviceName) {
        AtomicLong count = serviceCounts.get(serviceName);
        return (count != null) ? count.get() : 0L;
    }

    public long getCountForLevel(LogLevel level) {
        AtomicLong count = levelCounts.get(level);
        return (count != null) ? count.get() : 0L;
    }

    public void resetMetrics() {
        totalIngestedCount.set(0);
        serviceCounts.clear();
        levelCounts.clear();
    }
}
