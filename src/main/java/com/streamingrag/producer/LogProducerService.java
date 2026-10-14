package com.streamingrag.producer;

import com.streamingrag.config.AppProperties;
import com.streamingrag.model.SystemLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service responsible for producing structured SystemLog events to Kafka,
 * keyed by serviceName to guarantee partition-level ordering.
 */
@Service
public class LogProducerService {

    private static final Logger log = LoggerFactory.getLogger(LogProducerService.class);

    private final KafkaTemplate<String, SystemLog> kafkaTemplate;
    private final MockLogGenerator mockLogGenerator;
    private final AppProperties appProperties;
    private final AtomicLong producedCount = new AtomicLong(0);

    public LogProducerService(
            KafkaTemplate<String, SystemLog> kafkaTemplate,
            MockLogGenerator mockLogGenerator,
            AppProperties appProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.mockLogGenerator = mockLogGenerator;
        this.appProperties = appProperties;
    }

    /**
     * Publishes a SystemLog record to Kafka, using serviceName as the message key.
     *
     * @param systemLog the log event to send
     * @return CompletableFuture resolving with the SendResult
     */
    public CompletableFuture<SendResult<String, SystemLog>> sendLog(SystemLog systemLog) {
        String topic = appProperties.kafka().topic();
        String key = systemLog.serviceName();

        CompletableFuture<SendResult<String, SystemLog>> future = kafkaTemplate.send(topic, key, systemLog);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                producedCount.incrementAndGet();
                log.info("[PRODUCE] Sent log eventId={} service={} level={} to partition={} offset={}",
                        systemLog.eventId(),
                        systemLog.serviceName(),
                        systemLog.logLevel(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("[PRODUCE ERROR] Failed to send log eventId={} service={}: {}",
                        systemLog.eventId(), systemLog.serviceName(), ex.getMessage(), ex);
            }
        });

        return future;
    }

    /**
     * Generates a synthetic log and sends it to Kafka.
     *
     * @return the generated SystemLog
     */
    public SystemLog generateAndSend() {
        SystemLog systemLog = mockLogGenerator.generateLog();
        sendLog(systemLog);
        return systemLog;
    }

    /**
     * Generates and sends a burst of synthetic logs to Kafka.
     *
     * @param count number of logs to generate and send
     * @return list of generated logs
     */
    public List<SystemLog> generateAndSendBurst(int count) {
        List<SystemLog> logs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            logs.add(generateAndSend());
        }
        return logs;
    }

    /**
     * Continuous mock log generator task, controlled via configuration property `app.producer.enabled`.
     */
    @Scheduled(fixedRateString = "${app.producer.fixed-rate-ms:1000}")
    public void scheduledProduce() {
        if (appProperties.producer().enabled()) {
            generateAndSend();
        }
    }

    public long getProducedCount() {
        return producedCount.get();
    }
}
