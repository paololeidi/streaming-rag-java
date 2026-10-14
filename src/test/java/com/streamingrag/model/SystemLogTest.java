package com.streamingrag.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SystemLogTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    @DisplayName("Should successfully construct SystemLog with valid parameters")
    void shouldConstructValidSystemLog() {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now();
        SystemLog log = new SystemLog(
                eventId,
                now,
                "payment-gateway",
                LogLevel.INFO,
                "Payment authorized successfully",
                null
        );

        assertThat(log.eventId()).isEqualTo(eventId);
        assertThat(log.timestamp()).isEqualTo(now);
        assertThat(log.serviceName()).isEqualTo("payment-gateway");
        assertThat(log.logLevel()).isEqualTo(LogLevel.INFO);
        assertThat(log.message()).isEqualTo("Payment authorized successfully");
        assertThat(log.stackTrace()).isNull();
    }

    @Test
    @DisplayName("Should throw NullPointerException when required fields are missing")
    void shouldThrowWhenRequiredFieldsAreNull() {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now();

        assertThatThrownBy(() -> new SystemLog(null, now, "svc", LogLevel.INFO, "msg", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventId must not be null");

        assertThatThrownBy(() -> new SystemLog(eventId, null, "svc", LogLevel.INFO, "msg", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("timestamp must not be null");

        assertThatThrownBy(() -> new SystemLog(eventId, now, null, LogLevel.INFO, "msg", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("serviceName must not be null");

        assertThatThrownBy(() -> new SystemLog(eventId, now, "svc", null, "msg", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("logLevel must not be null");

        assertThatThrownBy(() -> new SystemLog(eventId, now, "svc", LogLevel.INFO, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("message must not be null");
    }

    @Test
    @DisplayName("Should format embedding text without stack trace matching the contract")
    void shouldFormatEmbeddingTextWithoutStackTrace() {
        UUID eventId = UUID.randomUUID();
        Instant timestamp = Instant.parse("2026-10-12T10:15:30.00Z");
        SystemLog log = new SystemLog(
                eventId,
                timestamp,
                "user-auth",
                LogLevel.WARN,
                "Rate limit approaching for IP 192.168.1.1",
                null
        );

        String expected = "[WARN] user-auth @ 2026-10-12T10:15:30Z\nRate limit approaching for IP 192.168.1.1";
        assertThat(log.toEmbeddingText()).isEqualTo(expected);
    }

    @Test
    @DisplayName("Should format embedding text with stack trace matching the contract")
    void shouldFormatEmbeddingTextWithStackTrace() {
        UUID eventId = UUID.randomUUID();
        Instant timestamp = Instant.parse("2026-10-12T10:15:30.00Z");
        String stack = "java.lang.RuntimeException: DB connection failed\n    at com.example.Db.connect(Db.java:10)";
        SystemLog log = new SystemLog(
                eventId,
                timestamp,
                "inventory-service",
                LogLevel.ERROR,
                "Stock lookup failed",
                stack
        );

        String expected = """
                [ERROR] inventory-service @ 2026-10-12T10:15:30Z
                Stock lookup failed
                stack_trace: java.lang.RuntimeException: DB connection failed
                    at com.example.Db.connect(Db.java:10)""";

        assertThat(log.toEmbeddingText()).isEqualTo(expected);
    }

    @Test
    @DisplayName("Should serialize and deserialize to/from JSON round-trip")
    void shouldSerializeAndDeserializeJson() throws Exception {
        UUID eventId = UUID.randomUUID();
        Instant timestamp = Instant.parse("2026-10-12T12:00:00Z");
        SystemLog original = new SystemLog(
                eventId,
                timestamp,
                "frontend-bff",
                LogLevel.INFO,
                "GET /api/v1/health returned 200",
                null
        );

        String json = objectMapper.writeValueAsString(original);
        SystemLog deserialized = objectMapper.readValue(json, SystemLog.class);

        assertThat(deserialized).isEqualTo(original);
    }
}
