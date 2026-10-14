package com.streamingrag.producer;

import com.streamingrag.model.LogLevel;
import com.streamingrag.model.SystemLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MockLogGeneratorTest {

    private MockLogGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new MockLogGenerator();
    }

    @Test
    @DisplayName("Should generate random log across the 4 microservices")
    void shouldGenerateRandomLog() {
        SystemLog log = generator.generateLog();

        assertThat(log).isNotNull();
        assertThat(log.eventId()).isNotNull();
        assertThat(log.timestamp()).isNotNull();
        assertThat(MockLogGenerator.SERVICES).contains(log.serviceName());
        assertThat(log.logLevel()).isNotNull();
        assertThat(log.message()).isNotBlank();
    }

    @Test
    @DisplayName("Should generate log for specific service")
    void shouldGenerateLogForSpecificService() {
        SystemLog log = generator.generateLog("payment-gateway");

        assertThat(log.serviceName()).isEqualTo("payment-gateway");
        assertThat(log.message()).isNotBlank();
    }

    @Test
    @DisplayName("Should generate ERROR log with non-blank stack trace")
    void shouldGenerateErrorLogWithStackTrace() {
        for (String service : MockLogGenerator.SERVICES) {
            SystemLog log = generator.generateLog(service, LogLevel.ERROR);

            assertThat(log.logLevel()).isEqualTo(LogLevel.ERROR);
            assertThat(log.stackTrace())
                    .as("Stack trace for ERROR log in service " + service)
                    .isNotBlank();
        }
    }

    @Test
    @DisplayName("Should generate INFO log with null stack trace")
    void shouldGenerateInfoLogWithNullStackTrace() {
        for (String service : MockLogGenerator.SERVICES) {
            SystemLog log = generator.generateLog(service, LogLevel.INFO);

            assertThat(log.logLevel()).isEqualTo(LogLevel.INFO);
            assertThat(log.stackTrace()).isNull();
        }
    }
}
