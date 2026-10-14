package com.streamingrag.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Enterprise data contract for ingested distributed system logs.
 * Matching the schema and embedding representation of the Python streaming-rag architecture.
 *
 * @param eventId     Unique identifier of the log event
 * @param timestamp   Event generation timestamp in UTC
 * @param serviceName Originating microservice name
 * @param logLevel    Log severity level (INFO, WARN, ERROR)
 * @param message     Core log message content
 * @param stackTrace  Optional stack trace if an exception occurred
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SystemLog(
    UUID eventId,
    Instant timestamp,
    String serviceName,
    LogLevel logLevel,
    String message,
    String stackTrace
) {

    public SystemLog {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(logLevel, "logLevel must not be null");
        Objects.requireNonNull(message, "message must not be null");
    }

    /**
     * Formats the log into the normalized text format required by the vector embedding model.
     * Matches the Python pipeline format:
     * [LEVEL] service-name @ timestamp
     * message
     * stack_trace: ... (optional)
     *
     * @return Formatted string for embedding generation
     */
    public String toEmbeddingText() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(logLevel).append("] ")
          .append(serviceName).append(" @ ")
          .append(timestamp)
          .append("\n")
          .append(message);

        if (stackTrace != null && !stackTrace.isBlank()) {
            sb.append("\nstack_trace: ").append(stackTrace.strip());
        }
        return sb.toString();
    }
}
