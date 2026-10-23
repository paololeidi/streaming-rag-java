package com.streamingrag.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

/**
 * Data transfer object representing a cited log evidence item.
 */
@Schema(description = "Cited log evidence item retrieved from the Apache Lucene vector store")
public record LogSource(
    @Schema(description = "Unique log event identifier", example = "214a1f3c-59bf-4da2-8561-12c1b979a4de")
    UUID eventId,

    @Schema(description = "UTC timestamp when the log event was emitted", example = "2026-10-12T14:22:10Z")
    Instant timestamp,

    @Schema(description = "Originating microservice name", example = "payment-gateway")
    String serviceName,

    @Schema(description = "Log severity level", example = "ERROR")
    String logLevel,

    @Schema(description = "Normalized log excerpt", example = "[ERROR] payment-gateway @ 2026-10-12T14:22:10Z\nStripe API unreachable")
    String excerpt,

    @Schema(description = "Cosine similarity relevance score (0.0 to 1.0)", example = "0.892")
    double score
) {}
