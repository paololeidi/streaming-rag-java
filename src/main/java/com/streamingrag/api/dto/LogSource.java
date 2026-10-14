package com.streamingrag.api.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Data transfer object representing a cited log evidence item.
 */
public record LogSource(
    UUID eventId,
    Instant timestamp,
    String serviceName,
    String logLevel,
    String excerpt,
    double score
) {}
