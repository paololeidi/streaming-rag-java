package com.streamingrag.api.dto;

import java.time.Instant;
import java.util.List;

/**
 * Structured response payload containing agent answer and cited log sources.
 */
public record QueryResponse(
    String answer,
    List<LogSource> sources,
    Instant generatedAt
) {}
