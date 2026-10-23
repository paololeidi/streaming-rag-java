package com.streamingrag.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * Structured response payload containing agent answer and cited log sources.
 */
@Schema(description = "Structured response containing the synthesized RCA report and cited log sources")
public record QueryResponse(
    @Schema(description = "Synthesized natural-language incident diagnosis and remediation report")
    String answer,

    @Schema(description = "Cited log records retrieved from Apache Lucene during agent reasoning")
    List<LogSource> sources,

    @Schema(description = "UTC timestamp when the response was generated")
    Instant generatedAt
) {}
