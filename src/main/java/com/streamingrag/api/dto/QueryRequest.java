package com.streamingrag.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

/**
 * Incoming request payload for RAG analysis query.
 */
@Schema(description = "Request payload for an SRE incident analysis query")
public record QueryRequest(
    @Schema(
        description = "Natural language incident query or question",
        example = "Investigate why checkout is failing and analyze errors in payment-gateway"
    )
    @NotBlank(message = "prompt must not be blank") String prompt,

    @Schema(
        description = "Maximum number of relevant log entries to retrieve from the vector store",
        example = "5",
        defaultValue = "5"
    )
    Integer k
) {
    public int getEffectiveK() {
        return (k != null && k > 0) ? k : 5;
    }
}
