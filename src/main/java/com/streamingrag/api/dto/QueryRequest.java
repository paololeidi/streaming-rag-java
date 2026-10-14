package com.streamingrag.api.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Incoming request payload for RAG analysis query.
 */
public record QueryRequest(
    @NotBlank(message = "prompt must not be blank") String prompt,
    Integer k
) {
    public int getEffectiveK() {
        return (k != null && k > 0) ? k : 5;
    }
}
