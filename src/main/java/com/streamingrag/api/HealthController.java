package com.streamingrag.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Health check endpoint for container orchestrators and readiness probes.
 */
@RestController
@Tag(name = "Health", description = "Liveness and readiness probe endpoints")
public class HealthController {

    @GetMapping("/health")
    @Operation(summary = "Application health check", description = "Returns HTTP 200 with status 'ok' when the service is ready.")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "ok"));
    }
}
