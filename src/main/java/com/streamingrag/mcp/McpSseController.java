package com.streamingrag.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Controller exposing the Model Context Protocol (MCP) SSE Transport (Port 8080).
 * Fully compatible with Cursor IDE and Claude Desktop MCP configurations.
 * Supports both standard MCP SSE (GET /sse + POST /messages?sessionId=...)
 * and MCP Streamable HTTP (POST /sse).
 */
@CrossOrigin(origins = "*", allowedHeaders = "*", methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.OPTIONS})
@RestController
public class McpSseController {

    private static final Logger log = LoggerFactory.getLogger(McpSseController.class);

    private final McpService mcpService;
    private final ObjectMapper objectMapper;
    private final Map<String, SseEmitter> activeSessions = new ConcurrentHashMap<>();

    @Autowired
    public McpSseController(McpService mcpService, ObjectMapper objectMapper) {
        this.mcpService = mcpService;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    public McpSseController(McpService mcpService) {
        this(mcpService, new ObjectMapper());
    }

    /**
     * MCP SSE stream endpoint.
     * Emits the endpoint URI where the MCP client should post JSON-RPC 2.0 messages.
     */
    @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter connectSse() {
        String sessionId = UUID.randomUUID().toString();
        log.info("[MCP SSE] Client connected with sessionId={}", sessionId);

        // 0L timeout = infinite / no timeout for persistent IDE connections
        SseEmitter emitter = new SseEmitter(0L);
        activeSessions.put(sessionId, emitter);

        emitter.onCompletion(() -> {
            log.info("[MCP SSE] Session completed sessionId={}", sessionId);
            activeSessions.remove(sessionId);
        });
        emitter.onTimeout(() -> {
            log.info("[MCP SSE] Session timed out sessionId={}", sessionId);
            activeSessions.remove(sessionId);
        });
        emitter.onError(e -> {
            log.warn("[MCP SSE] Session error sessionId={}: {}", sessionId, e.getMessage());
            activeSessions.remove(sessionId);
        });

        try {
            // Standard MCP SSE handshake: send endpoint event with relative URI
            emitter.send(SseEmitter.event()
                    .name("endpoint")
                    .data("/messages?sessionId=" + sessionId));
            log.info("[MCP SSE] Sent endpoint handshake for sessionId={}", sessionId);
        } catch (IOException e) {
            log.error("[MCP SSE ERROR] Failed to send endpoint handshake event: {}", e.getMessage());
            emitter.completeWithError(e);
        }

        return emitter;
    }

    /**
     * MCP Streamable HTTP fallback endpoint (POST /sse).
     * Handled when MCP clients probe Streamable HTTP directly.
     */
    @PostMapping(value = "/sse", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<McpProtocol.JsonRpcResponse> handleStreamableHttp(
            @RequestBody McpProtocol.JsonRpcRequest request) {
        log.info("[MCP Streamable HTTP] Received request method={} id={}",
                request != null ? request.method() : null,
                request != null ? request.id() : null);

        if (request != null && request.id() == null) {
            mcpService.processRequest(request);
            return ResponseEntity.accepted().build();
        }

        McpProtocol.JsonRpcResponse response = mcpService.processRequest(request);
        return ResponseEntity.ok(response);
    }

    /**
     * MCP JSON-RPC 2.0 message handler (POST /messages).
     * In MCP SSE transport, messages from client to server are posted here,
     * and responses from server to client are pushed over the active SSE stream.
     */
    @PostMapping("/messages")
    public ResponseEntity<McpProtocol.JsonRpcResponse> handleMessage(
            @RequestParam(required = false) String sessionId,
            @RequestBody McpProtocol.JsonRpcRequest request) {
        log.info("[MCP POST] Received message method={} id={} sessionId={}",
                request != null ? request.method() : null,
                request != null ? request.id() : null,
                sessionId);

        McpProtocol.JsonRpcResponse response = mcpService.processRequest(request);

        // If request is a notification (no id), JSON-RPC 2.0 specifies that the server MUST NOT reply.
        if (request != null && request.id() == null) {
            log.info("[MCP] Handled notification method='{}', skipping SSE response", request.method());
            return ResponseEntity.accepted().build();
        }

        // For SSE clients: emit response over the active SSE stream
        if (sessionId != null) {
            SseEmitter emitter = activeSessions.get(sessionId);
            if (emitter != null && response != null) {
                try {
                    String json = objectMapper.writeValueAsString(response);
                    emitter.send(SseEmitter.event()
                            .name("message")
                            .data(json));
                    log.info("[MCP SSE] Emitted 'message' event for method={} id={} to sessionId={}",
                            request != null ? request.method() : null,
                            request != null ? request.id() : null,
                            sessionId);
                } catch (Exception e) {
                    log.error("[MCP SSE ERROR] Failed sending message to sessionId={}: {}", sessionId, e.getMessage());
                    activeSessions.remove(sessionId);
                }
            }
        }

        // Return HTTP 200 with response body (supports direct HTTP clients like curl and tests)
        return ResponseEntity.ok(response);
    }
}
