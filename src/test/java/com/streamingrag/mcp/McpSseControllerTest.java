package com.streamingrag.mcp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class McpSseControllerTest {

    @Mock
    private McpService mcpService;

    private McpSseController controller;

    @BeforeEach
    void setUp() {
        controller = new McpSseController(mcpService);
    }

    @Test
    @DisplayName("Should establish SSE connection and return SseEmitter")
    void shouldConnectSse() {
        SseEmitter emitter = controller.connectSse();
        assertThat(emitter).isNotNull();
    }

    @Test
    @DisplayName("Should process POST /messages and return JsonRpcResponse")
    void shouldHandleMessage() {
        McpProtocol.JsonRpcRequest request = new McpProtocol.JsonRpcRequest("2.0", 1, "ping", Map.of());
        McpProtocol.JsonRpcResponse expectedResponse = McpProtocol.JsonRpcResponse.success(1, Map.of());

        when(mcpService.processRequest(any())).thenReturn(expectedResponse);

        ResponseEntity<McpProtocol.JsonRpcResponse> response = controller.handleMessage("session-123", request);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isEqualTo(expectedResponse);
    }

    @Test
    @DisplayName("Should return 202 Accepted for notifications with null id")
    void shouldHandleNotificationSilently() {
        McpProtocol.JsonRpcRequest notification = new McpProtocol.JsonRpcRequest("2.0", null, "notifications/initialized", Map.of());

        ResponseEntity<McpProtocol.JsonRpcResponse> response = controller.handleMessage("session-123", notification);

        assertThat(response.getStatusCode().value()).isEqualTo(202);
    }

    @Test
    @DisplayName("Should process POST /sse for Streamable HTTP requests")
    void shouldHandleStreamableHttp() {
        McpProtocol.JsonRpcRequest request = new McpProtocol.JsonRpcRequest("2.0", 1, "initialize", Map.of());
        McpProtocol.JsonRpcResponse expectedResponse = McpProtocol.JsonRpcResponse.success(1, Map.of("protocolVersion", "2024-11-05"));

        when(mcpService.processRequest(any())).thenReturn(expectedResponse);

        ResponseEntity<McpProtocol.JsonRpcResponse> response = controller.handleStreamableHttp(request);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isEqualTo(expectedResponse);
    }
}
