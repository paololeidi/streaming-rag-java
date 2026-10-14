package com.streamingrag.mcp;

import com.streamingrag.agent.SreTools;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class McpServiceTest {

    @Mock
    private SreTools sreTools;

    private McpService mcpService;

    @BeforeEach
    void setUp() {
        mcpService = new McpService(sreTools);
    }

    @Test
    @DisplayName("Should handle MCP initialize method")
    void shouldHandleInitialize() {
        McpProtocol.JsonRpcRequest request = new McpProtocol.JsonRpcRequest("2.0", 1, "initialize", Map.of());
        McpProtocol.JsonRpcResponse response = mcpService.processRequest(request);

        assertThat(response.jsonrpc()).isEqualTo("2.0");
        assertThat(response.id()).isEqualTo(1);
        assertThat(response.error()).isNull();

        McpProtocol.InitializeResult result = (McpProtocol.InitializeResult) response.result();
        assertThat(result.protocolVersion()).isEqualTo(McpProtocol.PROTOCOL_VERSION);
        assertThat(result.serverInfo().name()).isEqualTo("streaming-rag-mcp-server");
    }

    @Test
    @DisplayName("Should handle tools/list and return vector_search and log_stats definitions")
    void shouldHandleToolsList() {
        McpProtocol.JsonRpcRequest request = new McpProtocol.JsonRpcRequest("2.0", 2, "tools/list", Map.of());
        McpProtocol.JsonRpcResponse response = mcpService.processRequest(request);

        assertThat(response.result()).isInstanceOf(McpProtocol.ToolsListResult.class);
        McpProtocol.ToolsListResult result = (McpProtocol.ToolsListResult) response.result();

        assertThat(result.tools()).hasSize(2);
        assertThat(result.tools()).extracting(McpProtocol.ToolDefinition::name)
                .containsExactlyInAnyOrder("vector_search", "log_stats");
    }

    @Test
    @DisplayName("Should handle tools/call for vector_search")
    void shouldHandleToolCallVectorSearch() {
        when(sreTools.vectorSearch(eq("database timeout"), eq(3)))
                .thenReturn("Found 1 error: timeout");

        McpProtocol.JsonRpcRequest request = new McpProtocol.JsonRpcRequest(
                "2.0",
                3,
                "tools/call",
                Map.of("name", "vector_search", "arguments", Map.of("query", "database timeout", "k", 3))
        );

        McpProtocol.JsonRpcResponse response = mcpService.processRequest(request);

        assertThat(response.error()).isNull();
        McpProtocol.ToolCallResult result = (McpProtocol.ToolCallResult) response.result();
        assertThat(result.isError()).isFalse();
        assertThat(result.content().get(0).text()).isEqualTo("Found 1 error: timeout");
        verify(sreTools).vectorSearch("database timeout", 3);
    }

    @Test
    @DisplayName("Should handle tools/call for log_stats")
    void shouldHandleToolCallLogStats() {
        when(sreTools.logStats(eq("ERROR"), eq("user-auth"), eq(30)))
                .thenReturn("Stats: 5 errors in user-auth");

        McpProtocol.JsonRpcRequest request = new McpProtocol.JsonRpcRequest(
                "2.0",
                4,
                "tools/call",
                Map.of("name", "log_stats", "arguments", Map.of("log_level", "ERROR", "service_name", "user-auth", "minutes", 30))
        );

        McpProtocol.JsonRpcResponse response = mcpService.processRequest(request);

        assertThat(response.error()).isNull();
        McpProtocol.ToolCallResult result = (McpProtocol.ToolCallResult) response.result();
        assertThat(result.isError()).isFalse();
        assertThat(result.content().get(0).text()).isEqualTo("Stats: 5 errors in user-auth");
        verify(sreTools).logStats("ERROR", "user-auth", 30);
    }

    @Test
    @DisplayName("Should return method not found error for unknown method")
    void shouldHandleUnknownMethod() {
        McpProtocol.JsonRpcRequest request = new McpProtocol.JsonRpcRequest("2.0", 5, "unknown_method", Map.of());
        McpProtocol.JsonRpcResponse response = mcpService.processRequest(request);

        assertThat(response.error()).isNotNull();
        assertThat(response.error().code()).isEqualTo(-32601);
        assertThat(response.error().message()).contains("Method not found");
    }
}
