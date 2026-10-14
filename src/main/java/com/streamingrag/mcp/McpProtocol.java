package com.streamingrag.mcp;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

/**
 * Data structures matching the Model Context Protocol (MCP) JSON-RPC 2.0 specification.
 */
public final class McpProtocol {

    private McpProtocol() {}

    public static final String PROTOCOL_VERSION = "2024-11-05";

    public record JsonRpcRequest(
            String jsonrpc,
            Object id,
            String method,
            Map<String, Object> params
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record JsonRpcResponse(
            String jsonrpc,
            Object id,
            Object result,
            JsonRpcError error
    ) {
        public static JsonRpcResponse success(Object id, Object result) {
            return new JsonRpcResponse("2.0", id, result, null);
        }

        public static JsonRpcResponse error(Object id, int code, String message) {
            return new JsonRpcResponse("2.0", id, null, new JsonRpcError(code, message));
        }
    }

    public record JsonRpcError(int code, String message) {}

    public record ServerInfo(String name, String version) {}

    public record InitializeResult(
            String protocolVersion,
            ServerCapabilities capabilities,
            ServerInfo serverInfo
    ) {}

    public record ServerCapabilities(Map<String, Object> tools) {}

    public record ToolDefinition(
            String name,
            String description,
            Map<String, Object> inputSchema
    ) {}

    public record ToolsListResult(List<ToolDefinition> tools) {}

    public record ToolContent(String type, String text) {}

    public record ToolCallResult(List<ToolContent> content, boolean isError) {
        public static ToolCallResult text(String text) {
            return new ToolCallResult(List.of(new ToolContent("text", text)), false);
        }

        public static ToolCallResult error(String error) {
            return new ToolCallResult(List.of(new ToolContent("text", error)), true);
        }
    }
}
