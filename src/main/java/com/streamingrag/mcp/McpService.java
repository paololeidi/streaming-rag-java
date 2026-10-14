package com.streamingrag.mcp;

import com.streamingrag.agent.SreTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Service executing Model Context Protocol (MCP) JSON-RPC 2.0 requests.
 * Exposes vector_search and log_stats as standard MCP tools for Cursor IDE and Claude Desktop.
 */
@Service
public class McpService {

    private static final Logger log = LoggerFactory.getLogger(McpService.class);

    private final SreTools sreTools;

    public McpService(SreTools sreTools) {
        this.sreTools = sreTools;
    }

    public McpProtocol.JsonRpcResponse processRequest(McpProtocol.JsonRpcRequest request) {
        if (request == null || request.method() == null) {
            return McpProtocol.JsonRpcResponse.error(null, -32600, "Invalid Request");
        }

        Object id = request.id();
        String method = request.method();
        Map<String, Object> params = (request.params() != null) ? request.params() : Map.of();

        log.info("[MCP] Processing method='{}' id={}", method, id);

        return switch (method) {
            case "initialize" -> McpProtocol.JsonRpcResponse.success(id, handleInitialize());
            case "notifications/initialized", "initialized" -> McpProtocol.JsonRpcResponse.success(id, Map.of());
            case "ping" -> McpProtocol.JsonRpcResponse.success(id, Map.of());
            case "tools/list" -> McpProtocol.JsonRpcResponse.success(id, handleToolsList());
            case "tools/call" -> handleToolsCall(id, params);
            default -> {
                log.warn("[MCP] Method not found: {}", method);
                yield McpProtocol.JsonRpcResponse.error(id, -32601, "Method not found: " + method);
            }
        };
    }

    private McpProtocol.InitializeResult handleInitialize() {
        return new McpProtocol.InitializeResult(
                McpProtocol.PROTOCOL_VERSION,
                new McpProtocol.ServerCapabilities(Map.of()),
                new McpProtocol.ServerInfo("streaming-rag-mcp-server", "1.0.0")
        );
    }

    private McpProtocol.ToolsListResult handleToolsList() {
        McpProtocol.ToolDefinition vectorSearchTool = new McpProtocol.ToolDefinition(
                "vector_search",
                "Semantic vector search over ingested distributed microservice logs in Apache Lucene",
                Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "query", Map.of("type", "string", "description", "Incident symptoms, error messages or keywords"),
                                "k", Map.of("type", "integer", "description", "Maximum number of logs to retrieve (default: 5)")
                        ),
                        "required", List.of("query")
                )
        );

        McpProtocol.ToolDefinition logStatsTool = new McpProtocol.ToolDefinition(
                "log_stats",
                "Returns aggregate log counts, error distributions and ingestion metrics for the given time window",
                Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "log_level", Map.of("type", "string", "description", "Severity level: 'INFO', 'WARN', 'ERROR', or 'ALL'"),
                                "service_name", Map.of("type", "string", "description", "Service name ('user-auth', 'payment-gateway', etc.) or 'ALL'"),
                                "minutes", Map.of("type", "integer", "description", "Analysis time window in minutes (default: 15)")
                        ),
                        "required", List.of("log_level", "service_name")
                )
        );

        return new McpProtocol.ToolsListResult(List.of(vectorSearchTool, logStatsTool));
    }

    @SuppressWarnings("unchecked")
    private McpProtocol.JsonRpcResponse handleToolsCall(Object id, Map<String, Object> params) {
        String toolName = (String) params.get("name");
        Map<String, Object> arguments = (Map<String, Object>) params.getOrDefault("arguments", Map.of());

        if (toolName == null) {
            return McpProtocol.JsonRpcResponse.error(id, -32602, "Missing tool name in params");
        }

        try {
            if ("vector_search".equals(toolName)) {
                String query = (String) arguments.get("query");
                if (query == null || query.isBlank()) {
                    return McpProtocol.JsonRpcResponse.success(id, McpProtocol.ToolCallResult.error("Argument 'query' is required"));
                }
                int k = arguments.containsKey("k") ? ((Number) arguments.get("k")).intValue() : 5;
                String result = sreTools.vectorSearch(query, k);
                return McpProtocol.JsonRpcResponse.success(id, McpProtocol.ToolCallResult.text(result));
            } else if ("log_stats".equals(toolName)) {
                String logLevel = (String) arguments.getOrDefault("log_level", "ALL");
                String serviceName = (String) arguments.getOrDefault("service_name", "ALL");
                int minutes = arguments.containsKey("minutes") ? ((Number) arguments.get("minutes")).intValue() : 15;
                String result = sreTools.logStats(logLevel, serviceName, minutes);
                return McpProtocol.JsonRpcResponse.success(id, McpProtocol.ToolCallResult.text(result));
            } else {
                return McpProtocol.JsonRpcResponse.error(id, -32601, "Unknown tool: " + toolName);
            }
        } catch (Exception e) {
            log.error("[MCP ERROR] Failed executing tool {}: {}", toolName, e.getMessage(), e);
            return McpProtocol.JsonRpcResponse.success(id, McpProtocol.ToolCallResult.error("Tool execution failed: " + e.getMessage()));
        }
    }
}
