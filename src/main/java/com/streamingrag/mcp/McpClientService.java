package com.streamingrag.mcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamingrag.agent.SreTools;
import com.streamingrag.config.AppProperties;
import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.agent.tool.ToolParameters;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.agent.tool.ToolSpecifications;
import dev.langchain4j.service.tool.DefaultToolExecutor;
import dev.langchain4j.service.tool.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dynamic Model Context Protocol (MCP) client service.
 * Connects to external MCP servers configured via {@code app.mcp.server-url} (env {@code MCP_SERVER_URL}),
 * discovers external tools via {@code tools/list}, and bridges tool executions via {@code tools/call}
 * into the LangChain4j {@code SreAnalystAgent} with graceful fallback when unconfigured or unreachable.
 */
@Service
public class McpClientService {

    private static final Logger log = LoggerFactory.getLogger(McpClientService.class);

    private final AppProperties appProperties;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final AtomicLong requestIdGenerator = new AtomicLong(1);

    @Autowired
    public McpClientService(AppProperties appProperties, ObjectMapper objectMapper) {
        this(
                appProperties,
                objectMapper,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(
                                Math.max(1, appProperties.mcp().timeoutSeconds())))
                        .build()
        );
    }

    public McpClientService(AppProperties appProperties, ObjectMapper objectMapper, HttpClient httpClient) {
        this.appProperties = appProperties;
        this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
        this.httpClient = httpClient;
    }

    /**
     * Checks whether an external MCP server URL is configured.
     */
    public boolean isConfigured() {
        String url = appProperties.mcp().serverUrl();
        return url != null && !url.isBlank();
    }

    /**
     * Resolves the JSON-RPC HTTP endpoint URL from the configured MCP server URL.
     */
    public String resolveEndpointUrl() {
        String raw = appProperties.mcp().serverUrl();
        if (raw == null || raw.isBlank()) {
            return "";
        }
        String trimmed = raw.trim();
        try {
            URI uri = URI.create(trimmed);
            String path = uri.getPath();
            if (path == null || path.isEmpty() || "/".equals(path)) {
                return trimmed.replaceAll("/+$", "") + "/messages";
            }
        } catch (IllegalArgumentException ignored) {
        }
        return trimmed;
    }

    /**
     * Queries the external MCP server for available tool definitions using {@code tools/list}.
     * Returns an empty list if unconfigured or if the external server is unreachable.
     */
    @SuppressWarnings("unchecked")
    public List<McpProtocol.ToolDefinition> listExternalTools() {
        if (!isConfigured()) {
            return Collections.emptyList();
        }

        String endpoint = resolveEndpointUrl();
        try {
            McpProtocol.JsonRpcRequest rpcRequest = new McpProtocol.JsonRpcRequest(
                    "2.0",
                    requestIdGenerator.getAndIncrement(),
                    "tools/list",
                    Map.of()
            );

            McpProtocol.JsonRpcResponse rpcResponse = sendRpcRequest(endpoint, rpcRequest);
            if (rpcResponse == null || rpcResponse.error() != null || rpcResponse.result() == null) {
                log.warn("[MCP CLIENT] External MCP server at {} returned error or empty result on tools/list", endpoint);
                return Collections.emptyList();
            }

            Map<String, Object> resultMap = objectMapper.convertValue(
                    rpcResponse.result(), new TypeReference<Map<String, Object>>() {});
            Object rawTools = resultMap.get("tools");
            if (!(rawTools instanceof List<?> toolList)) {
                return Collections.emptyList();
            }

            List<McpProtocol.ToolDefinition> definitions = new ArrayList<>();
            for (Object item : toolList) {
                if (item instanceof Map<?, ?> mapItem) {
                    String name = (String) mapItem.get("name");
                    String description = (String) mapItem.get("description");
                    Map<String, Object> inputSchema = (Map<String, Object>) mapItem.get("inputSchema");
                    if (name != null && !name.isBlank()) {
                        definitions.add(new McpProtocol.ToolDefinition(
                                name,
                                description != null ? description : "",
                                inputSchema != null ? inputSchema : Map.of("type", "object")
                        ));
                    }
                }
            }

            log.info("[MCP CLIENT] Discovered {} external MCP tools from {}", definitions.size(), endpoint);
            return definitions;
        } catch (Exception e) {
            log.warn("[MCP CLIENT] Failed to discover tools from external MCP server {}: {}. Falling back to native tools.",
                    endpoint, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Executes a tool on the external MCP server via {@code tools/call}.
     *
     * @param toolName  name of the remote MCP tool
     * @param arguments tool invocation arguments
     * @return text output returned by the external MCP tool
     */
    @SuppressWarnings("unchecked")
    public String callExternalTool(String toolName, Map<String, Object> arguments) {
        if (!isConfigured()) {
            return "External MCP server is not configured.";
        }

        String endpoint = resolveEndpointUrl();
        try {
            Map<String, Object> params = Map.of(
                    "name", toolName,
                    "arguments", arguments != null ? arguments : Map.of()
            );
            McpProtocol.JsonRpcRequest rpcRequest = new McpProtocol.JsonRpcRequest(
                    "2.0",
                    requestIdGenerator.getAndIncrement(),
                    "tools/call",
                    params
            );

            McpProtocol.JsonRpcResponse rpcResponse = sendRpcRequest(endpoint, rpcRequest);
            if (rpcResponse == null) {
                return "Error: Empty response from external MCP server for tool " + toolName;
            }
            if (rpcResponse.error() != null) {
                return "Error from external MCP tool " + toolName + ": " + rpcResponse.error().message();
            }
            if (rpcResponse.result() == null) {
                return "";
            }

            Map<String, Object> resultMap = objectMapper.convertValue(
                    rpcResponse.result(), new TypeReference<Map<String, Object>>() {});
            Object contentObj = resultMap.get("content");
            if (contentObj instanceof List<?> contentList) {
                StringBuilder sb = new StringBuilder();
                for (Object item : contentList) {
                    if (item instanceof Map<?, ?> block) {
                        Object text = block.get("text");
                        if (text != null) {
                            if (!sb.isEmpty()) {
                                sb.append("\n");
                            }
                            sb.append(text);
                        }
                    }
                }
                return sb.toString();
            }
            return objectMapper.writeValueAsString(rpcResponse.result());
        } catch (Exception e) {
            log.error("[MCP CLIENT ERROR] Failed calling external MCP tool '{}': {}", toolName, e.getMessage());
            return "Failed calling external MCP tool '" + toolName + "': " + e.getMessage();
        }
    }

    /**
     * Builds LangChain4j {@link ToolSpecification} to {@link ToolExecutor} mappings for discovered external MCP tools.
     */
    public Map<ToolSpecification, ToolExecutor> getExternalToolExecutors() {
        List<McpProtocol.ToolDefinition> definitions = listExternalTools();
        if (definitions.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<ToolSpecification, ToolExecutor> toolMap = new LinkedHashMap<>();
        for (McpProtocol.ToolDefinition def : definitions) {
            ToolSpecification spec = toToolSpecification(def);
            ToolExecutor executor = (request, memoryId) -> {
                Map<String, Object> args = parseArguments(request.arguments());
                return callExternalTool(def.name(), args);
            };
            toolMap.put(spec, executor);
        }
        return toolMap;
    }

    /**
     * Combines native {@link SreTools} methods with any dynamically discovered external MCP tools.
     */
    public Map<ToolSpecification, ToolExecutor> buildCombinedToolMap(SreTools sreTools) {
        Map<ToolSpecification, ToolExecutor> combined = new LinkedHashMap<>();

        if (sreTools != null) {
            for (Method method : SreTools.class.getDeclaredMethods()) {
                if (method.isAnnotationPresent(Tool.class)) {
                    ToolSpecification spec = ToolSpecifications.toolSpecificationFrom(method);
                    combined.put(spec, new DefaultToolExecutor(sreTools, method));
                }
            }
        }

        Map<ToolSpecification, ToolExecutor> external = getExternalToolExecutors();
        for (Map.Entry<ToolSpecification, ToolExecutor> entry : external.entrySet()) {
            boolean existsNatively = combined.keySet().stream()
                    .anyMatch(nativeSpec -> nativeSpec.name().equals(entry.getKey().name()));
            if (!existsNatively) {
                combined.put(entry.getKey(), entry.getValue());
            }
        }

        return combined;
    }

    @SuppressWarnings("unchecked")
    private ToolSpecification toToolSpecification(McpProtocol.ToolDefinition def) {
        ToolSpecification.Builder builder = ToolSpecification.builder()
                .name(def.name())
                .description(def.description());

        Map<String, Object> schema = def.inputSchema();
        if (schema != null && !schema.isEmpty()) {
            ToolParameters.Builder paramBuilder = ToolParameters.builder();
            Object typeObj = schema.get("type");
            if (typeObj instanceof String typeStr) {
                paramBuilder.type(typeStr);
            }
            Object propsObj = schema.get("properties");
            if (propsObj instanceof Map<?, ?> propsMap) {
                paramBuilder.properties((Map<String, Map<String, Object>>) propsMap);
            }
            Object reqObj = schema.get("required");
            if (reqObj instanceof List<?> reqList) {
                paramBuilder.required((List<String>) reqList);
            }
            builder.parameters(paramBuilder.build());
        }

        return builder.build();
    }

    private Map<String, Object> parseArguments(String argumentsJson) {
        if (argumentsJson == null || argumentsJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(argumentsJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.warn("[MCP CLIENT] Failed parsing tool arguments JSON '{}': {}", argumentsJson, e.getMessage());
            return Map.of();
        }
    }

    private McpProtocol.JsonRpcResponse sendRpcRequest(String endpoint, McpProtocol.JsonRpcRequest rpcRequest) throws Exception {
        String body = objectMapper.writeValueAsString(rpcRequest);
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .timeout(Duration.ofSeconds(Math.max(1, appProperties.mcp().timeoutSeconds())))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("HTTP " + response.statusCode() + " from MCP endpoint " + endpoint);
        }
        return objectMapper.readValue(response.body(), McpProtocol.JsonRpcResponse.class);
    }
}
