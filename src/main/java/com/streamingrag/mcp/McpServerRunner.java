package com.streamingrag.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Runner supporting the Model Context Protocol (MCP) stdio transport.
 * Allows running the application as a local CLI subprocess registered in Cursor or Claude Desktop.
 */
@Component
@ConditionalOnProperty(prefix = "app.mcp.stdio", name = "enabled", havingValue = "true")
public class McpServerRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(McpServerRunner.class);

    private final McpService mcpService;
    private final ObjectMapper objectMapper;

    public McpServerRunner(McpService mcpService, ObjectMapper objectMapper) {
        this.mcpService = mcpService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("[MCP STDIO] Server listening on standard input...");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            }
            try {
                McpProtocol.JsonRpcRequest request = objectMapper.readValue(line, McpProtocol.JsonRpcRequest.class);
                McpProtocol.JsonRpcResponse response = mcpService.processRequest(request);
                String responseJson = objectMapper.writeValueAsString(response);
                System.out.println(responseJson);
                System.out.flush();
            } catch (Exception e) {
                log.error("[MCP STDIO ERROR] Failed processing input: {}", e.getMessage(), e);
            }
        }
    }
}
