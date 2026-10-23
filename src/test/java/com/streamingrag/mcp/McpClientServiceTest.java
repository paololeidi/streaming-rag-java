package com.streamingrag.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamingrag.agent.SreTools;
import com.streamingrag.config.AppProperties;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.service.tool.ToolExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class McpClientServiceTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private SreTools sreTools;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    private AppProperties createProperties(String mcpServerUrl) {
        return new AppProperties(
                new AppProperties.KafkaProperties("system-logs", "system-logs-dlq", "group", "localhost:9092", 1, (short) 1),
                new AppProperties.ProducerProperties(false, 1000L),
                new AppProperties.LuceneProperties("./data/lucene", true, 5),
                new AppProperties.OllamaProperties("http://localhost:11434", "llama3.2:3b", 0.0, 10),
                new AppProperties.McpProperties(mcpServerUrl, 5),
                new AppProperties.TracingProperties(false, "https://api.smith.langchain.com", "", "test")
        );
    }

    @Test
    @DisplayName("Should return unconfigured and empty tools when MCP server URL is blank")
    void shouldReturnEmptyToolsWhenUnconfigured() {
        McpClientService service = new McpClientService(createProperties(""), objectMapper, httpClient);

        assertThat(service.isConfigured()).isFalse();
        assertThat(service.listExternalTools()).isEmpty();
        assertThat(service.getExternalToolExecutors()).isEmpty();
    }

    @Test
    @DisplayName("Should discover external MCP tools via tools/list and execute via tools/call")
    @SuppressWarnings("unchecked")
    void shouldDiscoverAndExecuteExternalMcpTools() throws Exception {
        McpClientService service = new McpClientService(
                createProperties("http://localhost:8001/sse"), objectMapper, httpClient);

        HttpResponse<String> listResponse = mock(HttpResponse.class);
        when(listResponse.statusCode()).thenReturn(200);
        when(listResponse.body()).thenReturn("""
                {
                  "jsonrpc": "2.0",
                  "id": 1,
                  "result": {
                    "tools": [
                      {
                        "name": "pagerduty_incidents",
                        "description": "Fetch active PagerDuty incidents",
                        "inputSchema": {
                          "type": "object",
                          "properties": {
                            "severity": { "type": "string" }
                          },
                          "required": ["severity"]
                        }
                      }
                    ]
                  }
                }
                """);

        HttpResponse<String> callResponse = mock(HttpResponse.class);
        when(callResponse.statusCode()).thenReturn(200);
        when(callResponse.body()).thenReturn("""
                {
                  "jsonrpc": "2.0",
                  "id": 2,
                  "result": {
                    "content": [
                      { "type": "text", "text": "INC-901: Stripe gateway high latency" }
                    ],
                    "isError": false
                  }
                }
                """);

        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(listResponse)
                .thenReturn(callResponse);

        Map<ToolSpecification, ToolExecutor> combined = service.buildCombinedToolMap(sreTools);

        // Contains native tools (vectorSearch, logStats) + external tool (pagerduty_incidents)
        assertThat(combined.keySet())
                .extracting(ToolSpecification::name)
                .contains("vectorSearch", "logStats", "pagerduty_incidents");

        Map.Entry<ToolSpecification, ToolExecutor> externalEntry = combined.entrySet().stream()
                .filter(e -> "pagerduty_incidents".equals(e.getKey().name()))
                .findFirst()
                .orElseThrow();

        ToolExecutionRequest execReq = ToolExecutionRequest.builder()
                .id("call-1")
                .name("pagerduty_incidents")
                .arguments("{\"severity\":\"P1\"}")
                .build();

        String toolOutput = externalEntry.getValue().execute(execReq, "default");
        assertThat(toolOutput).isEqualTo("INC-901: Stripe gateway high latency");
    }

    @Test
    @DisplayName("Should gracefully fallback to native tools when external MCP server is unreachable")
    @SuppressWarnings("unchecked")
    void shouldFallbackGracefullyWhenExternalMcpUnreachable() throws Exception {
        McpClientService service = new McpClientService(
                createProperties("http://localhost:9999/sse"), objectMapper, httpClient);

        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new IOException("Connection refused"));

        Map<ToolSpecification, ToolExecutor> combined = service.buildCombinedToolMap(sreTools);
        assertThat(combined.keySet())
                .extracting(ToolSpecification::name)
                .containsExactlyInAnyOrder("vectorSearch", "logStats");
    }
}
