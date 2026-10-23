package com.streamingrag.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.listener.ChatModelErrorContext;
import dev.langchain4j.model.chat.listener.ChatModelListener;
import dev.langchain4j.model.chat.listener.ChatModelRequest;
import dev.langchain4j.model.chat.listener.ChatModelRequestContext;
import dev.langchain4j.model.chat.listener.ChatModelResponse;
import dev.langchain4j.model.chat.listener.ChatModelResponseContext;
import dev.langchain4j.model.output.TokenUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LangChain4j {@link ChatModelListener} providing structured LLM observability (latency, token usage,
 * tool call telemetry) and optional LangSmith trace forwarding when {@code LANGCHAIN_TRACING_V2=true}.
 */
@Component
public class LlmObservabilityListener implements ChatModelListener {

    private static final Logger log = LoggerFactory.getLogger(LlmObservabilityListener.class);

    public static final String ATTR_RUN_ID = "streamingrag.run_id";
    public static final String ATTR_START_TIME_MS = "streamingrag.start_time_ms";

    private final AppProperties appProperties;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalResponses = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong totalInputTokens = new AtomicLong(0);
    private final AtomicLong totalOutputTokens = new AtomicLong(0);

    @Autowired
    public LlmObservabilityListener(AppProperties appProperties, ObjectMapper objectMapper) {
        this(
                appProperties,
                objectMapper,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build()
        );
    }

    public LlmObservabilityListener(AppProperties appProperties, ObjectMapper objectMapper, HttpClient httpClient) {
        this.appProperties = appProperties;
        this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
        this.httpClient = httpClient;
    }

    public boolean isTracingEnabled() {
        AppProperties.TracingProperties tracing = appProperties.tracing();
        return tracing != null
                && tracing.enabled()
                && tracing.apiKey() != null
                && !tracing.apiKey().isBlank();
    }

    @Override
    public void onRequest(ChatModelRequestContext requestContext) {
        totalRequests.incrementAndGet();
        String runId = UUID.randomUUID().toString();
        long startTimeMs = System.currentTimeMillis();

        Map<Object, Object> attributes = requestContext.attributes();
        if (attributes != null) {
            attributes.put(ATTR_RUN_ID, runId);
            attributes.put(ATTR_START_TIME_MS, startTimeMs);
        }

        ChatModelRequest request = requestContext.request();
        String model = request != null ? request.model() : "unknown";
        int messageCount = (request != null && request.messages() != null) ? request.messages().size() : 0;
        int toolCount = (request != null && request.toolSpecifications() != null) ? request.toolSpecifications().size() : 0;

        log.info("[LLM TRACE: REQUEST] runId={} model={} messages={} tools={}",
                runId, model, messageCount, toolCount);

        if (isTracingEnabled()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("id", runId);
            payload.put("name", "ChatOllama");
            payload.put("run_type", "llm");
            payload.put("session_name", appProperties.tracing().project());
            payload.put("start_time", Instant.ofEpochMilli(startTimeMs).toString());
            payload.put("inputs", Map.of(
                    "model", model,
                    "message_count", messageCount,
                    "tool_count", toolCount
            ));
            sendLangSmithTrace("/runs", "POST", payload);
        }
    }

    @Override
    public void onResponse(ChatModelResponseContext responseContext) {
        totalResponses.incrementAndGet();
        Map<Object, Object> attributes = responseContext.attributes();
        String runId = attributes != null && attributes.get(ATTR_RUN_ID) != null
                ? attributes.get(ATTR_RUN_ID).toString()
                : UUID.randomUUID().toString();
        long startTimeMs = attributes != null && attributes.get(ATTR_START_TIME_MS) instanceof Long t
                ? t
                : System.currentTimeMillis();
        long latencyMs = Math.max(0L, System.currentTimeMillis() - startTimeMs);

        ChatModelResponse response = responseContext.response();
        String model = response != null ? response.model() : "unknown";
        TokenUsage tokenUsage = response != null ? response.tokenUsage() : null;
        int inputTokens = (tokenUsage != null && tokenUsage.inputTokenCount() != null) ? tokenUsage.inputTokenCount() : 0;
        int outputTokens = (tokenUsage != null && tokenUsage.outputTokenCount() != null) ? tokenUsage.outputTokenCount() : 0;
        int totalTokens = (tokenUsage != null && tokenUsage.totalTokenCount() != null)
                ? tokenUsage.totalTokenCount()
                : (inputTokens + outputTokens);

        totalInputTokens.addAndGet(inputTokens);
        totalOutputTokens.addAndGet(outputTokens);

        AiMessage aiMessage = response != null ? response.aiMessage() : null;
        List<ToolExecutionRequest> toolRequests = (aiMessage != null && aiMessage.hasToolExecutionRequests())
                ? aiMessage.toolExecutionRequests()
                : List.of();

        log.info("[LLM TRACE: RESPONSE] runId={} model={} latencyMs={} inputTokens={} outputTokens={} totalTokens={} toolCalls={}",
                runId, model, latencyMs, inputTokens, outputTokens, totalTokens, toolRequests.size());

        if (isTracingEnabled()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("end_time", Instant.now().toString());
            payload.put("outputs", Map.of(
                    "text", (aiMessage != null && aiMessage.text() != null) ? aiMessage.text() : "",
                    "tool_calls", toolRequests.size(),
                    "input_tokens", inputTokens,
                    "output_tokens", outputTokens,
                    "total_tokens", totalTokens,
                    "latency_ms", latencyMs
            ));
            sendLangSmithTrace("/runs/" + runId, "PATCH", payload);
        }
    }

    @Override
    public void onError(ChatModelErrorContext errorContext) {
        totalErrors.incrementAndGet();
        Map<Object, Object> attributes = errorContext.attributes();
        String runId = attributes != null && attributes.get(ATTR_RUN_ID) != null
                ? attributes.get(ATTR_RUN_ID).toString()
                : UUID.randomUUID().toString();
        long startTimeMs = attributes != null && attributes.get(ATTR_START_TIME_MS) instanceof Long t
                ? t
                : System.currentTimeMillis();
        long latencyMs = Math.max(0L, System.currentTimeMillis() - startTimeMs);

        Throwable error = errorContext.error();
        String errorMessage = error != null ? error.getMessage() : "unknown error";

        log.warn("[LLM TRACE: ERROR] runId={} latencyMs={} error={}", runId, latencyMs, errorMessage);

        if (isTracingEnabled()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("end_time", Instant.now().toString());
            payload.put("error", errorMessage);
            sendLangSmithTrace("/runs/" + runId, "PATCH", payload);
        }
    }

    protected void sendLangSmithTrace(String path, String method, Map<String, Object> payload) {
        try {
            String baseEndpoint = appProperties.tracing().endpoint().replaceAll("/+$", "");
            String body = objectMapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseEndpoint + path))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .header("x-api-key", appProperties.tracing().apiKey())
                    .method(method, HttpRequest.BodyPublishers.ofString(body))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .exceptionally(ex -> {
                        log.debug("[LLM TRACE] Failed exporting trace to LangSmith {}: {}", path, ex.getMessage());
                        return null;
                    });
        } catch (Exception e) {
            log.debug("[LLM TRACE] Could not serialize or dispatch LangSmith trace: {}", e.getMessage());
        }
    }

    public long getTotalRequests() {
        return totalRequests.get();
    }

    public long getTotalResponses() {
        return totalResponses.get();
    }

    public long getTotalErrors() {
        return totalErrors.get();
    }

    public long getTotalInputTokens() {
        return totalInputTokens.get();
    }

    public long getTotalOutputTokens() {
        return totalOutputTokens.get();
    }
}
