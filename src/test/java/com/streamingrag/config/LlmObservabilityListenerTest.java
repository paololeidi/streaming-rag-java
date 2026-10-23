package com.streamingrag.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.listener.ChatModelErrorContext;
import dev.langchain4j.model.chat.listener.ChatModelRequest;
import dev.langchain4j.model.chat.listener.ChatModelRequestContext;
import dev.langchain4j.model.chat.listener.ChatModelResponse;
import dev.langchain4j.model.chat.listener.ChatModelResponseContext;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LlmObservabilityListenerTest {

    @Mock
    private HttpClient httpClient;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    private AppProperties createProperties(boolean tracingEnabled, String apiKey) {
        return new AppProperties(
                new AppProperties.KafkaProperties("system-logs", "system-logs-dlq", "group", "localhost:9092", 1, (short) 1),
                new AppProperties.ProducerProperties(false, 1000L),
                new AppProperties.LuceneProperties("./data/lucene", true, 5),
                new AppProperties.OllamaProperties("http://localhost:11434", "llama3.2:3b", 0.0, 10),
                new AppProperties.McpProperties("", 5),
                new AppProperties.TracingProperties(tracingEnabled, "https://api.smith.langchain.com", apiKey, "streaming-rag-java")
        );
    }

    @Test
    @DisplayName("Should record request, response, error, and token counters without calling LangSmith when disabled")
    void shouldTrackTelemetryLocallyWhenLangSmithDisabled() {
        LlmObservabilityListener listener = new LlmObservabilityListener(
                createProperties(false, ""), objectMapper, httpClient);

        Map<Object, Object> attributes = new HashMap<>();
        ChatModelRequest request = ChatModelRequest.builder()
                .model("llama3.2:3b")
                .temperature(0.0)
                .messages(List.of(UserMessage.from("Why did payment-gateway fail?")))
                .build();

        ChatModelRequestContext reqCtx = new ChatModelRequestContext(request, attributes);
        listener.onRequest(reqCtx);

        assertThat(attributes).containsKeys(
                LlmObservabilityListener.ATTR_RUN_ID,
                LlmObservabilityListener.ATTR_START_TIME_MS
        );
        assertThat(listener.getTotalRequests()).isEqualTo(1L);

        ChatModelResponse response = ChatModelResponse.builder()
                .id("resp-1")
                .model("llama3.2:3b")
                .tokenUsage(new TokenUsage(120, 45, 165))
                .finishReason(FinishReason.STOP)
                .aiMessage(AiMessage.from("Root cause is Stripe timeout."))
                .build();

        ChatModelResponseContext respCtx = new ChatModelResponseContext(response, request, attributes);
        listener.onResponse(respCtx);

        assertThat(listener.getTotalResponses()).isEqualTo(1L);
        assertThat(listener.getTotalInputTokens()).isEqualTo(120L);
        assertThat(listener.getTotalOutputTokens()).isEqualTo(45L);

        ChatModelErrorContext errCtx = new ChatModelErrorContext(
                new RuntimeException("Simulated timeout"), request, null, attributes);
        listener.onError(errCtx);
        assertThat(listener.getTotalErrors()).isEqualTo(1L);

        verifyNoInteractions(httpClient);
    }

    @Test
    @DisplayName("Should export run traces to LangSmith when LANGCHAIN_TRACING_V2 is enabled with API key")
    @SuppressWarnings("unchecked")
    void shouldExportTracesToLangSmithWhenEnabled() {
        when(httpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(HttpResponse.class)));

        LlmObservabilityListener listener = new LlmObservabilityListener(
                createProperties(true, "lsv2_test_key"), objectMapper, httpClient);

        assertThat(listener.isTracingEnabled()).isTrue();

        Map<Object, Object> attributes = new HashMap<>();
        ChatModelRequest request = ChatModelRequest.builder()
                .model("llama3.2:3b")
                .messages(List.of(UserMessage.from("Test trace")))
                .build();

        listener.onRequest(new ChatModelRequestContext(request, attributes));

        ChatModelResponse response = ChatModelResponse.builder()
                .id("resp-2")
                .model("llama3.2:3b")
                .tokenUsage(new TokenUsage(50, 20, 70))
                .finishReason(FinishReason.STOP)
                .aiMessage(AiMessage.from("Traced answer"))
                .build();

        listener.onResponse(new ChatModelResponseContext(response, request, attributes));

        verify(httpClient, times(2)).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }
}
