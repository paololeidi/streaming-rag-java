package com.streamingrag.api;

import com.streamingrag.agent.SreAnalystAgent;
import com.streamingrag.agent.SreTools;
import com.streamingrag.api.dto.LogSource;
import com.streamingrag.api.dto.QueryRequest;
import com.streamingrag.api.dto.QueryResponse;
import dev.langchain4j.service.TokenStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryControllerTest {

    @Mock
    private SreAnalystAgent agent;

    @Mock
    private SreTools sreTools;

    @Mock
    private TokenStream tokenStream;

    private QueryController queryController;

    @BeforeEach
    void setUp() {
        queryController = new QueryController(agent, sreTools);
    }

    @Test
    @DisplayName("Should process synchronous query and return QueryResponse with citations")
    void shouldHandleSynchronousQuery() {
        UUID eventId = UUID.randomUUID();
        LogSource source = new LogSource(eventId, Instant.now(), "payment-gateway", "ERROR", "Timeout", 0.95);

        when(agent.query(anyString())).thenReturn("Root cause is Stripe API timeout.");
        when(sreTools.getCapturedSources()).thenReturn(List.of(source));

        QueryRequest request = new QueryRequest("Why did checkout fail?", 5);
        ResponseEntity<QueryResponse> response = queryController.query(request);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().answer()).isEqualTo("Root cause is Stripe API timeout.");
        assertThat(response.getBody().sources()).hasSize(1);
        assertThat(response.getBody().sources().get(0).eventId()).isEqualTo(eventId);

        verify(agent).query("Why did checkout fail?");
        verify(sreTools, atLeastOnce()).clearCapturedSources();
    }

    @Test
    @DisplayName("Should initiate SSE streaming query")
    void shouldHandleStreamingQuery() {
        when(agent.queryStream(anyString())).thenReturn(tokenStream);
        when(tokenStream.onNext(any())).thenReturn(tokenStream);
        when(tokenStream.onComplete(any())).thenReturn(tokenStream);
        when(tokenStream.onError(any())).thenReturn(tokenStream);

        QueryRequest request = new QueryRequest("Stream incident report", 5);
        SseEmitter emitter = queryController.streamQuery(request);

        assertThat(emitter).isNotNull();
        verify(agent, timeout(2000)).queryStream("Stream incident report");
        verify(tokenStream, timeout(2000)).start();
    }
}
