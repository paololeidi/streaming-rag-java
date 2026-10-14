package com.streamingrag.api;

import com.streamingrag.agent.SreAnalystAgent;
import com.streamingrag.agent.SreTools;
import com.streamingrag.api.dto.LogSource;
import com.streamingrag.api.dto.QueryRequest;
import com.streamingrag.api.dto.QueryResponse;
import dev.langchain4j.service.TokenStream;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * REST controller exposing synchronous and SSE streaming endpoints for Agentic RAG log analysis.
 */
@RestController
@RequestMapping("/api/v1/query")
public class QueryController {

    private static final Logger log = LoggerFactory.getLogger(QueryController.class);
    private static final long SSE_TIMEOUT_MS = 180_000L; // 3 minutes

    private final SreAnalystAgent agent;
    private final SreTools sreTools;

    public QueryController(SreAnalystAgent agent, SreTools sreTools) {
        this.agent = agent;
        this.sreTools = sreTools;
    }

    /**
     * Synchronous Agentic query endpoint.
     * Executes the agent ReAct loop with tool calling and returns answer with structured citations.
     */
    @PostMapping
    public ResponseEntity<QueryResponse> query(@Valid @RequestBody QueryRequest request) {
        log.info("[API: /api/v1/query] Received query prompt='{}' k={}", request.prompt(), request.getEffectiveK());

        sreTools.clearCapturedSources();
        try {
            String answer = agent.query(request.prompt());
            List<LogSource> sources = sreTools.getCapturedSources();
            QueryResponse response = new QueryResponse(answer, sources, Instant.now());
            return ResponseEntity.ok(response);
        } finally {
            sreTools.clearCapturedSources();
        }
    }

    /**
     * Real-time Server-Sent Events (SSE) streaming endpoint.
     * Streams LLM tokens as they are generated and terminates with a '[DONE]' event.
     */
    @PostMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamQuery(@Valid @RequestBody QueryRequest request) {
        log.info("[API: /api/v1/query/stream] Starting SSE stream for prompt='{}'", request.prompt());

        SseEmitter emitter = new SseEmitter(SSE_TIMEOUT_MS);

        Thread.startVirtualThread(() -> {
            try {
                TokenStream tokenStream = null;
                try {
                    tokenStream = agent.queryStream(request.prompt());
                } catch (IllegalArgumentException e) {
                    log.info("Direct TokenStream not supported with tools for this model, streaming agent output: {}", e.getMessage());
                }

                if (tokenStream != null) {
                    tokenStream
                            .onNext(token -> {
                                try {
                                    emitter.send(SseEmitter.event().data(token));
                                } catch (IOException e) {
                                    log.debug("Client disconnected during SSE stream: {}", e.getMessage());
                                    emitter.completeWithError(e);
                                }
                            })
                            .onComplete(response -> {
                                try {
                                    emitter.send(SseEmitter.event().data("[DONE]"));
                                    emitter.complete();
                                } catch (IOException e) {
                                    emitter.completeWithError(e);
                                }
                            })
                            .onError(emitter::completeWithError)
                            .start();
                } else {
                    // Fallback: execute agent query on virtual thread and stream synthesized tokens
                    String answer = agent.query(request.prompt());
                    String[] words = answer.split("(?<=\\s+)");
                    for (String word : words) {
                        emitter.send(SseEmitter.event().data(word));
                        Thread.sleep(15);
                    }
                    emitter.send(SseEmitter.event().data("[DONE]"));
                    emitter.complete();
                }
            } catch (Exception e) {
                log.error("[API: /api/v1/query/stream ERROR] Streaming failed: {}", e.getMessage(), e);
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }
}
