package com.streamingrag.agent;

import com.streamingrag.api.dto.LogSource;
import com.streamingrag.consumer.LogIngestionConsumer;
import com.streamingrag.model.LogLevel;
import com.streamingrag.vector.VectorStoreRepository;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Tools available to the LangChain4j SRE Analyst Agent for semantic log search and incident statistics.
 */
@Component
public class SreTools {

    private static final Logger log = LoggerFactory.getLogger(SreTools.class);
    private static final int MAX_EXCERPT_LENGTH = 400;

    private final VectorStoreRepository vectorStoreRepository;
    private final LogIngestionConsumer logIngestionConsumer;
    private final ThreadLocal<List<LogSource>> currentSources = ThreadLocal.withInitial(ArrayList::new);

    public SreTools(
            VectorStoreRepository vectorStoreRepository,
            LogIngestionConsumer logIngestionConsumer) {
        this.vectorStoreRepository = vectorStoreRepository;
        this.logIngestionConsumer = logIngestionConsumer;
    }

    /**
     * Semantic vector search tool over ingested distributed system logs in Apache Lucene.
     *
     * @param query natural language description or error keywords to search
     * @param k     maximum number of relevant log matches to retrieve
     * @return formatted excerpts with metadata for LLM reasoning
     */
    @Tool("Searches Apache Lucene vector store for top-k logs matching the given natural language or error query")
    public String vectorSearch(
            @P("Search query containing incident symptoms, error codes, or microservice names") String query,
            @P("Number of log entries to retrieve (e.g. 5)") int k) {
        int limit = (k > 0) ? k : 5;
        log.info("[TOOL: vectorSearch] query='{}' limit={}", query, limit);

        List<EmbeddingMatch<TextSegment>> matches = vectorStoreRepository.search(query, limit, 0.0);
        if (matches.isEmpty()) {
            return "No matching log entries found in Apache Lucene for query: \"" + query + "\"";
        }

        List<LogSource> captured = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("Found ").append(matches.size()).append(" matching log entries:\n\n");

        for (int i = 0; i < matches.size(); i++) {
            EmbeddingMatch<TextSegment> match = matches.get(i);
            TextSegment segment = match.embedded();
            String fullText = segment.text();
            String excerpt = (fullText.length() > MAX_EXCERPT_LENGTH)
                    ? fullText.substring(0, MAX_EXCERPT_LENGTH) + "... [truncated]"
                    : fullText;

            String eventIdStr = segment.metadata().getString("event_id");
            UUID eventId = (eventIdStr != null) ? UUID.fromString(eventIdStr) : UUID.randomUUID();
            String service = segment.metadata().getString("service_name");
            String level = segment.metadata().getString("log_level");
            String timestampStr = segment.metadata().getString("timestamp");
            Instant timestamp = (timestampStr != null) ? Instant.parse(timestampStr) : Instant.now();

            captured.add(new LogSource(eventId, timestamp, service, level, excerpt, match.score()));

            sb.append("[").append(i + 1).append("] Score: ").append(String.format("%.3f", match.score()))
                    .append(" | Service: ").append(service)
                    .append(" | Level: ").append(level)
                    .append(" | Timestamp: ").append(timestamp)
                    .append(" | EventID: ").append(eventId)
                    .append("\nExcerpt:\n").append(excerpt)
                    .append("\n---\n");
        }

        currentSources.set(captured);
        return sb.toString();
    }

    /**
     * Statistics tool returning aggregated breakdown counts of recent logs by severity level and service.
     *
     * @param logLevel    optional filter by log level ('INFO', 'WARN', 'ERROR', or 'ALL')
     * @param serviceName optional filter by service name or 'ALL'
     * @param minutes     time window in minutes
     * @return human-readable statistics breakdown
     */
    @Tool("Returns aggregate log counts, error distributions and ingestion metrics for the given time window")
    public String logStats(
            @P("Log level filter ('INFO', 'WARN', 'ERROR', or 'ALL')") String logLevel,
            @P("Service name filter (e.g., 'user-auth', 'payment-gateway', or 'ALL')") String serviceName,
            @P("Analysis time window in minutes") int minutes) {
        log.info("[TOOL: logStats] logLevel='{}' service='{}' minutes={}", logLevel, serviceName, minutes);

        long total = logIngestionConsumer.getTotalIngestedCount();
        StringBuilder sb = new StringBuilder();
        sb.append("Log Ingestion Statistics (Window: ").append(minutes).append(" min):\n");
        sb.append("- Total Ingested Logs: ").append(total).append("\n");

        sb.append("- Counts by Severity Level:\n");
        for (LogLevel level : LogLevel.values()) {
            if ("ALL".equalsIgnoreCase(logLevel) || level.name().equalsIgnoreCase(logLevel)) {
                sb.append("  * ").append(level).append(": ").append(logIngestionConsumer.getCountForLevel(level)).append("\n");
            }
        }

        sb.append("- Counts by Microservice:\n");
        List<String> services = List.of("payment-gateway", "user-auth", "inventory-service", "frontend-bff");
        for (String svc : services) {
            if ("ALL".equalsIgnoreCase(serviceName) || svc.equalsIgnoreCase(serviceName)) {
                sb.append("  * ").append(svc).append(": ").append(logIngestionConsumer.getCountForService(svc)).append("\n");
            }
        }

        return sb.toString();
    }

    /**
     * Retrieves the citations captured during the current analysis execution.
     *
     * @return unmodifiable list of cited log sources
     */
    public List<LogSource> getCapturedSources() {
        List<LogSource> sources = currentSources.get();
        return (sources != null) ? Collections.unmodifiableList(sources) : List.of();
    }

    /**
     * Clears thread-local captured citations.
     */
    public void clearCapturedSources() {
        currentSources.remove();
    }
}
