package com.streamingrag.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamingrag.agent.SreAnalystAgent;
import com.streamingrag.agent.SreTools;
import com.streamingrag.vector.LuceneEmbeddingStore;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.chat.StreamingChatLanguageModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import dev.langchain4j.model.ollama.OllamaChatModel;
import dev.langchain4j.model.ollama.OllamaStreamingChatModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.store.embedding.EmbeddingStore;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Enterprise LangChain4j configuration.
 * Configures the in-process ONNX embedding model, embedded Apache Lucene vector store,
 * Ollama Chat & Streaming models, and the declarative SreAnalystAgent AiService.
 */
@Configuration
public class LangChainConfig {

    private static final Logger log = LoggerFactory.getLogger(LangChainConfig.class);

    private final AppProperties appProperties;

    public LangChainConfig(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    /**
     * In-process ONNX Runtime embedding model.
     * Executes All-MiniLM-L6-v2 inside the JVM without any external Python dependency.
     */
    @Bean
    @ConditionalOnMissingBean(EmbeddingModel.class)
    public EmbeddingModel embeddingModel() {
        return new AllMiniLmL6V2EmbeddingModel();
    }

    /**
     * Embedded Apache Lucene vector store for storing dense vectors and querying with cosine k-NN search.
     * Operates completely in-process within the JVM (in-memory or persistent disk storage),
     * eliminating any external vector database container dependency.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(EmbeddingStore.class)
    public EmbeddingStore<TextSegment> embeddingStore(ObjectMapper objectMapper) throws IOException {
        Directory directory;
        if (appProperties.lucene().inMemory()) {
            log.info("[LANGCHAIN] Initializing in-memory Apache Lucene vector store (ByteBuffersDirectory)");
            directory = new ByteBuffersDirectory();
        } else {
            Path indexPath = Path.of(appProperties.lucene().indexPath());
            Files.createDirectories(indexPath);
            log.info("[LANGCHAIN] Initializing persistent Apache Lucene vector store at: {}", indexPath.toAbsolutePath());
            directory = FSDirectory.open(indexPath);
        }
        return new LuceneEmbeddingStore(directory, objectMapper);
    }

    /**
     * Ollama LLM client for synchronous reasoning and tool calling.
     */
    @Bean
    @ConditionalOnMissingBean(ChatLanguageModel.class)
    public ChatLanguageModel chatLanguageModel() {
        return OllamaChatModel.builder()
                .baseUrl(appProperties.ollama().baseUrl())
                .modelName(appProperties.ollama().modelName())
                .temperature(appProperties.ollama().temperature())
                .timeout(Duration.ofSeconds(appProperties.ollama().timeoutSeconds()))
                .build();
    }

    /**
     * Ollama streaming client for token-by-token SSE streaming.
     */
    @Bean
    @ConditionalOnMissingBean(StreamingChatLanguageModel.class)
    public StreamingChatLanguageModel streamingChatLanguageModel() {
        return OllamaStreamingChatModel.builder()
                .baseUrl(appProperties.ollama().baseUrl())
                .modelName(appProperties.ollama().modelName())
                .temperature(appProperties.ollama().temperature())
                .timeout(Duration.ofSeconds(appProperties.ollama().timeoutSeconds()))
                .build();
    }

    /**
     * Declarative AiService factory weaving LLM, tools, and system prompt.
     */
    @Bean
    @ConditionalOnMissingBean(SreAnalystAgent.class)
    public SreAnalystAgent sreAnalystAgent(
            ChatLanguageModel chatLanguageModel,
            StreamingChatLanguageModel streamingChatLanguageModel,
            SreTools sreTools) {
        return AiServices.builder(SreAnalystAgent.class)
                .chatLanguageModel(chatLanguageModel)
                .streamingChatLanguageModel(streamingChatLanguageModel)
                .tools(sreTools)
                .build();
    }
}
