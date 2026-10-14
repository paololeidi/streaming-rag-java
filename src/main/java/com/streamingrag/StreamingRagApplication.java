package com.streamingrag;

import com.streamingrag.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main entry point for the Enterprise Streaming RAG Java application.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(AppProperties.class)
public class StreamingRagApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamingRagApplication.class, args);
    }
}
