package com.streamingrag.vector;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2.AllMiniLmL6V2EmbeddingModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OnnxEmbeddingModelTest {

    @Test
    @DisplayName("Should initialize AllMiniLmL6V2EmbeddingModel in-process and generate 384-dim vector")
    void shouldInitializeOnnxModelInProcess() {
        EmbeddingModel embeddingModel = new AllMiniLmL6V2EmbeddingModel();

        String sampleLogText = "[ERROR] payment-gateway @ 2026-10-12T10:00:00Z\nStripe API connection timeout";
        Embedding embedding = embeddingModel.embed(sampleLogText).content();

        assertThat(embedding).isNotNull();
        // All-MiniLM-L6-v2 produces dense vectors of dimension 384
        assertThat(embedding.dimension()).isEqualTo(384);
        assertThat(embedding.vector()).hasSize(384);
    }
}
