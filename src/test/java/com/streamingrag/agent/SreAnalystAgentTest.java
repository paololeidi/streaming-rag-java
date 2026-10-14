package com.streamingrag.agent;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.output.Response;
import dev.langchain4j.service.AiServices;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SreAnalystAgentTest {

    @Mock
    private ChatLanguageModel chatLanguageModel;

    @Test
    @DisplayName("Should build SreAnalystAgent and execute query using ChatLanguageModel")
    void shouldBuildAndExecuteAgent() {
        when(chatLanguageModel.generate(anyList()))
                .thenReturn(Response.from(AiMessage.from("Incident report: service is healthy.")));

        SreAnalystAgent agent = AiServices.builder(SreAnalystAgent.class)
                .chatLanguageModel(chatLanguageModel)
                .build();

        String answer = agent.query("Check status of user-auth");

        assertThat(answer).isEqualTo("Incident report: service is healthy.");
    }
}
