package com.streamingrag.agent;

import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.service.TokenStream;
import dev.langchain4j.service.UserMessage;

/**
 * Declarative LangChain4j AI Service interface for autonomous Site Reliability Engineering (SRE) log analysis.
 */
public interface SreAnalystAgent {

    @SystemMessage(Prompts.SRE_SYSTEM_PROMPT)
    String query(@UserMessage String userMessage);

    @SystemMessage(Prompts.SRE_SYSTEM_PROMPT)
    TokenStream queryStream(@UserMessage String userMessage);
}
