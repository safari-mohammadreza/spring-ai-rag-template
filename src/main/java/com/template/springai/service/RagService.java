package com.template.springai.service;

import lombok.RequiredArgsConstructor;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class RagService {
    private final VectorStore vectorStore;
    private final ChatModel chatModel;

    public void addDocument(String text) {
        Document textDoc = Document.builder().text(text).metadata("source", "user").build();
        vectorStore.add(List.of(textDoc));
    }

    public String askWithContext(String question, int topK) {
        SearchRequest request = SearchRequest.builder().query(question).topK(topK).build();
        List<Document> hits = vectorStore.similaritySearch(request);

        String context = hits.stream()
                .map(Document::getFormattedContent)
                .collect(Collectors.joining(" --- "));
        String systemText = "You are an assistant that answers questions based ONLY on the provided context. " +
                "If the answer is not in the context, say you don't know. Be concise.";
        String userText = "Context: " + context + " Question: " + question;

        Prompt prompt = new Prompt(List.of(new SystemMessage(systemText), new UserMessage(userText)));

        ChatResponse response = chatModel.call(prompt);

        return response.getResult().getOutput().getText();
    }
}