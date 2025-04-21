package com.diaco.aranegar.service;

import com.diaco.aranegar.model.document.AranegarDocument;
import com.diaco.aranegar.repository.AranegarRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchService {

    private final AranegarRepository aranegarRepository;

    public Mono<AranegarDocument> saveInitialDocument(String username, String sessionId, String requestFilePath,
                                                      String requestFileName) {
        AranegarDocument document = AranegarDocument.builder()
                .sessionId(sessionId)
                .username(username)
                .referenceImagePath(requestFilePath)
                .title(requestFileName)
                .referenceImageName(requestFileName)
                .isCompleted(false)
                .createTime(System.currentTimeMillis())
                .build();

        log.info("Saving document to Elasticsearch: {}", document);

        return aranegarRepository.save(document)
                .doOnSuccess(savedDoc -> log.info("Document successfully saved: {}", savedDoc))
                .doOnError(error -> log.error("Error saving document to Elasticsearch", error));
    }

    public Mono<Void> updateCompletedDocument(String sessionId, String resultFilePath) {
        return aranegarRepository.findBySessionId(sessionId)
                .flatMap(doc -> {
                    doc.setIsCompleted(true);

                    doc.setResultImagePath(resultFilePath);
                    doc.setUpdateTime(Instant.now().toEpochMilli());
                    return aranegarRepository.save(doc)
                            .doOnSuccess(doc1 -> log.info("Document is completed for sessionId: {}", sessionId));
                })
                .switchIfEmpty(
                        Mono.defer(() -> {
                            log.info("No document found for sessionId: {}.", sessionId);
                            return Mono.empty();
                        })
                )
                .then();
    }
}
