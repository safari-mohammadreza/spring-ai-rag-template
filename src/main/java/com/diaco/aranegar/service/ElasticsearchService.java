package com.diaco.aranegar.service;

import com.diaco.aranegar.model.document.AranegarDocument;
import com.diaco.aranegar.repository.AranegarRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchService {

    private final AranegarRepository aranegarRepository;
    private final ReactiveElasticsearchOperations elasticsearchOperations;

    public Mono<AranegarDocument> saveInitialDocument(String username, String sessionId,
                                                      String editableImageName, String editableImagePath,
                                                      String referenceImageName, String referenceImagePath,
                                                      String editedImageName, String editedImagePath,
                                                      String maskImageName, String maskImagePath) {
        AranegarDocument document = AranegarDocument.builder()
                .sessionId(sessionId)
                .username(username)
                .referenceImageName(referenceImageName)
                .editableImageName(editableImageName)
                .editedImageName(editedImageName)
                .maskImageName(maskImageName)
                .referenceImagePath(referenceImagePath)
                .editableImagePath(editableImagePath)
                .editedImagePath(editedImagePath)
                .maskImagePath(maskImagePath)
                .title("new_file")
                .isCompleted(false)
                .createTime(System.currentTimeMillis())
                .build();

        log.info("Saving document to Elasticsearch: {}", document);

        return aranegarRepository.save(document)
                .doOnSuccess(savedDoc -> log.info("Document successfully saved: {}", savedDoc))
                .doOnError(error -> log.error("Error saving document to Elasticsearch", error));
    }

    public Mono<Void> updateCompletedDocument(String sessionId, String resultImagePath) {

        // Preprocess the resultImagePath to remove leading '/'
        if (resultImagePath != null && resultImagePath.startsWith("/")) {
            resultImagePath = resultImagePath.substring(1); // Remove the leading '/'
        }

        String finalResultImagePath = resultImagePath;
        return elasticsearchOperations
                .indexOps(AranegarDocument.class)
                .refresh()
                .then(
                        Mono.defer(() -> aranegarRepository.findBySessionId(sessionId))
                                .switchIfEmpty(Mono.defer(() -> {
                                    log.warn("No document found even after refresh for sessionId={}", sessionId);
                                    return Mono.empty();
                                }))
                                .flatMap(doc -> {
                                    doc.setIsCompleted(true);
                                    doc.setResultImagePath(finalResultImagePath);
                                    doc.setUpdateTime(Instant.now().toEpochMilli());
                                    return aranegarRepository.save(doc)
                                            .doOnSuccess(d -> log.info("Completed doc for {}", sessionId));
                                })
                )
                .then();
    }
}
