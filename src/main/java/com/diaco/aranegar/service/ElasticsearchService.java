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

    private final int HISTORY_LIMITATION = 20;

    public Mono<AranegarDocument> saveInitialDocument(
            String username,
            String sessionId,
            String editableImageName, String editableImagePath,
            String referenceImageName, String referenceImagePath,
            String editedImageName, String editedImagePath,
            String maskImageName, String maskImagePath, String resultFilename) {

        AranegarDocument document = AranegarDocument.builder()
                .sessionId(sessionId)
                .username(username)
                .referenceImageName(referenceImageName)
                .editableImageName(editableImageName)
                .editedImageName(editedImageName)
                .resultImageName(resultFilename)
                .maskImageName(maskImageName)
                .referenceImagePath(referenceImagePath)
                .editableImagePath(editableImagePath)
                .editedImagePath(editedImagePath)
                .maskImagePath(maskImagePath)
                .title("new_file")
                .isCompleted(false)
                .createTime(System.currentTimeMillis())
                .build();

        log.info("Saving document to Elasticsearch for user={}", username);

        Mono<Void> evictionMono = aranegarRepository.countByUsername(username)
                .flatMap(count -> {
                    if (count >= HISTORY_LIMITATION) {
                        log.info("User {} has {} docs, removing oldest before insert", username, count);
                        return aranegarRepository
                                .findByUsernameOrderByCreateTimeAsc(username)
                                .next()
                                .flatMap(oldest -> {
                                    log.info("Deleting oldest doc id={} createTime={}", oldest.getSessionId(),
                                            oldest.getCreateTime());
                                    return aranegarRepository.deleteById(oldest.getSessionId());
                                });
                    } else {
                        return Mono.empty();
                    }
                })
                .then();

        return evictionMono
                .then(aranegarRepository.save(document))
                .doOnSuccess(saved -> log.info("Document successfully saved (post-eviction): {}", saved.getSessionId()))
                .doOnError(err   -> log.error("Error in saveInitialDocument", err));
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
