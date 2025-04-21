package com.diaco.aranegar.repository;

import com.diaco.aranegar.model.document.AranegarDocument;
import org.springframework.data.elasticsearch.repository.ReactiveElasticsearchRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface AranegarRepository extends ReactiveElasticsearchRepository<AranegarDocument, String> {

    Flux<AranegarDocument> findByUsername(String username);
    Flux<AranegarDocument> findByUsernameOrderByCreateTimeDesc(String username);
    Mono<AranegarDocument> findBySessionId(String sessionId);
}
