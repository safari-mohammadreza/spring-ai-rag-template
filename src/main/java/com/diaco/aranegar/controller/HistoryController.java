package com.diaco.aranegar.controller;

import com.diaco.aranegar.model.document.AranegarDocument;
import com.diaco.aranegar.model.dto.AranegarDto;
import com.diaco.aranegar.model.dto.GenericResponseDto;
import com.diaco.aranegar.model.dto.HistoryDto;
import com.diaco.aranegar.model.dto.UpdateHistoryRequestDto;
import com.diaco.aranegar.model.enums.ResultEnum;
import com.diaco.aranegar.repository.AranegarRepository;
import com.diaco.aranegar.service.MinIOService;
import com.diaco.aranegar.util.JwtUtils;
import com.diaco.aranegar.util.Utils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/aranegar/documents")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin
public class HistoryController {

    private final AranegarRepository transcriptionRepository;
    private final ReactiveElasticsearchOperations elasticsearchOperations;
    private final JwtUtils jwtUtils;
    private final MinIOService minioService;
    private final Utils utils;

    @GetMapping()
    public Mono<ResponseEntity<GenericResponseDto<List<HistoryDto>>>> getDocumentsByUsername(
            @RequestHeader(name = "Authorization") String token) {

        String username = jwtUtils.getUserNameFromJwtToken(token);
        log.info("Fetching documents for username: {}", username);

        return transcriptionRepository.findByUsernameOrderByCreateTimeDesc(username)
                .map(document -> HistoryDto.builder()
                        .id(document.getSessionId())
                        .title(document.getTitle())
                        .createTime(utils.longToZonedDateTime(document.getCreateTime()))
                        .build()
                )
                .collectList()
                .map(docList -> ResponseEntity.ok(GenericResponseDto.success(docList)))
                .onErrorResume(error -> {
                    log.error("Error fetching documents for username: {}", username, error);
                    return Mono.just(
                            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                                    .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION,
                                            "Failed to fetch documents"))
                    );
                });
    }

    @GetMapping("/{documentId}")
    public Mono<ResponseEntity<GenericResponseDto<AranegarDto>>> getDocumentById(
            @PathVariable String documentId, @RequestHeader(name = "Authorization") String token
    ) {
        log.info("Fetching document with ID: {}", documentId);

        String caller = jwtUtils.getUserNameFromJwtToken(token);

        return transcriptionRepository.findById(documentId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Document not found")))
                .flatMap(document -> {
                    if (!caller.equals(document.getUsername())) {
                        log.error("Caller user is not the owner of this document!");
                        return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND)
                                .body(GenericResponseDto.<AranegarDto>failure(ResultEnum.GENERAL_NOT_FOUND,
                                        "Access denied")));
                    }

                    String username = document.getUsername();
                    String sessionId = document.getSessionId();

                    String referenceImagePath = minioService.generateMinIoFilePath(username, sessionId,
                            document.getReferenceImageName());
                    String editableImagePath = minioService.generateMinIoFilePath(username, sessionId,
                            document.getEditableImageName());
                    String resultImagePath = minioService.generateMinIoFilePath(username, sessionId,
                            document.getResultImageName());

                    Mono<String> referenceImageUrlMono = minioService.generateFileUrl(referenceImagePath);
                    Mono<String> editableImageUrlMono = minioService.generateFileUrl(editableImagePath);
                    Mono<String> resultImageUrlMono = minioService.generateFileUrl(resultImagePath);

                    return Mono.zip(referenceImageUrlMono, editableImageUrlMono, resultImageUrlMono)
                            .map(tuple -> {
                                String referenceFileUrl = tuple.getT1();
                                String editableFileUrl = tuple.getT2();
                                String resultFileUrl = tuple.getT3();

                                AranegarDto dto = AranegarDto.builder()
                                        .id(sessionId)
                                        .username(username)
                                        .referenceImageName(document.getReferenceImageName())
                                        .editableImageName(document.getEditableImageName())
                                        .resultImageName(document.getResultImageName())
                                        .createTime(utils.longToZonedDateTime(document.getCreateTime()))
                                        .referenceImageUrl(referenceFileUrl)
                                        .editableImageUrl(editableFileUrl)
                                        .resultImageUrl(resultFileUrl)
                                        .isCompleted(document.getIsCompleted())
                                        .build();

                                return ResponseEntity.ok(GenericResponseDto.success(dto));
                            });

                })
                .onErrorResume(ResponseStatusException.class, error -> {
                    log.error("Document not found: {}", documentId, error);
                    return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(GenericResponseDto.failure(ResultEnum.GENERAL_NOT_FOUND,
                                    "Document not found")));
                })
                .onErrorResume(error -> {
                    log.error("Error fetching document or generating audio URL", error);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION,
                                    "Failed to fetch document")));
                });
    }

    @DeleteMapping("/{sessionId}")
    public Mono<ResponseEntity<GenericResponseDto<Boolean>>> deleteHistory(
            @PathVariable String sessionId,
            @RequestHeader(name = "Authorization") String token) {

        String username = jwtUtils.getUserNameFromJwtToken(token);
        log.info("Received request to delete history. sessionId: {}, username: {}", sessionId, username);

        return transcriptionRepository.findById(sessionId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Document not found")))
                .flatMap(document -> {
                    if (!document.getUsername().equals(username)) {
                        log.error("User '{}' is not the owner of session '{}'", username, sessionId);
                        return Mono.error(new ResponseStatusException(HttpStatus.FORBIDDEN,
                                "You are not the owner of this document"));
                    }

                    return transcriptionRepository.deleteById(sessionId)
                            .then(elasticsearchOperations.indexOps(AranegarDocument.class).refresh())
                            .then(minioService.deleteFile(document.getReferenceImagePath()))
                            .then(minioService.deleteFile(document.getEditableImagePath()))
                            .then(minioService.deleteFile(document.getResultImagePath()))
                            .thenReturn(true);
                })
                .map(deleted -> ResponseEntity.ok(GenericResponseDto.success(true)))
                .onErrorResume(ResponseStatusException.class, ex -> {
                    log.warn("Request failed: {}", ex.getReason());
                    return Mono.just(ResponseEntity.status(ex.getStatusCode())
                            .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION, ex.getReason())));
                })
                .onErrorResume(error -> {
                    log.error("Unexpected error occurred while deleting history for sessionId: {}", sessionId, error);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION,
                                    "Unexpected error occurred.")));
                });
    }

    @DeleteMapping()
    public Mono<ResponseEntity<GenericResponseDto<Boolean>>> deleteAllUserHistory(
            @RequestHeader(name = "Authorization") String token) {

        String username = jwtUtils.getUserNameFromJwtToken(token);
        log.info("Received request to delete all history for username: {}", username);

        return transcriptionRepository.findByUsername(username)
                .collectList()
                .flatMap(documents -> {
                    if (documents.isEmpty()) {
                        log.warn("No documents found for user: {}", username);
                        return Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "No documents found"));
                    }

                    // Delete all documents and associated files
                    return Flux.fromIterable(documents)
                            .flatMap(document ->
                                    minioService.deleteFile(document.getResultImagePath())
                                            .then(minioService.deleteFile(document.getReferenceImagePath()))
                                            .then(minioService.deleteFile(document.getEditableImagePath()))
                                            .then(transcriptionRepository.deleteById(document.getSessionId()))
                                            .then(elasticsearchOperations.indexOps(AranegarDocument.class).refresh())
                            )
                            .then(Mono.just(true));
                })
                .map(deleted -> ResponseEntity.ok(GenericResponseDto.success(true)))
                .onErrorResume(ResponseStatusException.class, ex -> {
                    log.warn("Request failed: {}", ex.getReason());
                    return Mono.just(ResponseEntity.status(ex.getStatusCode())
                            .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION, ex.getReason())));
                })
                .onErrorResume(error -> {
                    log.error("Unexpected error occurred while deleting history for user: {}", username, error);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION,
                                    "Unexpected error occurred.")));
                });
    }

    //rename
    @PutMapping("/{sessionId}")
    public Mono<ResponseEntity<GenericResponseDto<Boolean>>> updateAudioFileName(
            @PathVariable String sessionId,
            @RequestBody UpdateHistoryRequestDto request,
            @RequestHeader(name = "Authorization") String token
    ) {

        String username = jwtUtils.getUserNameFromJwtToken(token);
        log.info("Received request to update audio file name. sessionId: {}, username: {}, newTitle: {}",
                sessionId, username, request.getNewTitle());

        if (containsControlCharacters(request.getNewTitle())) {
            log.error("Invalid newTitle: contains control characters. sessionId: {}", sessionId);
            return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(GenericResponseDto.failure(ResultEnum.INVALID_INPUT,
                            "File name contains invalid control characters.")));
        }

        return transcriptionRepository.findById(sessionId)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Document not found")))
                .flatMap(document -> {
                    if (!document.getUsername().equals(username)) {
                        log.error("User '{}' is not the owner of session '{}'", username, sessionId);
                        return Mono.error(new ResponseStatusException(HttpStatus.FORBIDDEN,
                                "You are not the owner of this document"));
                    }

                    document.setTitle(request.getNewTitle());

                    return transcriptionRepository.save(document)
                            .then(elasticsearchOperations.indexOps(AranegarDocument.class).refresh())
                            .thenReturn(ResponseEntity.ok(GenericResponseDto.success(true)));
                })
                .onErrorResume(ResponseStatusException.class, ex -> Mono.just(
                        ResponseEntity.status(ex.getStatusCode())
                                .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION, ex.getReason()))))
                .onErrorResume(error -> Mono.just(
                        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                                .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION, "Unexpected error occurred."))));
    }

    private boolean containsControlCharacters(String input) {
        return input.chars().anyMatch(Character::isISOControl);
    }

}