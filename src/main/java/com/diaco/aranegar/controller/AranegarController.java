package com.diaco.aranegar.controller;

import com.diaco.aranegar.base.exception.FileNotFoundException;
import com.diaco.aranegar.model.dto.FromAIProgressDto;
import com.diaco.aranegar.model.dto.FromAIResultDto;
import com.diaco.aranegar.model.dto.GeneralMessageDto;
import com.diaco.aranegar.model.dto.GenericResponseDto;
import com.diaco.aranegar.model.enums.ResultEnum;
import com.diaco.aranegar.service.ElasticsearchService;
import com.diaco.aranegar.service.MinIOService;
import com.diaco.aranegar.service.RabbitMQService;
import com.diaco.aranegar.util.JwtUtils;
import com.diaco.aranegar.util.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/aranegar")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin
public class AranegarController {

    private final ElasticsearchService elasticsearchService;
    private final RabbitMQService rabbitMQService;
    private final ObjectMapper objectMapper;
    private final JwtUtils jwtUtils;
    private final MinIOService minioService;
    private final Utils fileUtils;

    private final Map<String, Sinks.Many<String>> sinkMap = new HashMap<>();
    private final int MAX_FILE_SIZE = 5 * 1024 * 1024;


    @PostMapping("/process")
    public Mono<ResponseEntity<GenericResponseDto<String>>> processFile(
            @RequestPart("referenceImage") Mono<FilePart> referenceImageMono,
            @RequestPart("maskImage") Mono<FilePart> maskImageMono,
            @RequestPart("editableImage") Mono<FilePart> editableImageMono,
            @RequestPart("editedImage") Mono<FilePart> editedImageMono,
            @RequestHeader(name = "Authorization") String token) {

        String username = jwtUtils.getUserNameFromJwtToken(token);
        String sessionId = username.concat(UUID.randomUUID().toString());

        log.info("Received request to upload images. sessionId: {}, username: {}", sessionId, username);

        sinkMap.computeIfAbsent(sessionId,
                id -> Sinks.many().multicast().onBackpressureBuffer());

        return Mono.zip(referenceImageMono, editableImageMono, editedImageMono, maskImageMono)
                .flatMap(tuple -> {
                    FilePart referenceImagePart = tuple.getT1();
                    FilePart editableImagePart = tuple.getT2();
                    FilePart editedImagePart = tuple.getT3();
                    FilePart maskImagePart = tuple.getT4();

                    // 1. Validate referenceImage
                    Mono<ResponseEntity<GenericResponseDto<String>>> referenceValidation = validatePart(
                            referenceImagePart,
                            "ref",
                            ResultEnum.INVALID_INPUT,
                            sessionId
                    );

                    // 2. Validate editableImage
                    Mono<ResponseEntity<GenericResponseDto<String>>> editableValidation = validatePart(
                            editableImagePart,
                            "editable",
                            ResultEnum.INVALID_INPUT,
                            sessionId
                    );

                    // 3. Validate editedImage (same rules)
                    Mono<ResponseEntity<GenericResponseDto<String>>> editedValidation = validatePart(
                            editedImagePart,
                            "edited",
                            ResultEnum.INVALID_INPUT,
                            sessionId
                    );

                    // 3. Validate maskImage (same rules)
                    Mono<ResponseEntity<GenericResponseDto<String>>> maskValidation = validatePart(
                            maskImagePart,
                            "mask",
                            ResultEnum.INVALID_INPUT,
                            sessionId
                    );

                    // 4. Sequence validations, then process upload
                    return referenceValidation
                            .flatMap(Mono::just)
                            .switchIfEmpty(
                                    editableValidation
                                            .flatMap(Mono::just)
                                            .switchIfEmpty(
                                                    editedValidation
                                                            .flatMap(Mono::just)
                                                            .switchIfEmpty(
                                                                    maskValidation
                                                                            .flatMap(Mono::just)
                                                                            .switchIfEmpty(
                                                                                    processFileUpload(
                                                                                            referenceImagePart,
                                                                                            editableImagePart,
                                                                                            editedImagePart,
                                                                                            maskImagePart,
                                                                                            sessionId,
                                                                                            username)
                                                                            )
                                                            )
                                            )
                            );
                })
                .onErrorResume(error -> {
                    log.error("Unexpected error during upload, session {}: {}", sessionId, error.getMessage(), error);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(GenericResponseDto.failure(
                                    ResultEnum.GENERAL_EXCEPTION,
                                    "Unexpected error occurred.")));
                });
    }

    /**
     * Common validation logic for each FilePart.
     *
     * @param part      the incoming FilePart
     * @param label     a label for logging (e.g. "face", "file", "edited")
     * @param result    the ResultEnum to use on failure
     * @param sessionId the current session id for logs
     */
    private Mono<ResponseEntity<GenericResponseDto<String>>> validatePart(
            FilePart part,
            String label,
            ResultEnum result,
            String sessionId) {
        String ext = fileUtils.getFileExtension(part.filename());
        if (!fileUtils.isValidFileFormat(ext)) {
            log.error("Invalid {} extension {} for session {}", label, ext, sessionId);
            return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(GenericResponseDto.<String>failure(
                            result,
                            StringUtils.capitalize(label) + " file format is not supported.")));
        }

        return part.content()
                .map(DataBuffer::readableByteCount)
                .reduce(0L, Long::sum)
                .flatMap(size -> {
                    if (size > MAX_FILE_SIZE) {
                        log.error("{} image too large ({} bytes) for session {}", label, size, sessionId);
                        return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                .body(GenericResponseDto.<String>failure(
                                        result,
                                        StringUtils.capitalize(label) + " file exceeds size limit.")));
                    }
                    return fileUtils.detectMimeType(part)
                            .flatMap(mime -> {
                                if (!mime.startsWith("image/")) {
                                    log.error("{} MIME {} invalid for session {}", label, mime, sessionId);
                                    return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                            .body(GenericResponseDto.<String>failure(
                                                    result,
                                                    StringUtils.capitalize(label) + " must be an image.")));
                                }
                                return Mono.empty();
                            });
                });
    }

    private Mono<ResponseEntity<GenericResponseDto<String>>> processFileUpload(
            FilePart referenceImage,
            FilePart editableImage,
            FilePart editedImage,
            FilePart maskImage,
            String sessionId,
            String username) {

        // 1. Generate MinIO paths
        String referenceImageName = "ref_".concat(referenceImage.filename());
        String referenceImagePath = minioService.generateMinIoFilePath(username, sessionId, referenceImageName);

        String editableImageName = "editable_".concat(editableImage.filename());
        String editableImagePath = minioService.generateMinIoFilePath(username, sessionId, editableImageName);

        String editedImageName = "edited_".concat(editedImage.filename());
        String editedImagePath = minioService.generateMinIoFilePath(username, sessionId, editedImageName);

        String maskImageName = "mask_".concat(maskImage.filename());
        String maskImagePath = minioService.generateMinIoFilePath(username, sessionId, maskImageName);

        log.info("Uploading to MinIO...");

        // 2. Upload both in parallel, then get their URLs
        Mono<String> referenceUrlMono = minioService.saveFileToMinIO(referenceImagePath, referenceImage)
                .then(minioService.generateFileUrl(referenceImagePath));

        Mono<String> editableUrlMono = minioService.saveFileToMinIO(editableImagePath, editableImage)
                .then(minioService.generateFileUrl(editableImagePath));

        Mono<String> editedUrlMono = minioService.saveFileToMinIO(editedImagePath, editedImage)
                .then(minioService.generateFileUrl(editedImagePath));

        Mono<String> maskUrlMono = minioService.saveFileToMinIO(maskImagePath, maskImage)
                .then(minioService.generateFileUrl(maskImagePath));

        return Mono.zip(referenceUrlMono, editableUrlMono, maskUrlMono, editedUrlMono)
                .flatMap(urls -> {
                    String referenceImageUrl = urls.getT1();
                    String editableImageUrl = urls.getT2();
                    String maskImageUrl = urls.getT3();
                    String editedImageUrl = urls.getT4();

                    // 3. Send both URLs to RabbitMQ
                    return rabbitMQService.sendToQueue(referenceImageUrl, editableImageUrl, maskImageUrl, sessionId)
                            .flatMap(sent -> {
                                if (sent) {
                                    log.info("Successfully sent URLs to RabbitMQ for sessionId={}", sessionId);
                                    // 4. Save initial document in ES (adjust method signature if you need to store both URLs)
                                    return elasticsearchService
                                            .saveInitialDocument(username, sessionId, editableImageName,
                                                    editableImagePath, referenceImageName, referenceImagePath,
                                                    editedImageName, editedImagePath, maskImageName, maskImagePath)
                                            .thenReturn(ResponseEntity.ok(GenericResponseDto.success(sessionId)));
                                } else {
                                    log.error("Failed to send URLs to RabbitMQ for sessionId={}", sessionId);
                                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                                            .body(GenericResponseDto.<String>failure(
                                                    ResultEnum.GENERAL_EXCEPTION,
                                                    "Failed to queue file URLs.")));
                                }
                            });
                })
                .onErrorResume(error -> {
                    log.error("Error in file upload flow for sessionId={}", sessionId, error);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(GenericResponseDto.failure(
                                    ResultEnum.GENERAL_EXCEPTION,
                                    "An error occurred during file processing.")));
                });
    }

    @RabbitListener(queues = "${rabbit.queue.result}")
    public void consumeMessage(String message) {

        log.info("a new message has received: {}", message);
        GeneralMessageDto request;
        try {
            request = objectMapper.readValue(message, GeneralMessageDto.class);
        } catch (Exception e) {
            log.error("read value of received message: {} from rabbitmq failed!", message);
            throw new RuntimeException(e); // TODO: 4/8/25 proper exception
        }
        switch (request.getDataType()) {
            case PROGRESS -> {
                FromAIProgressDto fromAIProgressDto = objectMapper.convertValue(request.getData(), FromAIProgressDto.class);
                receiveProgress(fromAIProgressDto);
            }
            case RESULT -> {
                FromAIResultDto fromAIResultDto = objectMapper.convertValue(request.getData(), FromAIResultDto.class);
                receiveResult(fromAIResultDto);
            }
            default -> log.error("Invalid data type: {} in message: {}", request.getDataType(), message);
        }
    }

    public void receiveProgress(FromAIProgressDto request) {
        log.info("Received result segment for sessionId: {}, chunk_number: {}, total_chunks: {}", request.getSessionId(),
                request.getChunkNumber(), request.getTotalChunks());

        Sinks.Many<String> sink = sinkMap.computeIfAbsent(request.getSessionId(),
                id -> Sinks.many().multicast().onBackpressureBuffer());

        String progressed = String.format("%.2f",
                ((float) request.getChunkNumber() / request.getTotalChunks()) * 100);

        Sinks.EmitResult result = sink.tryEmitNext(progressed);

        if (result.isFailure()) {
            log.warn("Failed to emit result segment for sessionId: {}", request.getSessionId());
        }

        if (request.getChunkNumber() == request.getTotalChunks()) {
            sink.tryEmitNext("COMPLETION_SIGNAL");
        }
    }

    public void receiveResult(FromAIResultDto request) {
        log.info("Received result segment for sessionId: {}", request.getSessionId());

        elasticsearchService.updateCompletedDocument(request.getSessionId(), request.getResultFilePath()).subscribe();
    }

    @GetMapping(value = "/listen/{sessionId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamProgress(@PathVariable String sessionId) {
        log.info("Client requested transcription stream for sessionId: {}", sessionId);

        Sinks.Many<String> sink = sinkMap.get(sessionId);

        if (sink == null) {
            log.error("No transcription segments found for sessionId: {}", sessionId);
            return Flux.error(new FileNotFoundException("No transcription segments found for sessionId: "
                    + sessionId));
        }

        return sink.asFlux()
                .doOnSubscribe(subscription -> log.info("SSE subscription started for sessionId: {}", sessionId))
                .doOnCancel(() -> log.info("SSE subscription canceled for sessionId: {}", sessionId))
                .takeUntil("COMPLETION_SIGNAL"::equals)
                .doOnTerminate(() -> log.info("SSE subscription completed for sessionId: {}", sessionId));
    }
}
