package com.diaco.aranegar.controller;

import com.diaco.aranegar.base.exception.FileNotFoundException;
import com.diaco.aranegar.model.dto.FromAIProgressDto;
import com.diaco.aranegar.model.dto.FromAIResultDto;
import com.diaco.aranegar.model.dto.GeneralMessageDto;
import com.diaco.aranegar.model.dto.GenericResponseDto;
import com.diaco.aranegar.model.enums.ResultEnum;
import com.diaco.aranegar.service.AudioService;
import com.diaco.aranegar.service.ElasticsearchService;
import com.diaco.aranegar.service.MinIOService;
import com.diaco.aranegar.service.RabbitMQService;
import com.diaco.aranegar.util.JwtUtils;
import com.diaco.aranegar.util.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.*;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
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
    private final AudioService audioService;
    private final Utils fileUtils;

    private final Map<String, Sinks.Many<String>> sinkMap = new HashMap<>();
    private final int MAX_FILE_SIZE = 100 * 1024; //100KB


    @PostMapping("/process")
    public Mono<ResponseEntity<GenericResponseDto<String>>> processFile(
            @RequestPart("referenceImage") Mono<FilePart> referenceImageMono,
            @RequestPart("editableImage") Mono<FilePart> editableImageMono,
            @RequestHeader(name = "Authorization") String token) {

        String username = jwtUtils.getUserNameFromJwtToken(token);
        String sessionId = username.concat(UUID.randomUUID().toString());

        log.info("Received request to upload audio. sessionId: {}, username: {}", sessionId, username);

        sinkMap.computeIfAbsent(sessionId,
                id -> Sinks.many().multicast().onBackpressureBuffer());

        return Mono.zip(referenceImageMono, editableImageMono)
                .flatMap(tuple -> {
                    FilePart referenceImagePart = tuple.getT1();
                    FilePart editableImagePart = tuple.getT2();

                    // 4a. Validate reference extension
                    String faceExt = fileUtils.getFileExtension(referenceImagePart.filename());
                    if (!fileUtils.isValidFileFormat(faceExt)) {
                        log.error("Invalid face extension {} for session {}", faceExt, sessionId);
                        return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                .body(GenericResponseDto.<String>failure(ResultEnum.INVALID_INPUT,
                                        "Invalid face file format.")));
                    }

                    // 4b. Validate reference size then MIME
                    Mono<ResponseEntity<GenericResponseDto<String>>> faceValidation = referenceImagePart.content()
                            .map(DataBuffer::readableByteCount)
                            .reduce(0L, Long::sum)
                            .flatMap(size -> {
                                if (size > MAX_FILE_SIZE) {
                                    log.error("Face image too large ({} bytes) for session {}", size, sessionId);
                                    return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                            .body(GenericResponseDto.<String>failure(ResultEnum.INVALID_INPUT,
                                                    "Face image exceeds size limit.")));
                                }
                                return fileUtils.detectMimeType(referenceImagePart)
                                        .flatMap(mime -> {
                                            if (!mime.startsWith("image/")) {
                                                log.error("Face MIME {} invalid for session {}", mime, sessionId);
                                                return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                                        .body(GenericResponseDto.<String>failure(ResultEnum.INVALID_INPUT,
                                                                "Face must be an image.")));
                                            }
                                            return Mono.empty(); // OK
                                        });
                            });

                    // 5. Validate editable extension, size, and MIME
                    Mono<ResponseEntity<GenericResponseDto<String>>> fileValidation = editableImagePart.content()
                            .map(DataBuffer::readableByteCount)
                            .reduce(0L, Long::sum)
                            .flatMap(size -> {
                                if (size > MAX_FILE_SIZE) {
                                    log.error("File too large ({} bytes) for session {}", size, sessionId);
                                    return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                            .body(GenericResponseDto.<String>failure(ResultEnum.INVALID_INPUT,
                                                    "File exceeds size limit.")));
                                }
                                String fileExt = fileUtils.getFileExtension(editableImagePart.filename());
                                if (!fileUtils.isValidFileFormat(fileExt)) {
                                    log.error("Invalid file extension {} for session {}", fileExt, sessionId);
                                    return Mono.just(ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                            .body(GenericResponseDto.<String>failure(ResultEnum.INVALID_INPUT,
                                                    "Invalid file format.")));
                                }
                                return Mono.empty(); // OK
                            });

                    // 6. Run both validations in sequence, then upload
                    return faceValidation
                            .flatMap(Mono::just)           // if faceValidation emitted a ResponseEntity, short‑circuit
                            .switchIfEmpty(
                                    fileValidation
                                            .flatMap(Mono::just) // if fileValidation emitted a ResponseEntity
                                            .switchIfEmpty(
                                                    // both passed: call your upload
                                                    processFileUpload(referenceImagePart, editableImagePart, sessionId,
                                                            username)
                                            )
                            );
                })
                // 7. Global error handler
                .onErrorResume(error -> {
                    log.error("Unexpected error during upload, session {}: {}", sessionId, error.getMessage(), error);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body(GenericResponseDto.failure(ResultEnum.GENERAL_EXCEPTION,
                                    "Unexpected error occurred.")));
                });
    }

    private Mono<ResponseEntity<GenericResponseDto<String>>> processFileUpload(
            FilePart referenceImage,
            FilePart editableImage,
            String sessionId,
            String username) {

        // 1. Generate MinIO paths
        String referenceImageName = referenceImage.filename();
        String referenceImagePath = minioService.generateMinIoFilePath(username, sessionId, referenceImageName);

        String editableImageName = editableImage.filename();
        String editableImagePath = minioService.generateMinIoFilePath(username, sessionId, editableImageName);

        log.info("Uploading to MinIO: facePath={} filePath={}", referenceImagePath, editableImagePath);

        // 2. Upload both in parallel, then get their URLs
        Mono<String> referenceUrlMono = minioService.saveFileToMinIO(referenceImagePath, referenceImage)
                .then(minioService.generateFileUrl(referenceImagePath));

        Mono<String> editableUrlMono = minioService.saveFileToMinIO(editableImagePath, editableImage)
                .then(minioService.generateFileUrl(editableImagePath));

        return Mono.zip(referenceUrlMono, editableUrlMono)
                .flatMap(urls -> {
                    String referenceImageUrl = urls.getT1();
                    String editableImageUrl = urls.getT2();
                    log.info("Generated URLs: referenceImageUrl={} editableImageUrl={}",
                            referenceImageUrl, editableImageUrl);

                    // 3. Send both URLs to RabbitMQ
                    return rabbitMQService.sendToQueue(referenceImageUrl, editableImageUrl, sessionId)
                            .flatMap(sent -> {
                                if (sent) {
                                    log.info("Successfully sent URLs to RabbitMQ for sessionId={}", sessionId);
                                    // 4. Save initial document in ES (adjust method signature if you need to store both URLs)
                                    return elasticsearchService
                                            .saveInitialDocument(username, sessionId, editableImagePath, editableImageName)
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

    @PostMapping(value = "/trim", consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Resource>> trimAudio(
            @RequestPart("file") FilePart filePart,
            @RequestParam("start") double start,
            @RequestParam("end") double end) {

        // Ensure the temporary directory exists
        String tempDirPath = "/tmp/audio_processing/";
        File tempDir = new File(tempDirPath);
        if (!tempDir.exists()) {
            boolean created = tempDir.mkdirs();
            if (!created) {
                log.error("Failed to create temporary directory: {}", tempDirPath);
                return Mono.error(new RuntimeException("Failed to create temporary directory for audio processing"));
            }
            log.info("Temporary directory created at: {}", tempDirPath);
        }

        // Generate a unique temporary file path
        String tempInputFilePath = tempDirPath + UUID.randomUUID() + "_" + filePart.filename();

        return audioService.saveFile(filePart, tempInputFilePath)
                .doOnSuccess(unused -> log.info("File successfully uploaded to: {}", tempInputFilePath))
                .then(Mono.fromCallable(() -> {
                    try (ByteArrayOutputStream outputStream = audioService.trimAudioFile(
                            new File(tempInputFilePath), start, end)) {
                        log.info("Audio trimming completed successfully.");

                        org.springframework.core.io.Resource resource = new InputStreamResource(
                                new ByteArrayInputStream(outputStream.toByteArray()));
                        HttpHeaders headers = new HttpHeaders();
                        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
                        headers.setContentDisposition(ContentDisposition.builder("attachment")
                                .filename("trimmed_" + filePart.filename())
                                .build());
                        headers.setContentLength(outputStream.size());

                        return ResponseEntity.ok()
                                .headers(headers)
                                .body(resource);
                    } finally {
                        // Cleanup temporary input file
                        new File(tempInputFilePath).delete();
                        log.info("Temporary input file cleaned up.");
                    }
                }))
                .doOnError(e -> log.error("Error during audio processing: {}", e.getMessage(), e));
    }
}
