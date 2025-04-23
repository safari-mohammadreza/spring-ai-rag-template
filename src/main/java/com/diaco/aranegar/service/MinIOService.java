package com.diaco.aranegar.service;

import io.minio.*;
import io.minio.errors.MinioException;
import io.minio.http.Method;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.Tika;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class MinIOService {

    private final MinioClient minioClient;

    @Value("${minio.bucket.name}")
    private String bucketName;

    /**
     * Ensures the MinIO bucket exists, creates it if not.
     */
    @PostConstruct
    public void ensureBucketExists() {
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                log.info("Created MinIO bucket: {}", bucketName);
            } else {
                log.info("MinIO bucket already exists: {}", bucketName);
            }
        } catch (Exception e) {
            log.error("Error checking/creating MinIO bucket: {}", bucketName, e);
        }
    }

    public Mono<Void> saveFileToMinIO(String fileName, FilePart file) {
        return DataBufferUtils.join(file.content())
                .flatMap(dataBuffer -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);
                    String contentType = new Tika().detect(bytes);
                    log.info("request to uploading file with content-type: {}", contentType);

                    return uploadFile(fileName, bytes, contentType);
                })
                .doOnSuccess(v -> log.info("Audio file uploaded to MinIO: {}", fileName))
                .doOnError(error -> log.error("Failed to upload file to MinIO: {}", fileName, error));
    }

    /**
     * Save audio file to MinIO.
     */
    public Mono<Void> saveFileToMinIO(String filePath, byte[] audioBytes) {

        ByteArrayInputStream inputStream = new ByteArrayInputStream(audioBytes);

        try {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(filePath)
                            .stream(inputStream, audioBytes.length, -1)
                            .contentType("audio/wav")
                            .build()
            );
            log.info("Audio file {} saved to MinIO.", filePath);
        } catch (Exception e) {
            log.error("Error saving audio file to MinIO", e);
            return Mono.error(e);
        }
        return Mono.empty();
    }

    public Mono<Void> uploadFile(String fileName, byte[] fileData, String contentType) {
        return Mono.fromRunnable(() -> {
            try (InputStream inputStream = new ByteArrayInputStream(fileData)) {
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(fileName)
                                .stream(inputStream, fileData.length, -1)
                                .contentType(contentType)
                                .build()
                );
                log.info("File uploaded successfully to MinIO: {}/{}", bucketName, fileName);
            } catch (MinioException | IllegalArgumentException | IllegalStateException e) {
                log.error("Error uploading file to MinIO: {}", fileName, e);
                throw new RuntimeException("Failed to upload file to MinIO", e);
            } catch (IOException | InvalidKeyException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Generate a pre-signed URL for accessing the file from MinIO.
     */
    public Mono<String> generateFileUrl(String filePath) {
        try {
            String presignedUrl = minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucketName)
                            .object(filePath)
                            .expiry(7, TimeUnit.HOURS)
                            .build()
            );
            return Mono.just(presignedUrl);
        } catch (Exception e) {
            log.error("Error generating pre-signed URL for file: {}", filePath, e);
            return Mono.error(e);
        }
    }

    public String generateMinIoFilePath(String username, String sessionId, String fileName) {
        return String.format("%s/%s/%s", username, sessionId, fileName.replaceAll("\\s", ""));
    }

    public Mono<Boolean> deleteFile(String filePath) {
        return Mono.fromCallable(() -> {
                    try {
                        log.info("Attempting to delete file from MinIO: {}", filePath);

                        minioClient.removeObject(
                                RemoveObjectArgs.builder()
                                        .bucket(bucketName)
                                        .object(filePath)
                                        .build()
                        );

                        log.info("Successfully deleted file from MinIO: {}", filePath);
                        return true;
                    } catch (Exception e) {
                        log.error("Error deleting file from MinIO: {}", filePath, e);
                        throw new RuntimeException("Failed to delete file from MinIO", e);
                    }
                })
                .onErrorResume(error -> {
                    log.error("MinIO file deletion failed: {}", error.getMessage());
                    return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error deleting file from MinIO"));
                });
    }
}
