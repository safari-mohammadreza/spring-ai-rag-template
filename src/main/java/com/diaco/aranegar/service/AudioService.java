package com.diaco.aranegar.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.*;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class AudioService {

    public ByteArrayOutputStream trimAudioFile(File input, double start, double end) throws Exception {
        List<String> command = List.of(
                "ffmpeg",
                "-i", input.getAbsolutePath(),
                "-ss", String.valueOf(start),
                "-to", String.valueOf(end),
                "-f", "mp3",
                "-"
        );

        log.info("Executing command: {}", String.join(" ", command));

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);

        Process process = processBuilder.start();

        // Capture the FFmpeg output in a ByteArrayOutputStream
        try (InputStream inputStream = process.getInputStream();
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    log.info("FFmpeg: {}", line);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("Error trimming audio file, exit code: " + exitCode);
            }

            log.info("FFmpeg command executed successfully.");
            return outputStream;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error during audio trimming: " + e.getMessage(), e);
        }
    }

    public Mono<Void> saveFile(FilePart file, String localUploadDir) {
        return Mono.fromRunnable(() -> {
                    File localDir = new File(localUploadDir);
                    if (!localDir.exists() && !localDir.mkdirs()) {
                        throw new RuntimeException("Failed to create directory: " + localUploadDir);
                    }
                })
                .then(
                        file.transferTo(new File(localUploadDir))
                )
                .doOnSuccess(unused ->
                        log.info("File successfully uploaded to local path: {}/{}", localUploadDir, file.filename())
                )
                .doOnError(e ->
                        log.error("Error uploading file to local path: {}", e.getMessage(), e)
                );
    }
}
