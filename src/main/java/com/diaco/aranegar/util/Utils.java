package com.diaco.aranegar.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.tika.Tika;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;

@Component
@Slf4j
public class Utils {

    public String getFileExtension(String fileName) {
        int lastIndex = fileName.lastIndexOf('.');
        return (lastIndex == -1) ? "" : fileName.substring(lastIndex + 1).toLowerCase();
    }

    public boolean isValidFileFormat(String extension) {
        return Set.of("png", "jpg", "jpeg").contains(extension);
    }

    public boolean isValidMimeType(String mimeType) {
        return Set.of("image/jpeg", "image/png").contains(mimeType);
    }

    public Mono<String> detectMimeType(FilePart filePart) {
        Tika tika = new Tika();

        return filePart.content()
                .next()
                .map(DataBuffer::asByteBuffer)
                .map(ByteBuffer::array)
                .flatMap(bytes -> Mono.fromCallable(() -> tika.detect(new ByteArrayInputStream(bytes)))
                        .subscribeOn(Schedulers.boundedElastic()));
    }

    public ZonedDateTime longToZonedDateTime(long longTime) {
        return ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(longTime), ZoneId.of("UTC"));
    }
}
