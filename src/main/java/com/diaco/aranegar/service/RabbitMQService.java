package com.diaco.aranegar.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class RabbitMQService {

    private final RabbitTemplate rabbitTemplate;

    @Value("${rabbit.queue.request}")
    private String queueName;

    public Mono<Boolean> sendToQueue(String referenceImageUrl, String editableImageUrl, String maskImageUrl,
                                     String sessionId, String resultFilePath) {
        Map<String, String> message = new HashMap<>();
        message.put("reference_file_url", referenceImageUrl);
        message.put("editable_file_url", editableImageUrl);
        message.put("mask_file_url", maskImageUrl);
        message.put("session_id", sessionId);
        message.put("result_file_path", resultFilePath);

        // Wrap the RabbitMQ sending in a Mono so we can stay in the reactive world.
        return Mono.fromCallable(() -> {
                    rabbitTemplate.convertAndSend(queueName, message);
                    log.info("Message: {} sent to queue: {}.", message, queueName);
                    return true;
                })
                // Since RabbitTemplate is blocking, execute on a bounded elastic thread.
                .subscribeOn(Schedulers.boundedElastic());
    }
}

