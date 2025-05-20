package com.diaco.aranegar.base.exception;

import lombok.Getter;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;

@Getter
public class ErrorMessageException extends AmqpRejectAndDontRequeueException {
    private final String sessionId;
    private final String errorMessage;

    public ErrorMessageException(String sessionId, String errorMessage) {
        super("AI error for session " + sessionId + ": " + errorMessage);
        this.sessionId = sessionId;
        this.errorMessage = errorMessage;
    }

    @Override
    public String getMessage() {
        return super.getMessage();
    }
}