package com.diaco.aranegar.base.exception;

import lombok.Getter;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;

@Getter
public class ErrorMessageException extends AmqpRejectAndDontRequeueException {

    private final String errorMessage;

    public ErrorMessageException(String errorMessage) {
        super(errorMessage);
        this.errorMessage = errorMessage;
    }

    @Override
    public String getMessage() {
        return super.getMessage();
    }
}