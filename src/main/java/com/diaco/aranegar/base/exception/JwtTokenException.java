package com.diaco.aranegar.base.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class JwtTokenException extends RuntimeException {
    private final HttpStatus httpStatus;

    public JwtTokenException(String message, HttpStatus httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }
}