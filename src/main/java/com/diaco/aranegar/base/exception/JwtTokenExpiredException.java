package com.diaco.aranegar.base.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class JwtTokenExpiredException extends RuntimeException {
    private final HttpStatus httpStatus;

    public JwtTokenExpiredException(String message, HttpStatus httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }
}