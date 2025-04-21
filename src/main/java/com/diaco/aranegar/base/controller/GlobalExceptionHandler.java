package com.diaco.aranegar.base.controller;

import com.diaco.aranegar.base.exception.FileNotFoundException;
import com.diaco.aranegar.model.dto.GenericResponseDto;
import com.diaco.aranegar.model.enums.ResultEnum;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(FileNotFoundException.class)
    public ResponseEntity<GenericResponseDto<String>> handleFileNotFoundException
            (FileNotFoundException exception) {
        return new ResponseEntity<>(GenericResponseDto.failure(ResultEnum.GENERAL_NOT_FOUND,
                (exception.getMessage() != null ? exception.getMessage() : "Username or password are incorrect!")),
                HttpStatus.NOT_FOUND);
    }
}
