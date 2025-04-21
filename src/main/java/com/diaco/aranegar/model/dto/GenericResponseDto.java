package com.diaco.aranegar.model.dto;

import com.diaco.aranegar.model.enums.ResultEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class GenericResponseDto<T> {

    private Integer statusCode;
    private boolean status;
    private T content;
    private String message;
    private int pageNumber;
    private int pageSize;
    private int totalPages;
    private Long count;


    public static <T> GenericResponseDto<T> success(T content) {
        return new GenericResponseDto<>(true, content, null);
    }

    public static <T> GenericResponseDto<T> success(ResultEnum resultEnum) {
        return new GenericResponseDto<>(true, resultEnum, "Done!");
    }

    public static <T> GenericResponseDto<T> failure(ResultEnum resultEnum, String message) {
        return new GenericResponseDto<>(false, resultEnum, message);
    }

    public static <T> GenericResponseDto<T> failure(T content , ResultEnum resultEnum, String message) {
        return new GenericResponseDto<>(false, resultEnum, content, message);
    }

    public static <T> GenericResponseDto<T> success(
            T content,
            int pageNumber,
            int pageSize,
            int totalPages,
            Long count
    ) {
        return new GenericResponseDto<>(
                true, pageNumber, pageSize, totalPages, count, content);
    }

    public GenericResponseDto(boolean status, ResultEnum resultEnum, String message) {
        this.status = status;
        this.statusCode = resultEnum != null ? resultEnum.getCode() : null;
        this.message = message;
    }

    public GenericResponseDto(boolean status, T content, String message) {
        this.status = status;
        this.content = content;
        this.message = message;
    }

    public GenericResponseDto(boolean status, ResultEnum resultEnum, T content, String message) {
        this.status = status;
        this.statusCode = resultEnum != null ? resultEnum.getCode() : null;
        this.content = content;
        this.message = message;
    }

    public GenericResponseDto(boolean status, int pageNumber, int pageSize, int totalPages, Long count, T content) {
        this.status = status;
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.totalPages = totalPages;
        this.count = count;
        this.content = content;
    }

}
