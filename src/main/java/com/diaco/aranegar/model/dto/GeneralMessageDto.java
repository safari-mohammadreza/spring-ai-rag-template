package com.diaco.aranegar.model.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonDeserialize
public class GeneralMessageDto<T> {
    private DataType dataType;
    private T data;
}