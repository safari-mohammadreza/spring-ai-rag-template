package com.diaco.aranegar.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FromAIErrorDto {

    @JsonProperty("session_id")
    private String sessionId;
    @JsonProperty("error_message")
    private String errorMessage;
}
