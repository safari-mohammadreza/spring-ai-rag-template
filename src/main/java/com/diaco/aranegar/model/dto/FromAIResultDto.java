package com.diaco.aranegar.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FromAIResultDto {

    @JsonProperty("session_id")
    private String sessionId;
    @JsonProperty("result_file_path")
    private String resultFilePath;
}
