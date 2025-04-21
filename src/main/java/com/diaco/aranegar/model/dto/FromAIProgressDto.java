package com.diaco.aranegar.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FromAIProgressDto {

    @JsonProperty("session_id")
    private String sessionId;
    @JsonProperty("total_chunks")
    private int totalChunks;
    @JsonProperty("chunk_number")
    private int chunkNumber;
}
