package com.diaco.aranegar.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UpdateHistoryRequestDto {

    @JsonProperty("new_title")
    private String newTitle;
}
