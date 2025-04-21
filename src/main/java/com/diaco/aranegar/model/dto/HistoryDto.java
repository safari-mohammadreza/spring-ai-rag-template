package com.diaco.aranegar.model.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@Builder
public class HistoryDto {

    private String id;
    private String title;
    @JsonFormat(timezone = "UTC", pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    @JsonProperty("create_time")
    private ZonedDateTime createTime;
}
