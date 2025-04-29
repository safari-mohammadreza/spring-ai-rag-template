package com.diaco.aranegar.model.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@Builder
public class AranegarDto {

    private String id;
    private String username;
    private String referenceImageName;
    private String editedImageName;
    private String resultImageName;
    private String referenceImageUrl;
    private String editedImageUrl;
    private String resultImageUrl;
    @JsonFormat(timezone = "UTC", pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private ZonedDateTime createTime;
    private Boolean isCompleted;
}
