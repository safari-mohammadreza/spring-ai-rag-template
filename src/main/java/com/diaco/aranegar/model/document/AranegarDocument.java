package com.diaco.aranegar.model.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(indexName = "aranegar")
public class AranegarDocument {

    @Id
    private String sessionId;
    private String username;
    private String title;
    private String referenceImageName;
    private String editableImageName;
    private String resultImageName;
    private String referenceImagePath;
    private String editableImagePath;
    private String resultImagePath;
    private Boolean isCompleted;
    private Long createTime;
    private Long updateTime;
}
