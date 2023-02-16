package com.awstraining.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class ImageModelDto {

    private String name;
    private long size;
    private String fileExtension;
    private LocalDateTime lastUpdateDate;
    private byte[] bitmap;
}
