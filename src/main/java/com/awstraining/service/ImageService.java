package com.awstraining.service;

import com.awstraining.dto.ImageModelDto;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public interface ImageService {

    List<ImageModelDto> findAllMetadata();

    ImageModelDto getOneRandomMetadata();

    byte[] download(String name);

    void upload(MultipartFile file);

    void deleteByName(String name);
}
