package com.awstraining.controller;

import com.awstraining.dto.ImageModelDto;
import com.awstraining.service.ImageService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/images/")
@RequiredArgsConstructor
public class ImageController {

    @Autowired
    private final ImageService imageService;

    @GetMapping("list-metadata")
    public List<ImageModelDto> getAllMetadata() {
        return imageService.findAllMetadata();
    }

    @GetMapping(value = "download/{name}", produces = MediaType.IMAGE_JPEG_VALUE)
    public ResponseEntity<ByteArrayResource> download(@PathVariable String name) {
        var data=  imageService.download(name);
        return ResponseEntity.ok()
                .contentLength(data.length)
                .header("Content-type", "application/octet-stream")
                .header("Content-disposition", "attachment; fileName=\"" + name + "\"")
                .body(new ByteArrayResource(data));
    }

    @PostMapping("upload")
    public void upload(@RequestParam("file")  MultipartFile multipartFile) {
        imageService.upload(multipartFile);
    }

    @DeleteMapping("delete/{name}")
    public void delete(@PathVariable String name) {
        imageService.deleteByName(name);
    }

    @GetMapping("get-random")
    public ImageModelDto getRandom() {
        return imageService.getOneRandomMetadata();
    }
}
