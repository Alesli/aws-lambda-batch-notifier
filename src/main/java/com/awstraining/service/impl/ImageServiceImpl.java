package com.awstraining.service.impl;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.awstraining.controller.ImageController;
import com.awstraining.dao.ImageRepository;
import com.awstraining.dto.ImageModelDto;
import com.awstraining.entity.ImageModel;
import com.awstraining.mapper.ImageModelMapper;
import com.awstraining.service.ImageService;
import com.awstraining.service.NotificationService;
import com.awstraining.service.S3Utils;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import javax.transaction.Transactional;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@Service
@RequiredArgsConstructor
public class ImageServiceImpl implements ImageService {
    private static final String IMAGE_UPLOAD_ACTION = "Image has been uploaded successful";

    @Value("${s3.bucket.name}")
    private String s3BucketName;

    private final ImageRepository imageRepository;
    private final ImageModelMapper imageModelMapper;
    private final S3Utils s3Utils;
    private final TransferManager transferManager;
    private final NotificationService notificationService;

    @Value("${server.port}")
    private String serverPort;

    @Transactional
    public List<ImageModelDto> findAllMetadata() {
        return imageModelMapper.convertEntityToDto(imageRepository.findAll());
    }

    @Transactional
    public ImageModelDto getOneRandomMetadata() {
        return imageModelMapper.convertEntityToDto(imageRepository.getOneRandom());
    }

    @Transactional
    public byte[] download(String name) {
        Optional<ImageModel> imageModel = imageRepository.getOneByName(name);
        return imageModelMapper.downloadModel(imageModel.get(), s3Utils).getBitmap();
    }

    @Transactional
    public void upload(MultipartFile multipartFile) {
        imageRepository.getOneByName(multipartFile.getOriginalFilename())
                .ifPresent(image -> {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Image is already present");
                });
        try {
        var upload = transferManager.upload(s3BucketName,
                multipartFile.getOriginalFilename(), multipartFile.getInputStream(),
                new ObjectMetadata());
        upload.waitForUploadResult();
        var imageModel = ImageModel.builder()
                .fileExtension(FilenameUtils.getExtension(multipartFile.getOriginalFilename()))
                .name(multipartFile.getOriginalFilename())
                .size(multipartFile.getSize())
                .lastUpdateDate(LocalDateTime.now())
                .build();
            imageRepository.saveAndFlush(imageModel);
            notificationService.sendMessageToQueue(createMessage(imageModel));
        } catch (IOException | InterruptedException e) {
            deleteByName(multipartFile.getOriginalFilename());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }
    private String createMessage(ImageModel imageMetadata) {
        var downloadLink = linkTo(methodOn(ImageController.class).download(imageMetadata.getName()));
        return StringUtils.joinWith(":::", IMAGE_UPLOAD_ACTION, imageMetadata.toString(),
                downloadLink);
    }

    @Transactional
    public void deleteByName(String name) {
        List<ImageModel> imageModelList = imageRepository.findAllByName(name);
        imageModelList.stream()
                .map(ImageModel::getName)
                .forEach(s3Utils::deleteImage);
        imageModelList.forEach(imageRepository::delete);
    }
}
