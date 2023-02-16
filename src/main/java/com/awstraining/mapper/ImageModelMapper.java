package com.awstraining.mapper;

import com.awstraining.dto.ImageModelDto;
import com.awstraining.entity.ImageModel;
import com.awstraining.service.S3Utils;
import org.mapstruct.*;

import java.util.List;

@Mapper(componentModel = "spring")
public interface ImageModelMapper {

    @Named("mapToImageDto")
    ImageModelDto convertEntityToDto(ImageModel imageModel);

    @IterableMapping(qualifiedByName = "mapToImageDto")
    List<ImageModelDto> convertEntityToDto(List<ImageModel> imageModelList);

    @Mapping(target = "bitmap", ignore = true)
    ImageModelDto downloadModel(ImageModel entityModel, @Context S3Utils s3Utils);

    @AfterMapping
    default void setBitmapToDto(@MappingTarget ImageModelDto target, ImageModel source, @Context S3Utils s3Utils) {
        target.setBitmap(s3Utils.downloadImage(source.getName()));
    }
}
