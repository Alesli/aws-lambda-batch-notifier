package com.awstraining.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;

@Service
public class S3Utils {

    @Value("${s3.bucket.name}")
    private String s3BucketName;

    @Autowired
    private AmazonS3 amazonS3Client;

    public byte[] downloadImage(String imageName) {
        doesBucketExist();
        S3Object object = amazonS3Client.getObject(s3BucketName, imageName);
        try {
            return object.getObjectContent().readAllBytes();
        } catch (IOException | AmazonClientException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    public void deleteImage(String objectName) {
        doesBucketExist();
        amazonS3Client.deleteObject(s3BucketName, objectName);
    }

    private void doesBucketExist() {
        if (!amazonS3Client.doesBucketExistV2(s3BucketName)) {
            amazonS3Client.createBucket(s3BucketName);
        }
    }
}
