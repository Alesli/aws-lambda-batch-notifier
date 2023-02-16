package com.awstraining.configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class AwsConfig {

    AWSCredentials credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials();

    @Bean
    public AmazonS3 amazonS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    @Bean
    public TransferManager transferManager(AmazonS3 s3Client) {
        return TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();
    }

    @Bean
    public AmazonSNS snsClient(){
        return AmazonSNSClient
                .builder()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    @Bean
    public AmazonSQS sqsClient(){
        return AmazonSQSClient
                .builder()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }
}
