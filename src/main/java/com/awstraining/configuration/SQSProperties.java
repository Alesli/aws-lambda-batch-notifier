package com.awstraining.configuration;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "sqs")
@Component
@Setter
@Getter
@ToString
@EqualsAndHashCode
public class SQSProperties {

  private String queueUrl;
}
