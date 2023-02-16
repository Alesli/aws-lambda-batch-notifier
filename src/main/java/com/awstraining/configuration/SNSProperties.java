package com.awstraining.configuration;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


@ConfigurationProperties(prefix = "sns")
@Component
@Setter
@Getter
@ToString
@EqualsAndHashCode
public class SNSProperties {

  private String topicArn;
}
