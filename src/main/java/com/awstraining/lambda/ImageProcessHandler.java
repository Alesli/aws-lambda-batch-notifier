package com.awstraining.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.awstraining.configuration.SNSProperties;
import com.awstraining.configuration.SQSProperties;
import com.awstraining.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class ImageProcessHandler implements RequestHandler<Map<String, Object>, APIGatewayProxyResponseEvent> {

    private static final String DETAIL_TYPE_PARAM = "detail-type";
    private static final String API = "API";

    private final NotificationService notificationService;
    private final SNSProperties snsProperties;
    private LambdaLogger log;

    @Override
    public APIGatewayProxyResponseEvent handleRequest(Map<String, Object> stringObjectMap, Context context) {
        Object detail = stringObjectMap.get(DETAIL_TYPE_PARAM);
        String detailType = detail == null ? API : String.valueOf(detail);
        log = context.getLogger();

        int processedMessages = processQueueMessages();

        log.log("Handled Request for ARN = " + snsProperties.getTopicArn()
                + "; Request Source = " + detailType
                + "; Function Name = " + context.getFunctionName()
                + "; Processed Messages count = " + processedMessages
                + "; Remaining Time in millis = " + context.getRemainingTimeInMillis());

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(200)
                .withBody("")
                .withIsBase64Encoded(false);
    }

    private int processQueueMessages() {
        var messages = notificationService.readMessages();

        if (messages.isEmpty()) {
            log.log(" Messages not found. End processing");
            return 0;
        }
        messages.stream()
                .map(Message::getBody)
                .collect(Collectors.joining("\n=========================\n"));
        log.log(" Result message = \n" + messages);

        messages.forEach(message -> {
                    notificationService.sendMessageToTopic(message.getBody());
                    log.log(" Deleting message id = " + message.getMessageId());
                    notificationService.deleteMessage(message.getReceiptHandle());
                }
        );
        return messages.size();
    }
}
