package com.awstraining.service.impl;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.AmazonSNSException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.awstraining.configuration.SNSProperties;
import com.awstraining.configuration.SQSProperties;
import com.awstraining.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
@RequiredArgsConstructor
public class NotificationServiceImpl implements NotificationService {

    private static final String SNS_PROTOCOL = "email";

    private final SNSProperties snsProperties;
    private final SQSProperties sqsProperties;
    private final AmazonSNS snsClient;
    private final AmazonSQS sqsClient;

    @Override
    public void subscribeEmail(String email) {
        try {
            var subscribeRequest = new SubscribeRequest()
                    .withProtocol(SNS_PROTOCOL)
                    .withEndpoint(email)
                    .withTopicArn(snsProperties.getTopicArn());
            snsClient.subscribe(subscribeRequest);
        } catch (AmazonSNSException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }

    }

    @Override
    public void unsubscribeEmail(String email) {
        try {
            var listResult = snsClient.listSubscriptionsByTopic(snsProperties.getTopicArn());
            var subscriptions = listResult.getSubscriptions();
            subscriptions.stream()
                    .filter(subscription -> email.equals(subscription.getEndpoint()))
                    .findAny()
                    .ifPresent(subscription -> {
                        try {
                            var unsubscribeRequest = new UnsubscribeRequest()
                                    .withSubscriptionArn(subscription.getSubscriptionArn());
                            snsClient.unsubscribe(unsubscribeRequest);
                        } catch (AmazonSNSException e) {
                            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
                        }
                    });
        } catch (AmazonSNSException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @Override
    public void sendMessageToQueue(String message) {
        try {
            var request = new SendMessageRequest()
                    .withQueueUrl(sqsProperties.getQueueUrl())
                    .withMessageBody(message);
            sqsClient.sendMessage(request);
        } catch (AmazonSQSException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @Override
    public void sendMessageToTopic(String message) {
        try {
            var publishRequest = new PublishRequest()
                    .withMessage(message)
                    .withTopicArn(snsProperties.getTopicArn());
            snsClient.publish(publishRequest);
        } catch (AmazonSNSException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @Override
    public DeleteMessageResult deleteMessage(String handle) {
        try {
            var request = new DeleteMessageRequest()
                    .withQueueUrl(sqsProperties.getQueueUrl())
                    .withReceiptHandle(handle);
           return sqsClient.deleteMessage(request);
        } catch (AmazonSNSException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    @Override
    public List<Message> readMessages() {
        try {
            var queueUrl = sqsProperties.getQueueUrl();
            var request = new ReceiveMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withWaitTimeSeconds(10)
                    .withMaxNumberOfMessages(5);
            var messages = sqsClient.receiveMessage(request).getMessages();
            messages.stream()
                    .map(Message::getReceiptHandle)
                    .forEach(receipt -> sqsClient.deleteMessage(queueUrl, receipt));
            return messages;
        } catch (AmazonSQSException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }
}
