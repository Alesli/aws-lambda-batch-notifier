package com.awstraining.service;

import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.Message;

import java.util.List;

public interface NotificationService {

  void subscribeEmail(String email);

  void unsubscribeEmail(String email);

  void sendMessageToQueue(String message);

  void sendMessageToTopic(String message);
  DeleteMessageResult deleteMessage(String message);

  List<Message> readMessages();
}
