package com.awstraining.listener;


import com.awstraining.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

//@Component
//@RequiredArgsConstructor
public class NotificationQueueListener {

//    private final NotificationService notificationService;

//    @Scheduled(fixedRate = 3000)
//    public void readBatchFromQueueAndPushToTopic() {
//        var messages = notificationService.readMessages();
//        messages.forEach(message -> {
//                    notificationService.sendMessageToTopic(message.getBody());
//                    notificationService.deleteMessage(message.getReceiptHandle());
//                }
//        );
//    }
}
