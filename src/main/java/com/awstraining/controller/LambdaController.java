package com.awstraining.controller;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import lombok.RequiredArgsConstructor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/lambda")
@RequiredArgsConstructor
public class LambdaController {

    private final AWSLambda lambda;

    @Value("${lambda.function.arn}")
    private String functionArn;


    @PutMapping("/action")
    public ResponseEntity<Object> triggerBatchNotifier() {
        try {
            InvokeRequest invokeRequest = new InvokeRequest()
                    .withFunctionName(functionArn)
                    .withPayload("{\"detail-type\": \"Application\"}");
//                    .withFunctionName("task9-uploads-batch-notifier");
            InvokeResult result = lambda.invoke(invokeRequest);
            System.out.println(result.getLogResult());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
