package com.colak.springtutorial.producer;


import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
public class TextProducer {

    // Constants for topic configuration
    public static final String TOPIC = "TEXT-DATA";
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public void configureTopic(KafkaAdmin kafkaAdmin) {
        NewTopic newTopic = TopicBuilder
                .name(TOPIC)
                .partitions(1)
                .replicas(1) // Use 1 for local testing
                .build();

        kafkaAdmin.createOrModifyTopics(newTopic);
    }


    // To enable transactions in the spring boot producer microservice, we add Transactional annotation on top of the method.
    // By default, spring rollback a transaction if any runtime exception occurs, no rollback for checked exceptions,
    // you can specify this by mentioning which exception to rollback.
    @Transactional("kafkaTransactionManager")
    public void sendMessages(List<String> messages) {
        for (String message : messages) {
            kafkaTemplate.send(TOPIC, message);
            // Simulate an error in the middle of sending messages
            if ("error".equals(message)) {
                throw new RuntimeException("Simulated failure");
            }
        }
    }

    public void sendMessageWithTransaction2(String message) {
        kafkaTemplate.executeInTransaction(kafkaOperations -> {
            kafkaOperations.send(TOPIC, message);

            // Return any value to indicate transaction success
            return true;
        });
    }

}