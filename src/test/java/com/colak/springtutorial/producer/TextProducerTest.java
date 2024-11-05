package com.colak.springtutorial.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@EmbeddedKafka(partitions = 1,
        topics = {TextProducer.TOPIC},
        brokerProperties = {
                "transaction.state.log.replication.factor=1", // Set replication factor to 1 for transaction logs
                "log.retention.hours=1", // Optional: Adjust log retention for test purposes
                "offsets.topic.replication.factor=1" // Set replication factor for the offsets topic
        }
)
@Slf4j
class TextProducerTest {

    @Autowired
    private TextProducer producer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;  // Embedded Kafka broker

    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        consumer.subscribe(Collections.singleton(TextProducer.TOPIC));
    }

    @Test
    void testSuccessfulMessageSend() {
        List<String> expectedMessages = List.of("message1", "message2");
        producer.sendMessages(expectedMessages);

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1));

        // Verify the number of records consumed
        assertEquals(2, consumerRecords.count(), "Should consume two messages");

        // Verify the consumed records
        Iterable<ConsumerRecord<String, String>> iterable = consumerRecords.records(TextProducer.TOPIC);
        List<String> actualMessages = StreamSupport.stream(iterable.spliterator(), false)
                .map(ConsumerRecord::value)
                .toList();

        assertEquals(expectedMessages, actualMessages, "The consumed messages should match the sent messages");
    }

    @Test
    void testTransactionRollbackOnError() {
        // Use assertThrows to check for the expected exception
        List<String> messageList = List.of("message1", "error", "message2");
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> producer.sendMessagesWithTransactionTemplate(messageList));

        // Verify that the exception message matches what you expect
        assertEquals("Simulated failure", exception.getMessage());

        // Ensure no messages were received because the transaction rolled back
        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1));
        assertTrue(consumerRecords.isEmpty());
    }

    @Test
    void testSuccessfulMessageSendWithTransactionTemplate() {
        List<String> expectedMessages = List.of("message1", "message2");
        producer.sendMessagesWithTransactionTemplate(expectedMessages);

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1));

        // Verify the number of records consumed
        assertEquals(2, consumerRecords.count(), "Should consume two messages");

        // Verify the consumed records
        Iterable<ConsumerRecord<String, String>> iterable = consumerRecords.records(TextProducer.TOPIC);
        List<String> actualMessages = StreamSupport.stream(iterable.spliterator(), false)
                .map(ConsumerRecord::value)
                .toList();

        assertEquals(expectedMessages, actualMessages, "The consumed messages should match the sent messages");
    }

    @Test
    void testTransactionRollbackOnErrorWithTransactionTemplate() {
        // Use assertThrows to check for the expected exception
        List<String> messageList = List.of("message1", "error", "message2");
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> producer.sendMessagesWithTransactionTemplate(messageList));

        // Verify that the exception message matches what you expect
        assertEquals("Simulated failure", exception.getMessage());

        // Ensure no messages were received because the transaction rolled back
        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1));
        assertTrue(consumerRecords.isEmpty());
    }

}