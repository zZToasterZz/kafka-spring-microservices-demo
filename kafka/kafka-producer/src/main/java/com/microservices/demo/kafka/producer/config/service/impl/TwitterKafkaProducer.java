package com.microservices.demo.kafka.producer.config.service.impl;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaProducer.class);
    private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(final String topicName, final Long key, final TwitterAvroModel message) {
        LOGGER.info("Sending message: {}, to topic: {}", message, topicName);
        final CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture =
                kafkaTemplate.send(topicName, key, message);

        addCallback(topicName, message, kafkaResultFuture);
    }

    private static void addCallback(final String topicName,
                                    final TwitterAvroModel message,
                                    final CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
        kafkaResultFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error while sending message {} to topic {}", message, topicName, throwable);
            } else {
                final RecordMetadata recordMetadata = result.getRecordMetadata();
                LOGGER.debug("Received new Metadata. Topic: {}; Partition: {}; Offset: {}; Timestamp: {}, at Time {}.",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        recordMetadata.timestamp(),
                        System.nanoTime());
            }
        });
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOGGER.info("Closing kafka producer!.");
            kafkaTemplate.destroy();
        }
    }
}
