package com.microservices.demo.config.twitter.to.kafka.service.listener;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.twitter.to.kafka.service.transformer.TwitterStatusToAvroTransformer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);
    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusToAvroTransformer transformer;

    public TwitterKafkaStatusListener(final KafkaConfigData kafkaConfigData,
                                      final KafkaProducer<Long, TwitterAvroModel> kafkaProducer,
                                      final TwitterStatusToAvroTransformer transformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.transformer = transformer;
    }

    @Override
    public void onStatus(final Status status) {
        LOGGER.info("Received Twitter status: {}; Sending to Kafka Topic: {}", status.getText(),
                kafkaConfigData.getTopicName());
        final TwitterAvroModel model = transformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), model.getUserId(), model);
    }
}