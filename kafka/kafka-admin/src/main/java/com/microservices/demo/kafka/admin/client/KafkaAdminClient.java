package com.microservices.demo.kafka.admin.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaAdminClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;
    private static final String TOPIC_READING_ATTEMPTS_EXHAUSTED =
            "Reached max number of retires for reading kafka topic(s)!.";

    public KafkaAdminClient(final KafkaConfigData kafkaConfigData,
                            final RetryConfigData retryConfigData,
                            final AdminClient adminClient,
                            final RetryTemplate retryTemplate,
                            final WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (final Throwable e) {
            throw new KafkaClientException("Reached max number of retries for creating kafka topic(s)!.", e);
        }
        checkTopicsCreated();
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        final Integer maxRetry = retryConfigData.getMaxAttempts();
        final Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while (getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatusCode getSchemaRegistryStatus() {
        try {
            return webClient.get()
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (final Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    //Wait until topics created or max retry attempts reached.
    //Custom retry logic cause the createTopics is an asynchronous operation.
    public void checkTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        final Integer maxRetry = retryConfigData.getMaxAttempts();
        final Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for (final String topic : kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topics, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    private static boolean isTopicCreated(final Collection<TopicListing> topics, final String topic) {
        if (Objects.isNull(topics)) {
            return false;
        } else {
            return topics.stream().anyMatch(topicListing -> topicListing.name().equals(topic));
        }
    }

    private static void checkMaxRetry(final int retry, final Integer maxRetry) {
        if (retry > maxRetry) {
            throw new KafkaClientException(TOPIC_READING_ATTEMPTS_EXHAUSTED);
        }
    }

    private static void sleep(final Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (final InterruptedException e) {
            throw new KafkaClientException("Error while sleeping while waiting for new topics to be created!.");
        }
    }

    private CreateTopicsResult doCreateTopics(final RetryContext retryContext) {
        final List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOGGER.info("Creating {} topic(s), attempt {}.", topicNames.size(), retryContext.getRetryCount());

        final List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
                topic, kafkaConfigData.getNumberOfPartitions(), kafkaConfigData.getReplicationFactor())).toList();
        return adminClient.createTopics(kafkaTopics);
    }

    private Collection<TopicListing> getTopics() {
        final Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (final Throwable e) {
            throw new KafkaClientException(TOPIC_READING_ATTEMPTS_EXHAUSTED, e);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(final RetryContext retryContext)
            throws ExecutionException, InterruptedException {
        LOGGER.info("Reading kafka topic {}, attempt {}", kafkaConfigData.getTopicNamesToCreate().toArray(),
                retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if (topics != null) {
            topics.forEach(topic -> {
                LOGGER.debug("Topic with name {}.", topic.name());
            });
        }
        return topics;
    }
}
