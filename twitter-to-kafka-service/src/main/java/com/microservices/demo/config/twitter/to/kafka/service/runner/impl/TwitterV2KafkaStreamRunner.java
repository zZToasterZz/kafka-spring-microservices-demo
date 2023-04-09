package com.microservices.demo.config.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.config.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} " +
        "&& not ${twitter-to-kafka-service.enable-v1-tweets} " +
        "&& not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterV2KafkaStreamRunner implements StreamRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterV2KafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData configData;
    private final TwitterV2StreamHelper streamHelper;
    private static final String BEARER_TOKEN_NOT_SET = """
                    There was an error trying to get the bearer token. Please make sure to set the 
                    TWITTER_BEARER_TOKEN environment variable.
                    """;
    private static final String ERROR_WHILE_STREAMING_TWEETS = "There was an error streaming tweets.";

    public TwitterV2KafkaStreamRunner(final TwitterToKafkaServiceConfigData configData,
                                      final TwitterV2StreamHelper streamHelper) {
        this.configData = configData;
        this.streamHelper = streamHelper;
    }

    @Override
    public void start() {

        final String bearerToken = configData.getTwitterV2BearerToken();
        if (null  != bearerToken) {
            try {
                streamHelper.setupRules(bearerToken, getRules());
                streamHelper.connectStream(bearerToken);
            } catch (URISyntaxException | IOException e) {
                LOGGER.error(ERROR_WHILE_STREAMING_TWEETS, e);
                throw new RuntimeException(ERROR_WHILE_STREAMING_TWEETS, e);
            }
        } else {
            LOGGER.error(BEARER_TOKEN_NOT_SET);
            throw new RuntimeException(BEARER_TOKEN_NOT_SET);
        }
    }

    private Map<String, String> getRules() {
        final List<String> keywords = configData.getTwitterKeywords();
        final Map<String, String> rules = new HashMap<>();
        for (final String keyword : keywords) {
            rules.put(keyword, "Keyword: " + keyword);
        }
        LOGGER.info("Created filter for twitter stream for keywords: {}", keywords);
        return rules;
    }
}
