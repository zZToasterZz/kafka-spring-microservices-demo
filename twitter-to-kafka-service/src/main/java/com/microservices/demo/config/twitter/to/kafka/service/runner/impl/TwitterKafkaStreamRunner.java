package com.microservices.demo.config.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.config.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.config.twitter.to.kafka.service.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v1-tweets} " +
        "&& not ${twitter-to-kafka-service.enable-v2-tweets} " +
        "&& not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData configData;
    private final TwitterKafkaStatusListener statusListener;
    private TwitterStream twitterStream;

    TwitterKafkaStreamRunner(final TwitterToKafkaServiceConfigData configData,
                             final TwitterKafkaStatusListener statusListener) {
        this.configData = configData;
        this.statusListener = statusListener;
    }
    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(statusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            LOGGER.info("Closing twitter stream.");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        final String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
        final FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOGGER.info("Started filtering twitter stream for keywords: {}", Arrays.toString(keywords));
    }
}
