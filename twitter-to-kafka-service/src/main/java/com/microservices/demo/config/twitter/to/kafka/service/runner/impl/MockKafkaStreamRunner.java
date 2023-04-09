package com.microservices.demo.config.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.config.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.demo.config.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.config.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-mock-tweets} " +
        "&& not ${twitter-to-kafka-service.enable-v1-tweets} " +
        "&& not ${twitter-to-kafka-service.enable-v2-tweets}")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData configData;
    private final TwitterKafkaStatusListener statusListener;
    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[] {
            "Lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "Integer", "nec", "odio",
            "Praesent", "libero", "Sed", "cursus", "ante", "dapibus", "diam", "Sed", "nisi", "Nulla", "quis", "sem",
            "at", "nibh", "elementum", "imperdiet"
    };
    private static final String tweetAsRawJson = """
            {
                "created_at" :  "{0}",
                "id"         :  "{1}",
                "text"       :  "{2}",
                "user"       :  {"id" : "{3}"}
            }
            """;
    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    private static final String SPACE = " ";
    private static final String TWEET_STATUS_CREATE_ERROR = "Error occurred while creating twitter status!";;

    public MockKafkaStreamRunner(final TwitterToKafkaServiceConfigData configData,
                                 final TwitterKafkaStatusListener statusListener) {
        this.configData = configData;
        this.statusListener = statusListener;
    }

    @Override
    public void start() {
        final String[] keywords = configData.getTwitterKeywords().toArray(new String[]{});
        final int minTweetLength = configData.getMockMinTweetLength();
        final int maxTweetLength = configData.getMockMaxTweetLength();
        final long sleepTimeMs = configData.getMockSleepMs();
        LOGGER.info("Starting mock filtering twitter streams for keywords: {} with min tweet size {} and " +
                        "max tweet size {} and delay {}", keywords, minTweetLength, maxTweetLength, sleepTimeMs);

        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(final String[] keywords,
                                       final int minTweetLength,
                                       final int maxTweetLength,
                                       final long sleepTimeMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                try {
                    final String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    final Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    statusListener.onStatus(status);
                    sleep(sleepTimeMs);
                } catch (final TwitterException e) {
                    LOGGER.error(TWEET_STATUS_CREATE_ERROR, e);
                }
            }
        });
    }

    private void sleep(final long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping while waiting for a new status!!.", e);
        }
    }

    private String getFormattedTweet(final String[] keywords, final int minTweetLength, final int maxTweetLength) {
        final String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(final String[] params) {
        String tweet = tweetAsRawJson;
        for(int i = 0; i< params.length; ++i) {
            tweet = tweet.replace("{"+i+"}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(final String[] keywords, final int minTweetLength, final int maxTweetLength) {
        final StringBuilder tweet = new StringBuilder();
        final int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private String constructRandomTweet(final String[] keywords, final StringBuilder tweet, final int tweetLength) {
        for (int i = 0; i < tweetLength; ++i) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(SPACE);
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(SPACE);
            }
        }
        return tweet.toString().trim();
    }
}
