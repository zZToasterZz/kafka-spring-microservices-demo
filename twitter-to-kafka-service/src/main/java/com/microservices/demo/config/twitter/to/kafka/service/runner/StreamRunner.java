package com.microservices.demo.config.twitter.to.kafka.service.runner;

import twitter4j.TwitterException;

import java.io.IOException;
import java.net.URISyntaxException;

public interface StreamRunner {
    void start() throws TwitterException, URISyntaxException, IOException;
}
