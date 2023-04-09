package com.microservices.demo.config.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.config.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} " +
        "&& not ${twitter-to-kafka-service.enable-v1-tweets} " +
        "&& not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterV2StreamHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterV2StreamHelper.class);
    private final TwitterToKafkaServiceConfigData configData;
    private final TwitterKafkaStatusListener statusListener;
    private static final String tweetAsRawJson = """
            {
                "created_at" :  "{0}",
                "id"         :  "{1}",
                "text"       :  "{2}",
                "user"       :  {"id" : "{3}"}
            }
            """;
    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MM dd HH:mm:ss zzz yyyy";

    TwitterV2StreamHelper(final TwitterToKafkaServiceConfigData configData,
                          final TwitterKafkaStatusListener statusListener) {
        this.configData = configData;
        this.statusListener = statusListener;
    }

    void connectStream(final String bearerToken) throws URISyntaxException, IOException {
        final CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        final URIBuilder uriBuilder = new URIBuilder(configData.getTwitterV2BaseUrl());

        final HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

        final HttpResponse httpResponse = httpClient.execute(httpGet);
        final HttpEntity httpEntity = httpResponse.getEntity();
        if (null != httpEntity) {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(httpEntity.getContent()));
            String line = reader.readLine();
            while (line != null) {
                line = reader.readLine();
                if (line.isEmpty()) {
                    String tweet = getFormattedTweet(line);
                    Status status = null;
                    try {
                        status = TwitterObjectFactory.createStatus(tweet);
                    } catch (TwitterException e) {
                        LOGGER.error("Could not create status for text: {}", tweet, e);
                    }
                    if (status != null) {
                        statusListener.onStatus(status);
                    }
                }
            }
        }
    }

    /**
     * Helper method to setup rules before streaming data
     *
     * @param bearerToken
     * @param rules
     */
    void setupRules(final String bearerToken, final Map<String, String> rules) throws URISyntaxException, IOException {
        final List<String> existingRules = getRules(bearerToken);
        if (existingRules.size() > 0) {
            deleteRules(bearerToken, existingRules);
        }
        createRules(bearerToken, rules);
        LOGGER.info("Created rules for twitter stream: {}", rules.keySet().toArray());
    }

    /**
     * Helper method to create rules for filtering
     *
     * @param bearerToken
     * @param rules
     */
    private void createRules(final String bearerToken, final Map<String, String> rules)
            throws URISyntaxException, IOException {

        final CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        final URIBuilder uriBuilder = new URIBuilder(configData.getTwitterV2RulesBaseUrl());

        final HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        final StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules));
        httpPost.setEntity(body);
        final HttpResponse response = httpClient.execute(httpPost);
        final HttpEntity entity = response.getEntity();
        if (null != entity) {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    /**
     * Helper method to get existing rules.
     *
     * @param bearerToken
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    private List<String> getRules(final String bearerToken) throws URISyntaxException, IOException {
        final List<String> rules = new ArrayList<>();

        final CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        final URIBuilder uriBuilder = new URIBuilder(configData.getTwitterV2RulesBaseUrl());
        final HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpGet.setHeader("content-type", "application/json");

        final HttpResponse httpResponse = httpClient.execute(httpGet);
        final HttpEntity httpEntity = httpResponse.getEntity();
        if (null != httpEntity) {
            JSONObject json = new JSONObject(EntityUtils.toString(httpEntity, "UTF-8"));
            if (json.length() > 1 && json.has("data")) {
                JSONArray jsonArray = json.getJSONArray("data");
                for (int i = 0; i < jsonArray.length(); ++i) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    rules.add(jsonObject.getString("id"));
                }
            }
        }
        return rules;
    }

    /**
     * Helper method to delete rules
     * @param bearerToken
     * @param existingRules
     * @throws URISyntaxException
     * @throws IOException
     */
    private void deleteRules(final String bearerToken, final List<String> existingRules)
            throws URISyntaxException, IOException {

        final CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        final URIBuilder uriBuilder = new URIBuilder(configData.getTwitterV2RulesBaseUrl());

        final HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        final StringEntity body =
                new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
        httpPost.setEntity(body);
        final HttpResponse response = httpClient.execute(httpPost);
        final HttpEntity entity = response.getEntity();
        if (null != entity) {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    private String getFormattedString(final String string, List<String> ids) {
        StringBuilder sb = new StringBuilder();
        if (ids.size() == 1) {
            return String.format(string, "\"" + ids.get(0) + "\"");
        } else {
            for (String id : ids) {
                sb.append("\"" + id + "\"" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }

    private String getFormattedString(final String string, final Map<String, String> rules) {
        StringBuilder sb = new StringBuilder();
        if (rules.size() == 1) {
            String key = rules.keySet().iterator().next();
            return String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
        } else {
            for (Map.Entry<String, String> entry : rules.entrySet()) {
                String value = entry.getKey();
                String tag = entry.getValue();
                sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }

    private String getFormattedTweet(final String data) {
        final JSONObject jsonObject = new JSONObject(data).getJSONObject("data");
        final String[] params = new String[]{
                ZonedDateTime.parse(jsonObject.getString("created_at").toString())
                        .withZoneSameInstant(ZoneId.of("UTC"))
                        .format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                jsonObject.getString("id"),
                jsonObject.getString("text").replaceAll("\"", "\\\\\""),
                jsonObject.getString("author_id")
        };

        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(final String[] params) {
        final String tweet = tweetAsRawJson;
        int i = 0;
        for (final String param : params) {
            tweet.replace("{" + (i++) + "}", param);
        }
        return tweet;
    }
}
