package com.microservices.demo.kafka.admin.exception;

/**
 * Exception class for Kafka client error situations
 */
public class KafkaClientException extends RuntimeException {
    public KafkaClientException() {
    }
    public KafkaClientException(final String message) {
        super(message);
    }
    public KafkaClientException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
