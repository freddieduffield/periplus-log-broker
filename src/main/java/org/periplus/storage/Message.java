package org.periplus.storage;

import java.time.Instant;
import java.util.Map;

public class Message {
    private long timestamp;
    private String key;
    private String value;
    private Map<String, String> headers;

    public Message(long timestamp, String key, String value, Map<String, String> headers) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    public static Message create(String key, String value, Map<String, String> headers) {
        return new Message(Instant.now().toEpochMilli(), key, value, headers);
    }

    public static Message create(Instant timestamp, String key, String value, Map<String, String> headers) {
        return new Message(timestamp.toEpochMilli(), key, value, headers);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }
}