package org.periplus.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class BrokerConfig {
    // Default values as constants (Effective Java Item 22)
    private static final long DEFAULT_SEGMENT_SIZE_BYTES = 1024L * 1024 * 1024; // 1GB
    private static final int DEFAULT_MAX_INDEX_ENTRIES = 10_000_000;
    private static final String DEFAULT_DATA_DIRECTORY = "./data";

    private final long segmentSizeBytes;
    private final int maxIndexEntries;
    private final String dataDirectory;
    private final int brokerId;
    private final String listenAddress;

    private BrokerConfig(Builder builder) {
        this.segmentSizeBytes = builder.segmentSizeBytes;
        this.maxIndexEntries = builder.maxIndexEntries;
        this.dataDirectory = builder.dataDirectory;
        this.brokerId = builder.brokerId;
        this.listenAddress = builder.listenAddress;
    }

    // Modern factory method (Java 9+)
    public static BrokerConfig fromProperties(String configFile) throws IOException {
        var props = new Properties();
        try (var input = Files.newBufferedReader(Path.of(configFile))) {
            props.load(input);
        }

        return new Builder()
                .segmentSizeBytes(parseLong(props, "log.segment.bytes", DEFAULT_SEGMENT_SIZE_BYTES))
                .maxIndexEntries(parseInt(props, "log.index.max.entries", DEFAULT_MAX_INDEX_ENTRIES))
                .dataDirectory(props.getProperty("log.data.directory", DEFAULT_DATA_DIRECTORY))
                .brokerId(parseInt(props, "broker.id"))
                .listenAddress(props.getProperty("network.listen.address"))
                .build();
    }

    // Helper methods
    private static long parseLong(Properties props, String key, long defaultValue) {
        String value = props.getProperty(key);
        return value != null ? Long.parseLong(value) : defaultValue;
    }

    private static int parseInt(Properties props, String key, int defaultValue) {
        String value = props.getProperty(key);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    private static int parseInt(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Required property missing: " + key);
        }
        return Integer.parseInt(value);
    }

    // Getters
    public long segmentSizeBytes() {
        return segmentSizeBytes;
    }

    public int maxIndexEntries() {
        return maxIndexEntries;
    }

    public String dataDirectory() {
        return dataDirectory;
    }

    // Builder pattern (Effective Java Item 2)
    public static class Builder {
        private long segmentSizeBytes = DEFAULT_SEGMENT_SIZE_BYTES;
        private int maxIndexEntries = DEFAULT_MAX_INDEX_ENTRIES;
        private String dataDirectory = DEFAULT_DATA_DIRECTORY;
        private int brokerId = -1;
        private String listenAddress;

        public Builder segmentSizeBytes(long segmentSizeBytes) {
            this.segmentSizeBytes = segmentSizeBytes;
            return this;
        }

        public Builder maxIndexEntries(int maxIndexEntries) {
            this.maxIndexEntries = maxIndexEntries;
            return this;
        }
        public Builder dataDirectory(String dataDirectory) {
            this.dataDirectory = dataDirectory;
            return this;
        }

        public Builder brokerId(int brokerId) {
            this.brokerId = brokerId;
            return this;
        }

        public Builder listenAddress(String listenAddress) {
            this.listenAddress = listenAddress;
            return this;
        }

        public BrokerConfig build() {
            validateConfig();
            return new BrokerConfig(this);
        }

        private void validateConfig() {
            if (brokerId < 0) throw new IllegalStateException("broker.id must be set");
            if (listenAddress == null) throw new IllegalStateException("network.listen.address must be set");
            if (segmentSizeBytes <= 0) throw new IllegalArgumentException("log.segment.bytes must be positive");
        }
    }
}