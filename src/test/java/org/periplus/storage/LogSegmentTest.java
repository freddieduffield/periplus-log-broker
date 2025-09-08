package org.periplus.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.periplus.config.BrokerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive test suite for LogSegment class.
 * Tests functionality, performance, concurrency, and edge cases.
 */
class LogSegmentTest {

    private static final long BASE_OFFSET = 0L;
    @TempDir
    Path tempDir;
    private Path partitionDir;
    private LogSegment segment;
    private BrokerConfig config;

    @BeforeEach
    void setUp() throws IOException {
        partitionDir = tempDir.resolve("partition-0");
        Files.createDirectories(partitionDir);
        String configFile = "broker.properties";
        config = BrokerConfig.fromProperties(configFile);
        segment = new LogSegment(partitionDir, BASE_OFFSET, config);
    }

    // === BASIC FUNCTIONALITY TESTS ===

    @Test
    @DisplayName("Should create segment directory and files on initialization")
    void testSegmentInitialization() throws IOException {
        // Given: Fresh LogSegment (created in setUp)

        // Then: Directory structure should exist
        Path expectedSegmentDir = partitionDir.resolve("segment-0000000000000000");
        assertTrue(Files.exists(expectedSegmentDir));
        assertTrue(Files.exists(expectedSegmentDir.resolve("log")));
        assertTrue(Files.exists(expectedSegmentDir.resolve("index")));
    }

    @Test
    @DisplayName("Should append single message and assign correct offset")
    void testSingleMessageAppend() throws IOException {
        // Given
        Message message = createTestMessage("key1", "value1");

        // When
        OffsetEntry result = segment.append(message);

        // Then
        assertThat(result.logicalOffset()).isEqualTo(0L);
        assertThat(result.filePosition()).isEqualTo(0L);
    }

    @Test
    @DisplayName("Should append multiple messages with sequential offsets")
    void testMultipleMessageAppend() throws IOException {
        // Given
        List<Message> messages = List.of(
                createTestMessage("key1", "value1"),
                createTestMessage("key2", "value2"),
                createTestMessage("key3", "value3")
        );

        // When
        List<OffsetEntry> results = new ArrayList<>();
        for (Message msg : messages) {
            results.add(segment.append(msg));
        }

        // Then: Offsets should be sequential
        assertThat(results).hasSize(3);
        assertThat(results.get(0).logicalOffset()).isEqualTo(0L);
        assertThat(results.get(1).logicalOffset()).isEqualTo(1L);
        assertThat(results.get(2).logicalOffset()).isEqualTo(2L);

        // File positions should increase (messages take space)
        assertThat(results.get(1).filePosition()).isGreaterThanOrEqualTo(results.get(0).filePosition());
        assertThat(results.get(2).filePosition()).isGreaterThanOrEqualTo(results.get(1).filePosition());
    }

    @Test
    @DisplayName("Should read messages from start offset")
    void testReadFromStartOffset() throws IOException {
        // Given: Append some messages
        List<Message> originalMessages = List.of(
                createTestMessage("key1", "value1"),
                createTestMessage("key2", "value2"),
                createTestMessage("key3", "value3")
        );

        for (Message msg : originalMessages) {
            segment.append(msg);
        }

        // When: Read from offset 0
        ReadResult result = segment.readFrom(0L, 10L);

        // Then: Should get all messages
        assertThat(result.messages()).hasSize(3);
        assertMessageEquals(result.messages().get(0), "key1", "value1");
        assertMessageEquals(result.messages().get(1), "key2", "value2");
        assertMessageEquals(result.messages().get(2), "key3", "value3");
    }

    @Test
    @DisplayName("Should read messages from middle offset")
    void testReadFromMiddleOffset() throws IOException {
        // Given
        for (int i = 0; i < 5; i++) {
            segment.append(createTestMessage("key" + i, "value" + i));
        }

        // When: Read from offset 2
        ReadResult result = segment.readFrom(2L, 10L);

        // Then: Should get messages 2, 3, 4
        assertThat(result.messages()).hasSize(3);
        assertMessageEquals(result.messages().get(0), "key2", "value2");
        assertMessageEquals(result.messages().get(1), "key3", "value3");
        assertMessageEquals(result.messages().get(2), "key4", "value4");
    }

    @Test
    @DisplayName("Should respect max count limit")
    void testReadWithMaxCount() throws IOException {
        // Given
        for (int i = 0; i < 10; i++) {
            segment.append(createTestMessage("key" + i, "value" + i));
        }

        // When: Read with max count of 3
        ReadResult result = segment.readFrom(0L, 3L);

        // Then: Should only get 3 messages
        assertThat(result.messages()).hasSize(3);
        assertMessageEquals(result.messages().get(0), "key0", "value0");
        assertMessageEquals(result.messages().get(1), "key1", "value1");
        assertMessageEquals(result.messages().get(2), "key2", "value2");
    }

    // === PERSISTENCE TESTS ===

    @Test
    @DisplayName("Should persist and recover data after restart")
    void testPersistenceAfterRestart() throws IOException {
        // Given: Append messages to original segment
        segment.append(createTestMessage("key1", "value1"));
        segment.append(createTestMessage("key2", "value2"));

        // When: Create new segment with same base offset (simulating restart)
        LogSegment newSegment = new LogSegment(partitionDir, BASE_OFFSET, config);
        newSegment.append(createTestMessage("key3", "value3")); // Should continue from offset 2

        ReadResult result = newSegment.readFrom(0L, 10L);

        // Then: Should have all messages with correct offsets
        assertThat(result.messages()).hasSize(3);
        assertMessageEquals(result.messages().get(0), "key1", "value1");
        assertMessageEquals(result.messages().get(1), "key2", "value2");
        assertMessageEquals(result.messages().get(2), "key3", "value3");
    }

    // === EDGE CASES ===

    @Test
    @DisplayName("Should handle read from non-existent offset")
    void testReadFromNonExistentOffset() throws IOException {
        // Given: Append only 2 messages (offsets 0, 1)
        segment.append(createTestMessage("key1", "value1"));
        segment.append(createTestMessage("key2", "value2"));

        // When: Try to read from offset 5
        ReadResult result = segment.readFrom(5L, 10L);

        // Then: Should return empty result
        assertThat(result.messages()).isEmpty();
    }

    @Test
    @DisplayName("Should handle read with maxCount = 0")
    void testReadWithZeroMaxCount() throws IOException {
        // Given
        segment.append(createTestMessage("key1", "value1"));

        // When
        ReadResult result = segment.readFrom(0L, 0L);

        // Then
        assertThat(result.messages()).isEmpty();
    }

    @Test
    @DisplayName("Should handle empty segment read")
    void testReadFromEmptySegment() throws IOException {
        // When: Read from empty segment
        ReadResult result = segment.readFrom(0L, 10L);

        // Then
        assertThat(result.messages()).isEmpty();
    }

    @ParameterizedTest
    @DisplayName("Should handle various message sizes efficiently")
    @ValueSource(ints = {1, 100, 1024, 10240})
        // Small to medium message sizes
    void testVariousMessageSizes(int valueSize) throws IOException {
        // Given: Message with specific payload size
        String largeValue = "x".repeat(valueSize);
        Message message = createTestMessage("key", largeValue);

        // When
        OffsetEntry entry = segment.append(message);
        ReadResult result = segment.readFrom(0L, 1L);

        // Then
        assertThat(entry.logicalOffset()).isEqualTo(0L);
        assertThat(result.messages()).hasSize(1);
        assertThat(result.messages().get(0).getValue()).isEqualTo(largeValue);
    }

    // === NON-ZERO BASE OFFSET TESTS ===

    @ParameterizedTest
    @DisplayName("Should handle non-zero base offsets correctly")
    @CsvSource({
            "100, 0",
            "1000, 5",
            "50000, 10"
    })
    void testNonZeroBaseOffset(long baseOffset, int messageCount) throws IOException {
        // Given: Segment with non-zero base offset
        LogSegment nonZeroSegment = new LogSegment(partitionDir, baseOffset, config);

        // When: Append messages
        List<OffsetEntry> entries = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            entries.add(nonZeroSegment.append(createTestMessage("key" + i, "value" + i)));
        }

        // Then: Offsets should start from baseOffset
        if (messageCount > 0) {
            assertThat(entries.get(0).logicalOffset()).isEqualTo(baseOffset);
            assertThat(entries.get(messageCount - 1).logicalOffset()).isEqualTo(baseOffset + messageCount - 1);

            // Read verification
            ReadResult result = nonZeroSegment.readFrom(baseOffset, messageCount);
            assertThat(result.messages()).hasSize(messageCount);
        }
    }

    // === HELPER METHODS ===

    private Message createTestMessage(String key, String value) {
        return new Message(
                System.currentTimeMillis(),
                key,
                value,
                new HashMap<>() // Empty headers for simplicity
        );
    }

    private void assertMessageEquals(Message actual, String expectedKey, String expectedValue) {
        assertThat(actual.getKey()).isEqualTo(expectedKey);
        assertThat(actual.getValue()).isEqualTo(expectedValue);
    }
}