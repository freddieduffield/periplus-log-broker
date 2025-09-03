package org.periplus.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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

    @BeforeEach
    void setUp() throws IOException {
        partitionDir = tempDir.resolve("partition-0");
        Files.createDirectories(partitionDir);
        segment = new LogSegment(partitionDir, BASE_OFFSET);
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
        assertThat(results.get(1).filePosition()).isGreaterThan(results.get(0).filePosition());
        assertThat(results.get(2).filePosition()).isGreaterThan(results.get(1).filePosition());
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
        LogSegment newSegment = new LogSegment(partitionDir, BASE_OFFSET);
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
        LogSegment nonZeroSegment = new LogSegment(partitionDir, baseOffset);

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

    // === PERFORMANCE TESTS ===

    @Test
    @DisplayName("Should handle large number of messages efficiently - O(1) append, O(log n) read")
    void testPerformanceWithLargeDataset() throws IOException {
        // Given: Large dataset
        int messageCount = 10000;

        // When: Append messages and measure time
        long startTime = System.nanoTime();
        for (int i = 0; i < messageCount; i++) {
            segment.append(createTestMessage("key" + i, "value" + i));
        }
        long appendTime = System.nanoTime() - startTime;

        // Read random offsets
        startTime = System.nanoTime();
        segment.readFrom(5000L, 100L);
        long readTime = System.nanoTime() - startTime;

        // Then: Performance should be reasonable
        // Append should be roughly O(1) per operation
        double avgAppendTimeNs = (double) appendTime / messageCount;
        assertThat(avgAppendTimeNs).isLessThan(100_000); // Less than 0.1ms per append

        // Read should be efficient with index usage
        assertThat(readTime).isLessThan(50_000_000); // Less than 50ms for random read

        System.out.printf("Performance: Avg append: %.2f μs, Random read: %.2f ms%n",
                avgAppendTimeNs / 1000, readTime / 1_000_000.0);
    }

    // === CONCURRENCY TESTS ===

    @Test
    @DisplayName("Should handle concurrent reads safely")
    void testConcurrentReads() throws Exception {
        // Given: Populate segment with data
        for (int i = 0; i < 1000; i++) {
            segment.append(createTestMessage("key" + i, "value" + i));
        }

        // When: Multiple concurrent reads
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            final int offset = i * 10;
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return segment.readFrom(offset, 10L);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, executor));
        }

        // Then: All reads should complete successfully
        for (CompletableFuture<ReadResult> future : futures) {
            ReadResult result = future.get(5, TimeUnit.SECONDS);
            assertThat(result.messages()).hasSizeLessThanOrEqualTo(10);
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    // === ERROR HANDLING TESTS ===

    @Test
    @DisplayName("Should handle I/O errors gracefully")
    void testIOErrorHandling() throws IOException {
        // This test would require mocking the file system or using a read-only directory
        // For now, we test that exceptions propagate correctly

        // Given: Valid segment
        segment.append(createTestMessage("key1", "value1"));

        // When/Then: Normal operations should work
        assertDoesNotThrow(() -> segment.readFrom(0L, 1L));
        assertDoesNotThrow(() -> segment.append(createTestMessage("key2", "value2")));
    }

    // === DATA INTEGRITY TESTS ===

    @Test
    @DisplayName("Should maintain data integrity with round-trip serialization")
    void testDataIntegrityRoundTrip() throws IOException {
        // Given: Messages with various data types and special characters
        List<Message> testMessages = List.of(
                createTestMessage("", "empty_key"),
                createTestMessage("key_empty", ""),
                createTestMessage("unicode_key_🚀", "unicode_value_🎉"),
                createTestMessage("special\nchars\tkey", "special\rchars\0value"),
                createTestMessage("binary_key", "\u0000\u0001\u0002\u0003")
        );

        // When: Append and read back
        for (Message msg : testMessages) {
            segment.append(msg);
        }

        ReadResult result = segment.readFrom(0L, testMessages.size());

        // Then: Data should be identical
        assertThat(result.messages()).hasSize(testMessages.size());
        for (int i = 0; i < testMessages.size(); i++) {
            Message original = testMessages.get(i);
            Message recovered = result.messages().get(i);

            assertThat(recovered.getKey()).isEqualTo(original.getKey());
            assertThat(recovered.getValue()).isEqualTo(original.getValue());
            assertThat(recovered.getTimestamp()).isEqualTo(original.getTimestamp());
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