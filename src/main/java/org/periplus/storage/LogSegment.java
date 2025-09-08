package org.periplus.storage;

import org.periplus.config.BrokerConfig;
import org.periplus.network.serialization.BinaryMessageSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class LogSegment {
    private final Path segmentDirectory;
    private final long baseOffset;
    private final LogFile logFile;
    private final OffsetIndex offsetIndex;
    private long nextOffset;
    private final BrokerConfig config;

    public LogSegment(Path partitionDir, long baseOffset, BrokerConfig config) throws IOException {
        this.config = config;
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;

        this.segmentDirectory = partitionDir.resolve("segment-" +
                String.format("%016d", baseOffset));
        Files.createDirectories(segmentDirectory);

        Path logFilePath = segmentDirectory.resolve("log");
        Path indexFilePath = segmentDirectory.resolve("index");

        this.logFile = new LogFile(logFilePath);
        this.offsetIndex = new OffsetIndex(indexFilePath, config.maxIndexEntries());

        if (offsetIndex.getLastOffset().isPresent()) {
            this.nextOffset = offsetIndex.getLastOffset().get() + 1;
        }
    }

    public OffsetEntry append(Message message) throws IOException {
        long currentPosition = logFile.getCurrentPosition();

        BinaryMessageSerializer serializer = new BinaryMessageSerializer();
        byte[] bytes = serializer.serialize(message);
        logFile.append(bytes);

        long assignedOffset = nextOffset;

        offsetIndex.addEntry(assignedOffset, currentPosition);

        nextOffset++;

        return new OffsetEntry(assignedOffset, currentPosition);
    }

    public ReadResult readFrom(long startOffset, long maxCount) throws IOException {
        if (startOffset < baseOffset) {
            throw new IllegalArgumentException("startOffset " + startOffset + " is before baseOffset " + baseOffset);
        }

        BinaryMessageSerializer serializer = new BinaryMessageSerializer();
        List<Message> messages = new ArrayList<>();
        // Read messages sequentially, tracking current offset
        OffsetEntry currentOffsetEntry = offsetIndex.findPositionForOffset(startOffset);
        if (currentOffsetEntry == null) {
            return new ReadResult(messages);
        }
        long currentFilePosition = currentOffsetEntry.filePosition();
        long currentLogicalOffset = currentOffsetEntry.logicalOffset();

        while (messages.size() < maxCount && currentLogicalOffset < nextOffset) {
            // Skip messages until you reach start_offset
            byte[] messageLengthBytes = logFile.readBytesAtPosition(currentFilePosition, 4);

            if (messageLengthBytes.length < 1) {
                break;
            }

            int messageLength = ByteBuffer.wrap(messageLengthBytes).getInt();
            currentFilePosition += 4;
            byte[] messageBytes = logFile.readBytesAtPosition(currentFilePosition, messageLength);
            currentFilePosition += messageLength;

            // Collect messages until you hit max_count or end of segment
            if (currentLogicalOffset >= startOffset) {
                messages.add(serializer.deserialize(messageBytes));
            }
            currentLogicalOffset++;
        }

        return new ReadResult(messages);
    }

    public boolean isFull() throws IOException {
        return logFile.getCurrentFileSize() >= config.segmentSizeBytes() ||
                offsetIndex.getEntries().size() >= config.maxIndexEntries();
    }
}
