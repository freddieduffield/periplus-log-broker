package org.periplus.storage;

import org.periplus.network.serialization.BinaryMessageSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LogSegment {
    private final Path segmentDirectory;
    private final long baseOffset;
    private long nextOffset;
    private final LogFile logFile;
    private final OffsetIndex offsetIndex;

    public LogSegment(Path partitionDir, long baseOffset) throws IOException {
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;

        this.segmentDirectory = partitionDir.resolve("segment-" +
                String.format("%016d", baseOffset));
        Files.createDirectories(segmentDirectory);

        Path logFilePath = segmentDirectory.resolve("log");
        Path indexFilePath = segmentDirectory.resolve("index");

        this.logFile = new LogFile(logFilePath);
        this.offsetIndex = new OffsetIndex(indexFilePath);

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

    public byte[] readFrom(long startOffset, long maxCount) {
        BinaryMessageSerializer serializer = new BinaryMessageSerializer();
//          Read messages sequentially, tracking current offset

            long currentOffset = offsetIndex.findPositionForOffset(startOffset);
            while (currentOffset < maxCount) {
                if (currentOffset >= startOffset) {
                    byte[] messageBytes = logFile.readBytesAtPosition(startOffset, )
                    Message message = serializer.deserialize(messageBytes);
                }
                currentOffset++;

            }
//          Skip messxages until you reach start_offset
//          Collect messages until you hit max_count or end of segment
    }
}
