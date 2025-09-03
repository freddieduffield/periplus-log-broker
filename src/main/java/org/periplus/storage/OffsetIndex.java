package org.periplus.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class OffsetIndex {
    private final List<OffsetEntry> entries = new ArrayList<OffsetEntry>();
    private final Path indexToFilePath;

    public OffsetIndex(Path indexToFilePath) throws IOException {
        this.indexToFilePath = indexToFilePath;
        loadFromDisk();
    }

    public void addEntry(long offset, long position) {
        entries.add(new OffsetEntry(offset, position));
    }

    public OffsetEntry findPositionForOffset(long targetOffset) {
        //  finding starting points for sequential reads, not exact lookups - floor search
        int index = Collections.binarySearch(entries, new OffsetEntry(targetOffset, 0));
        if (index >= 0) {
            return entries.get(index);
        }

        int insertionPoint = -index - 1;
        if (insertionPoint > 0) {
            return entries.get(insertionPoint - 1);
        }

        return null;
    }

    public void saveToDisk() throws IOException {
        try (var channel = FileChannel.open(indexToFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            var buffer  = ByteBuffer.allocateDirect(entries.size() * 16);
            for (var entry: entries) {
                buffer.putLong(entry.logicalOffset());
                buffer.putLong(entry.filePosition());
            }

            buffer.flip();
            channel.write(buffer);
            channel.force(true);
        }
    }

    public void loadFromDisk() throws IOException {
        if (!Files.exists(indexToFilePath)) {
            return;
        }

        entries.clear();
        try (var channel = FileChannel.open(indexToFilePath, StandardOpenOption.READ)) {
            var buffer = ByteBuffer.allocateDirect((int) channel.size());
            channel.read(buffer);
            buffer.flip();

            while(buffer.hasRemaining()) {
                long logicalOffset = buffer.getLong();
                long filePosition = buffer.getLong();
                entries.add(new OffsetEntry(logicalOffset, filePosition));
            }
        }
    }

    public Optional<Long> getLastOffset() {
        return entries.isEmpty() ?
                Optional.empty()
                : Optional.of(entries.get(entries.size() - 1).logicalOffset());
    }
}
