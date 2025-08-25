package org.periplus.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LogFile {
    private final FileChannel channel;

    public LogFile(Path path) throws IOException {
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC);
    }

    public synchronized void append(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message);
        channel.write(buffer);
    }

    public byte[] readBytesAtPosition(int offset, int length) throws IOException {
    // offset indicates line
        ByteBuffer buffer = ByteBuffer.allocate(length);
        channel.read(buffer, offset);
        buffer.flip();
        return buffer.array();
    }
}
