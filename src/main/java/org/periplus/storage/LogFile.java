package org.periplus.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class LogFile {
    private final FileChannel channel;

    public LogFile(Path path) throws IOException {
        Path channelPath = Objects.requireNonNull(path, "Channel path cannot be null");
        this.channel = FileChannel.open(channelPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.SYNC
        );
    }

    public synchronized void append(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message);
        while (buffer.hasRemaining()) {
            channel.write(buffer, channel.size());
        }
    }

    public byte[] readBytesAtPosition(long offset, int length) throws IOException {
        // offset indicates line
        if (channel.size() - channel.position() < length) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        channel.read(buffer, offset);
        buffer.flip();
        return buffer.array();
    }

    public long getCurrentFileSize() throws IOException {
        return channel.size();
    }

    public long getCurrentPosition() throws IOException {
        return channel.position();
    }

}
