package org.periplus.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class LogFileTest {
    @Test
    public void testWhenPathIsNull()  {
        assertThrows(NullPointerException.class, () -> new LogFile(null));
    }

    @Test
    public void testLogFile(@TempDir Path tempDir) throws IOException {
        Path testFile = tempDir.resolve("test-channel.dat");
        LogFile logFile = new LogFile(testFile);

        assertTrue(Files.exists(testFile));
        assertEquals(0, logFile.getCurrentFileSize());

        byte[] bytes = new byte[1024];
        Arrays.fill(bytes, (byte) 'a');
        logFile.append(bytes);
        byte[] bytesReadFromFile = logFile.readBytesAtPosition(0, 1024);
        assertArrayEquals(bytes, bytesReadFromFile);
    }
}
