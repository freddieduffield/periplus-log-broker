package org.periplus.network.serialization;

import org.periplus.storage.Message;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BinaryMessageSerializer implements MessageSerializer {
    @Override
    public byte[] serialize(Message message) {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(boas);

        try {
            dos.writeLong(message.getTimestamp());

            if (message.getKey() != null) {
                byte[] keyBytes = message.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
            } else {
                dos.writeInt(0);
            }

            dos.writeInt(message.getValue().length);
            dos.write(message.getValue());

            dos.writeInt(message.getHeaders().size());
            for (Map.Entry<String, String> header : message.getHeaders().entrySet()) {
                dos.write(header.getKey().getBytes());
                dos.write(header.getValue().getBytes());
            }

            byte[] messageBytes = boas.toByteArray();
            ByteArrayOutputStream finalStream = new ByteArrayOutputStream();
            DataOutputStream finalDos = new DataOutputStream(finalStream);
            finalDos.write(messageBytes.length);
            finalDos.write(messageBytes);

            return finalStream.toByteArray();

        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public Message deserialize(byte[] data) {
        return null;
    }
}
