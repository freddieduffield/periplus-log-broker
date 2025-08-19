package org.periplus.network.serialization;

import org.periplus.storage.Message;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
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
                writeString(dos, header.getKey());
                writeString(dos, header.getValue());
            }

            byte[] messageBytes = boas.toByteArray();
            ByteArrayOutputStream finalStream = new ByteArrayOutputStream();
            DataOutputStream finalDos = new DataOutputStream(finalStream);
            finalDos.writeInt(messageBytes.length);
            finalDos.write(messageBytes);

            return finalStream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public Message deserialize(byte[] data) {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        try {
            int totalLength = dis.readInt();

            if (totalLength != data.length - 4) {
                throw new SerializationException("Invalid message length");
            }

            long timestamp = dis.readLong();

            int keyLength = dis.readInt();
            String key = null;
            if (keyLength > 0) {
                byte[] keyBytes = new byte[keyLength];
                dis.readFully(keyBytes);
                key = new String(keyBytes, StandardCharsets.UTF_8);
            }

            int valueLength = dis.readInt();
            byte[] value = new byte[valueLength];
            dis.readFully(value);

            int headerCount = dis.readInt();
            Map<String, String> headers = new HashMap<>();
            for(int i = 0; i < headerCount; i++) {
                String headerKey = readString(dis);
                String headerValue = readString(dis);
                headers.put(headerKey, headerValue);
            }

            return new Message(timestamp, key, value, headers);


        } catch (IOException e) {
            throw new SerializationException("failed to deserialize message", e);
        }
    }

    private void writeString(DataOutputStream dos, String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    public String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
