package org.periplus.storage;

import org.junit.jupiter.api.Test;
import org.periplus.network.serialization.BinaryMessageSerializer;
import org.periplus.network.serialization.MessageSerializer;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageTest {
    @Test
    public void testRoundTripSerialization() {
        Message originalMessage = new Message(
                1755632474,
                "wassup",
                "Hello, world",
                Map.of("content-type", "binary")
        );

        MessageSerializer messageSerializer = new BinaryMessageSerializer();
        byte[] serializedMessage = messageSerializer.serialize(originalMessage);
        Message deserializedMessage = messageSerializer.deserialize(serializedMessage);

        assertEquals(deserializedMessage.getTimestamp(), originalMessage.getTimestamp());
        assertEquals(deserializedMessage.getHeaders(), originalMessage.getHeaders());
        assertEquals(deserializedMessage.getKey(), originalMessage.getKey());
        assertEquals(deserializedMessage.getValue(), originalMessage.getValue());
    }

    @Test
    public void testNullKeyHandling() {
        Message message = new Message(123L, null, "data", Map.of());
        MessageSerializer serializer = new BinaryMessageSerializer();
        byte[] serialized = serializer.serialize(message);
        Message deserialized = serializer.deserialize(serialized);

        assertEquals(deserialized.getKey(), null);


    }
}
