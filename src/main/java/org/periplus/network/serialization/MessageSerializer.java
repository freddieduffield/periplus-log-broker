package org.periplus.network.serialization;

import org.periplus.storage.Message;

public interface MessageSerializer {
    byte[] serialize(Message message);
    Message deserialize(byte[] data);
}