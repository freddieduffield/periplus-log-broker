package org.periplus.storage;

import java.util.List;
import java.util.Optional;

public record ReadResult(List<Message> messages, Optional<CorruptionInfo> corruption) {
    public ReadResult(List<Message> messages) {
        this(messages, Optional.empty());
    }
}

