package org.periplus.storage;

public record CorruptionInfo(long offset, long filePosition, String errorType) {
}
