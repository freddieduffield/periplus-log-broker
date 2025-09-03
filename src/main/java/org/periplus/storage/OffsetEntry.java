package org.periplus.storage;

public record OffsetEntry(long logicalOffset, long filePosition) implements Comparable<OffsetEntry> {

    @Override
    public int compareTo(OffsetEntry o) {
        return Long.compare(this.logicalOffset, o.logicalOffset);
    }
}
