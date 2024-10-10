package org.example;

import java.util.concurrent.atomic.LongAdder;

public class MetricsCollector {
    private final LongAdder totalWriteLatency = new LongAdder();
    private final LongAdder totalReadLatency = new LongAdder();
    private final LongAdder writeOperations = new LongAdder();
    private final LongAdder readOperations = new LongAdder();
    private final LongAdder writeErrors = new LongAdder();
    private final LongAdder readErrors = new LongAdder();

    public void recordWriteLatency(long latency) {
        totalWriteLatency.add(latency);
        writeOperations.increment();
    }

    public void recordReadLatency(long latency) {
        totalReadLatency.add(latency);
        readOperations.increment();
    }

    public void incrementWriteErrors() {
        writeErrors.increment();
    }

    public void incrementReadErrors() {
        readErrors.increment();
    }

    // Getters for metrics
    public long getTotalWriteLatency() {
        return totalWriteLatency.sum();
    }

    public long getTotalReadLatency() {
        return totalReadLatency.sum();
    }

    public long getWriteOperations() {
        return writeOperations.sum();
    }

    public long getReadOperations() {
        return readOperations.sum();
    }

    public long getWriteErrors() {
        return writeErrors.sum();
    }

    public long getReadErrors() {
        return readErrors.sum();
    }
}
