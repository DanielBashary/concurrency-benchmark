package org.example;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class BenchmarkTask implements Runnable {
    private final Collection collection;
    private final AtomicInteger documentIdCounter;
    private final JsonObject documentToAdd;
    private final MetricsCollector metricsCollector;

    public BenchmarkTask(Collection collection, AtomicInteger documentIdCounter, JsonObject documentToAdd, MetricsCollector metricsCollector) {
        this.collection = collection;
        this.documentIdCounter = documentIdCounter;
        this.documentToAdd = documentToAdd;
        this.metricsCollector = metricsCollector;
    }

    @Override
    public void run() {
        int documentId = documentIdCounter.getAndIncrement();
        String docIdStr = String.valueOf(documentId);

        // Write Operation
        long writeStart = System.nanoTime();
        try {
            collection.upsert(docIdStr, documentToAdd);
            metricsCollector.recordWriteLatency(System.nanoTime() - writeStart);
        } catch (Exception e) {
            metricsCollector.incrementWriteErrors();
            LoggerFactory.getLogger(BenchmarkTask.class).error("Write operation failed: {}", e.getMessage());
        }

        // Read Operations
        for (int i = 0; i < 3; i++) {
            long readStart = System.nanoTime();
            try {
                collection.get(docIdStr);
                metricsCollector.recordReadLatency(System.nanoTime() - readStart);
            } catch (Exception e) {
//                metricsCollector.incrementReadErrors();
//                LoggerFactory.getLogger(BenchmarkTask.class).error("Read operation failed: {}", e.getMessage());
            }
        }
    }
}
