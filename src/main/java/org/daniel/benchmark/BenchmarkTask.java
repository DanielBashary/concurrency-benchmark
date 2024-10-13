package org.daniel.benchmark;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.daniel.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkTask implements Runnable {
    private final Collection collection;
    private final AtomicInteger documentIdCounter;
    private final MetricsCollector metricsCollector;
    private final JsonObject documentToAdd;
    private final AtomicBoolean isRunning;
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTask.class);

    public BenchmarkTask(Collection collection, AtomicInteger documentIdCounter,
                         MetricsCollector metricsCollector, JsonObject documentToAdd, AtomicBoolean isRunning) {
        this.collection = collection;
        this.documentIdCounter = documentIdCounter;
        this.metricsCollector = metricsCollector;
        this.documentToAdd = documentToAdd;
        this.isRunning = isRunning;
    }

    /**
     * Executes the benchmark task. Continuously performs a single write operation and 3 read operations
     * continuously runs until the isRunning flag is set to false.
     */
    @Override
    public void run() {
        while (isRunning.get()) {
            int documentId = documentIdCounter.getAndIncrement();
            String docIdStr = String.valueOf(documentId);

            long writeStart = System.nanoTime();
            try {
                collection.upsert(docIdStr, documentToAdd);
                metricsCollector.recordWriteLatency(System.nanoTime() - writeStart);
            } catch (Exception e) {
                metricsCollector.incrementWriteErrors();
                logger.error("Write operation failed for document ID {}: {}", docIdStr, e.getMessage(), e);
            }

            for (int i = 0; i < 3; i++) {
                long readStart = System.nanoTime();
                try {
                    collection.get(docIdStr);
                    metricsCollector.recordReadLatency(System.nanoTime() - readStart);
                } catch (Exception e) {
                    metricsCollector.incrementReadErrors();
                    logger.error("Read operation failed for document ID {}: {}", docIdStr, e.getMessage(), e);
                }
            }
        }
    }
}
