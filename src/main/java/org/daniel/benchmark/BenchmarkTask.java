package org.daniel.benchmark;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.daniel.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a single benchmark task that performs write and read operations.
 * Each task runs in a separate thread.
 */
public class BenchmarkTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTask.class);

    private final Collection collection;
    private final AtomicInteger documentIdCounter;
    private final MetricsCollector metricsCollector;
    private final JsonObject documentToAdd;
    private final AtomicBoolean isRunning;

    public BenchmarkTask(Collection collection, AtomicInteger documentIdCounter, MetricsCollector metricsCollector, JsonObject documentToAdd, AtomicBoolean isRunning) {
        this.collection = collection;
        this.documentIdCounter = documentIdCounter;
        this.metricsCollector = metricsCollector;
        this.documentToAdd = documentToAdd;
        this.isRunning = isRunning;
    }

    /**
     * Executes the benchmark task.
     * Continuously performs write and read operations until the isRunning flag is set to false.
     */
    @Override
    public void run() {
        while (isRunning.get()) {
            int documentId = documentIdCounter.getAndIncrement();
            String docIdStr = String.valueOf(documentId);

            performWriteOperation(docIdStr);
            performReadOperations(docIdStr);
        }
    }

    /**
     * Performs a write operation to the Couchbase collection.
     *
     * @param documentId The ID of the document to write.
     */
    private void performWriteOperation(String documentId) {
        long startTime = System.nanoTime();
        try {
            collection.upsert(documentId, documentToAdd);
            long latency = System.nanoTime() - startTime;
            metricsCollector.recordWriteLatency(latency);
        } catch (Exception e) {
            metricsCollector.incrementWriteErrors();
            if (logger.isDebugEnabled()) {
                logger.debug("Write operation failed for document ID {}: {}", documentId, e.getMessage(), e);
            }
        }
    }

    /**
     * Performs multiple read operations from the Couchbase collection.
     *
     * @param documentId The ID of the document to read.
     */
    private void performReadOperations(String documentId) {
        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            try {
                collection.get(documentId);
                long latency = System.nanoTime() - startTime;
                metricsCollector.recordReadLatency(latency);
            } catch (Exception e) {
                metricsCollector.incrementReadErrors();
                logger.debug("Read operation failed for document ID {}: {}", documentId, e.getMessage(), e);
            }
        }
    }
}
