package org.example;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkTask implements Runnable {
    private final Collection collection;
    private final AtomicInteger documentIdCounter;
    private final MetricsCollector metricsCollector;
    private final Path jsonFilePath;
    private final AtomicBoolean isRunning;
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTask.class);

    public BenchmarkTask(Collection collection, AtomicInteger documentIdCounter,
                         MetricsCollector metricsCollector, Path jsonFilePath, AtomicBoolean isRunning) {
        this.collection = collection;
        this.documentIdCounter = documentIdCounter;
        this.metricsCollector = metricsCollector;
        this.jsonFilePath = jsonFilePath;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {
        while (isRunning.get()) {
            int documentId = documentIdCounter.getAndIncrement();
            String docIdStr = String.valueOf(documentId);

            try {
                // Load JSON data for this iteration
                Map<String, Object> jsonData = CouchbaseUtils.loadJsonData(jsonFilePath.toString());
                JsonObject documentToAdd = JsonObject.from(jsonData);

                // Write Operation
                long writeStart = System.nanoTime();
                try {
                    collection.upsert(docIdStr, documentToAdd);
                    metricsCollector.recordWriteLatency(System.nanoTime() - writeStart);
                } catch (Exception e) {
                    metricsCollector.incrementWriteErrors();
                    logger.error("Write operation failed: {}", e.getMessage());
                }

                // Read Operations (three times)
                for (int i = 0; i < 3; i++) {
                    long readStart = System.nanoTime();
                    try {
                        collection.get(docIdStr);
                        metricsCollector.recordReadLatency(System.nanoTime() - readStart);
                    } catch (Exception e) {
                        metricsCollector.incrementReadErrors();
                        logger.error("Read operation failed: {}", e.getMessage());
                    }
                }
            } catch (IOException e) {
                metricsCollector.incrementWriteErrors();
                logger.error("Failed to load JSON file {}: {}", jsonFilePath, e.getMessage());
            }
        }
    }
}
