package org.daniel.benchmark;

import com.couchbase.client.java.json.JsonObject;
import org.daniel.couchbase.CouchbaseClientManager;
import org.daniel.metrics.MetricsCollector;
import org.daniel.util.JsonUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Executes the benchmark by creating and managing threads that perform read/write operations on Couchbase.
 */
public class CouchbaseBenchmarkExecutor {
    private final CouchbaseClientManager couchbaseClientManager;
    private final List<Path> jsonFilePaths;
    private final String bucketName;
    private final MetricsCollector metricsCollector;
    private final AtomicInteger documentIdCounter;

    public CouchbaseBenchmarkExecutor(CouchbaseClientManager couchbaseClientManager, List<Path> jsonFilePaths, MetricsCollector metricsCollector, AtomicInteger documentIdCounter) {
        this.couchbaseClientManager = couchbaseClientManager;
        this.metricsCollector = metricsCollector;
        this.jsonFilePaths = jsonFilePaths;
        this.documentIdCounter = documentIdCounter;
        this.bucketName = couchbaseClientManager.getCollection().bucketName();
    }

    /**
     * Runs the benchmark with the specified number of threads and duration.
     *
     * @param threadCount     Number of concurrent threads to run.
     * @param durationSeconds Duration of the benchmark in seconds.
     * @throws IOException if there is an error loading JSON files.
     */
    public void runBenchmarkWithThreadCount(int threadCount, int durationSeconds) throws IOException {
        // Flush the bucket before each run
        couchbaseClientManager.getCluster().buckets().flushBucket(bucketName);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount, Thread.ofVirtual().factory());
        AtomicBoolean isRunning = new AtomicBoolean(true);

        // Submit benchmark tasks to the executor service
        for (int i = 0; i < threadCount; i++) {
            Path jsonFilePath = jsonFilePaths.get(i % jsonFilePaths.size());
            Map<String, Object> jsonData = JsonUtils.loadJsonData(jsonFilePath.toString());
            JsonObject documentToAdd = JsonObject.from(jsonData);
            executorService.submit(new BenchmarkTask(
                    couchbaseClientManager.getCollection(),
                    documentIdCounter,
                    metricsCollector,
                    documentToAdd,
                    isRunning
            ));
        }

        try {
            // Let the benchmark run for the specified duration
            Thread.sleep(TimeUnit.SECONDS.toMillis(durationSeconds));
            isRunning.set(false);  // Signal tasks to stop
            executorService.shutdown();  // Initiate shutdown
        } catch (InterruptedException e) {
            executorService.shutdownNow(); // Force shutdown
            Thread.currentThread().interrupt();
        }
    }
}
