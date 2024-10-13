package org.example;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseBenchmarkExecutor {
    private final CouchbaseClientManager couchbaseClientManager;
    private final List<Path> jsonFilePaths;
    private final SystemUsageMonitor systemUsageMonitor;

    public CouchbaseBenchmarkExecutor(CouchbaseClientManager couchbaseClientManager, List<Path> jsonFilePaths) {
        this.couchbaseClientManager = couchbaseClientManager;
        this.jsonFilePaths = jsonFilePaths;
        this.systemUsageMonitor = new SystemUsageMonitor();
    }

    public void runBenchmarkWithThreadCount(int threadCount, int durationSeconds) throws InterruptedException {
        // Clear the bucket before starting the benchmark
        CouchbaseUtils.clearBucket(couchbaseClientManager.getCluster(),
                couchbaseClientManager.getCollection().bucketName());

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        AtomicBoolean isRunning = new AtomicBoolean(true);
        MetricsCollector metricsCollector = new MetricsCollector();
        AtomicInteger documentIdCounter = new AtomicInteger();

        // Start threads
        for (int i = 0; i < threadCount; i++) {
            Path jsonFilePath = jsonFilePaths.get(i % jsonFilePaths.size()); // Round-robin assignment
            executorService.submit(new BenchmarkTask(
                    couchbaseClientManager.getCollection(),
                    documentIdCounter,
                    metricsCollector,
                    jsonFilePath,
                    isRunning
            ));
        }

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            isRunning.set(false);
            executorService.shutdownNow();
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        // Print results and system usage
        ResultPrinter.printBenchmarkResults(threadCount, metricsCollector);
        systemUsageMonitor.logSystemUsage();
    }
}
