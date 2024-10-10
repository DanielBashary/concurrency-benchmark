package org.example;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseBenchmarkExecutor {
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseBenchmarkExecutor.class);
    private final CouchbaseClientManager couchbaseClientManager;
    private final List<Path> jsonFilePaths;
    private final SystemUsageMonitor systemUsageMonitor;

    public CouchbaseBenchmarkExecutor(CouchbaseClientManager couchbaseClientManager, List<Path> jsonFilePaths) {
        this.couchbaseClientManager = couchbaseClientManager;
        this.jsonFilePaths = jsonFilePaths;
        this.systemUsageMonitor = new SystemUsageMonitor();
    }

    public void runBenchmarkWithThreadCount(int threadCount, int durationSeconds) {
        // Clear the bucket before starting the benchmark
        CouchbaseUtils.clearBucket(couchbaseClientManager.getCluster(),
                couchbaseClientManager.getCollection().bucketName());

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        AtomicBoolean isRunning = new AtomicBoolean(true);
        MetricsCollector metricsCollector = new MetricsCollector();
        AtomicInteger documentIdCounter = new AtomicInteger();

        // Schedule termination after the specified duration
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(() -> {
            isRunning.set(false);
            executorService.shutdownNow();
            logger.info("Benchmark duration elapsed. Shutting down executor service.");
        }, durationSeconds, TimeUnit.SECONDS);

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

        // Await termination
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(durationSeconds + 10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        scheduler.shutdownNow();

        // Print results and system usage
        ResultPrinter.printBenchmarkResults(threadCount, metricsCollector);
        systemUsageMonitor.logSystemUsage();
    }
}
