package org.example;

import com.couchbase.client.java.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CouchbaseBenchmarkExecutor {
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseBenchmarkExecutor.class);
    private final CouchbaseClientManager couchbaseClientManager;
    private final Map<String, Object> userData;
    private final MetricsCollector metricsCollector;
    private final SystemUsageMonitor systemUsageMonitor;

    public CouchbaseBenchmarkExecutor(CouchbaseClientManager couchbaseClientManager, Map<String, Object> userData) {
        this.couchbaseClientManager = couchbaseClientManager;
        this.userData = userData;
        this.metricsCollector = new MetricsCollector();
        this.systemUsageMonitor = new SystemUsageMonitor();
    }

    public void runBenchmarkWithThreadCount(int threadCount, int durationSeconds) {
        CouchbaseUtils.clearBucket(couchbaseClientManager.getCluster(), couchbaseClientManager.getCollection().bucketName());

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        AtomicInteger documentIdCounter = new AtomicInteger();
        AtomicBoolean isRunning = new AtomicBoolean(true);
        JsonObject documentToAdd = JsonObject.create().put("data", userData);

        scheduler.schedule(() -> {
            isRunning.set(false);
            executorService.shutdownNow();
            logger.info("Benchmark duration elapsed. Shutting down executor service.");
        }, durationSeconds, TimeUnit.SECONDS);

        while (isRunning.get()) {
            try {
                executorService.submit(new BenchmarkTask(
                        couchbaseClientManager.getCollection(),
                        documentIdCounter,
                        documentToAdd,
                        metricsCollector
                ));
            } catch (RejectedExecutionException e) {
                logger.warn("Task submission rejected after executor shutdown.");
                break;
            }
        }

        scheduler.shutdownNow();
        // Print results and system usage
        ResultPrinter.printBenchmarkResults(threadCount, metricsCollector);
        systemUsageMonitor.logSystemUsage();
    }
}
