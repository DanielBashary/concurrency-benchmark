package org.example;

import com.couchbase.client.java.json.JsonObject;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CouchbaseBenchmarkExecutor {
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseBenchmarkExecutor.class);
    private final CouchbaseClientManager couchbaseClientManager;
    private final Map<String, Object> userData;

    // Latency metrics
    private final AtomicLong totalWriteLatency = new AtomicLong();
    private final AtomicLong totalReadLatency = new AtomicLong();

    // CPU and memory tracking
    private final OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public CouchbaseBenchmarkExecutor(CouchbaseClientManager couchbaseClientManager, Map<String, Object> userData) {
        this.couchbaseClientManager = couchbaseClientManager;
        this.userData = userData;
    }

    public void runBenchmarkWithThreadCount(int threadCount, int seconds) {
        CouchbaseUtils.clearBucket(couchbaseClientManager.getCluster(), couchbaseClientManager.getCollection().bucketName());
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        AtomicInteger documentReads = new AtomicInteger();
        AtomicInteger documentWrites = new AtomicInteger();
        AtomicInteger documentIdCounter = new AtomicInteger();
        JsonObject documentToAdd = JsonObject.create().put("data", userData);
        scheduledExecutorService.schedule(() -> {
            executorService.shutdown();
            logger.info("ShutDown");
        }, seconds, TimeUnit.SECONDS);

        while (!executorService.isShutdown()) {
            executorService.execute(() -> {
                try {
                    int documentId = documentIdCounter.getAndIncrement();

                    // Measure write latency
                    long writeStart = System.nanoTime();
                    couchbaseClientManager.getCollection().upsert(String.valueOf(documentId), documentToAdd);
                    totalWriteLatency.addAndGet(System.nanoTime() - writeStart);
                    documentWrites.incrementAndGet();

                    // Perform 3 reads for each write
                    for (int i = 0; i < 3; i++) {
                        long readStart = System.nanoTime();
                        couchbaseClientManager.getCollection().get(String.valueOf(documentId));
                        totalReadLatency.addAndGet(System.nanoTime() - readStart);
                        documentReads.incrementAndGet();
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            });
        }
        executorService.shutdownNow();
        scheduledExecutorService.shutdownNow();
        printBenchmarkResults(threadCount, documentReads.get(), documentWrites.get());
        logSystemUsage();
    }

    private void printBenchmarkResults(int threadCount, int documentReads, int documentWrites) {
        logger.info("Thread Count: {} | Document Writes: {} | Document Reads: {}", threadCount, documentWrites, documentReads);
        if (documentWrites > 0) {
            logger.info("Average Write Latency (ns): {}", totalWriteLatency.get() / documentWrites);
        }
        if (documentReads > 0) {
            logger.info("Average Read Latency (ns): {}", totalReadLatency.get() / documentReads);
        }
    }

    private void logSystemUsage() {
        logger.info("CPU Load: {}%", osBean.getCpuLoad() * 100);
        logger.info("Available Memory (MB): {}", osBean.getFreeMemorySize() / (1024 * 1024));
    }
}

