package org.daniel.benchmark;

import com.couchbase.client.java.json.JsonObject;
import org.daniel.couchbase.CouchbaseClientManager;
import org.daniel.couchbase.CouchbaseMetricsRetriever;
import org.daniel.metrics.MetricsCollector;
import org.daniel.metrics.MetricPrinter;
import org.daniel.metrics.SystemUsageMonitor;
import org.daniel.util.JsonUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class CouchbaseBenchmarkExecutor {
    private final CouchbaseClientManager couchbaseClientManager;
    private final List<Path> jsonFilePaths;
    private final SystemUsageMonitor systemUsageMonitor;
    private final String bucketName;
    private final CouchbaseMetricsRetriever metricsRetriever;

    public CouchbaseBenchmarkExecutor(CouchbaseClientManager couchbaseClientManager, List<Path> jsonFilePaths) {
        this.couchbaseClientManager = couchbaseClientManager;
        this.jsonFilePaths = jsonFilePaths;
        this.systemUsageMonitor = new SystemUsageMonitor();
        this.bucketName = couchbaseClientManager.getCollection().bucketName();
        this.metricsRetriever = new CouchbaseMetricsRetriever(couchbaseClientManager.getCluster(), bucketName);
    }

    public void runBenchmarkWithThreadCount(int threadCount, int durationSeconds) throws IOException {
        couchbaseClientManager.getCluster().buckets().flushBucket(bucketName);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        AtomicBoolean isRunning = new AtomicBoolean(true);
        MetricsCollector metricsCollector = new MetricsCollector();
        AtomicInteger documentIdCounter = new AtomicInteger();

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

        try{
            Thread.sleep(TimeUnit.SECONDS.toMillis(durationSeconds));
            isRunning.set(false);
            executorService.shutdown();
        }catch (InterruptedException e){
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        MetricPrinter.printBenchmarkResults(threadCount, metricsCollector);

        systemUsageMonitor.logSystemUsage();

        Map<String, Double> couchbaseMetrics = metricsRetriever.retrieveMetrics();
        MetricPrinter.printCouchbaseMetrics(threadCount, couchbaseMetrics);
    }
}
