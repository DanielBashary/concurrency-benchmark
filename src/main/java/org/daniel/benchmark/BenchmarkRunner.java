package org.daniel.benchmark;

import org.daniel.config.AppConfig;
import org.daniel.couchbase.CouchbaseClientManager;
import org.daniel.metrics.CouchbaseMetricsRetriever;
import org.daniel.metrics.MetricPrinter;
import org.daniel.metrics.MetricsCollector;
import org.daniel.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for orchestrating the benchmark runs.
 * Manages multiple runs, collects metrics, and prints results.
 */

public class BenchmarkRunner {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkRunner.class);
    private final AppConfig config;
    private final CouchbaseClientManager clusterManager;
    private final CouchbaseMetricsRetriever metricsRetriever;
    private final int threadCount;
    private final int processSeconds;
    private final int runs;

    public BenchmarkRunner(AppConfig config, CouchbaseClientManager clusterManager) {
        this.config = config;
        this.clusterManager = clusterManager;
        this.threadCount = Integer.parseInt(config.getProperty("threadCount"));
        this.processSeconds = Integer.parseInt(config.getProperty("processSeconds"));
        this.runs = Integer.parseInt(config.getProperty("threadPoolRuns"));
        this.metricsRetriever = new CouchbaseMetricsRetriever(clusterManager.getCluster(), clusterManager.getCollection().bucketName());
    }

    /**
     * Executes the benchmark runs based on the configuration.
     *
     * @throws IOException if there is an error loading JSON files.
     */

    public void runBenchmarks() throws IOException {
        logger.info("Starting benchmark with {} threads.", threadCount);
        List<MetricsCollector> runMetricsCollectors = new ArrayList<>();

        // Loop to execute multiple benchmark runs
        for (int run = 1; run <= runs; run++) {
            logger.info("Starting run {}/{}", run, runs);
            runSingleBenchmark(runMetricsCollectors);

            if (run < runs) {
                sleepBetweenRuns();
            }
        }

        // Print the results after all runs are completed
        printBenchmarkResults(runMetricsCollectors);
    }

    /**
     * Executes a single benchmark run and collects the metrics.
     *
     * @param runMetricsCollectors List to collect metrics from each run.
     * @throws IOException if there is an error loading JSON files.
     */
    private void runSingleBenchmark(List<MetricsCollector> runMetricsCollectors) throws IOException {
        MetricsCollector metricsCollector = new MetricsCollector();
        AtomicInteger documentIdCounter = new AtomicInteger();
        List<Path> jsonFilePaths = JsonUtils.loadJsonFilePaths("json-files");

        CouchbaseBenchmarkExecutor benchmarkExecutor = new CouchbaseBenchmarkExecutor(
                clusterManager,
                jsonFilePaths,
                metricsCollector,
                documentIdCounter
        );

        // Run the benchmark with the specified thread count and duration
        benchmarkExecutor.runBenchmarkWithThreadCount(threadCount, processSeconds);

        // Collect metrics from this run
        runMetricsCollectors.add(metricsCollector);
    }

    /**
     * Sleeps for a specified duration between runs to allow the system to cool down.
     */
    private void sleepBetweenRuns() {
        int sleepSeconds = Integer.parseInt(config.getProperty("sleepBetweenRuns"));
        logger.info("Sleeping for {} seconds before the next run...", sleepSeconds);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
        } catch (InterruptedException e) {
            logger.error("Sleep interrupted: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Prints the benchmark results for each run and averages across all runs.
     *
     * @param runMetricsCollectors List of metrics collected from each run.
     */
    private void printBenchmarkResults(List<MetricsCollector> runMetricsCollectors) {
        // Print benchmark results for each run
        for (int run = 1; run <= runs; run++) {
            logger.info("=== Results for Run {}/{} ===", run, runs);
            MetricsCollector metricsCollector = runMetricsCollectors.get(run - 1);
            MetricPrinter.printBenchmarkResults(threadCount, metricsCollector);
        }

        // Print the average metrics over all runs
        MetricPrinter.printAverageBenchmarkResults(threadCount, runMetricsCollectors);

        // Retrieve and print Couchbase metrics
        Map<String, Double> couchbaseMetrics = metricsRetriever.retrieveMetrics();
        MetricPrinter.printCouchbaseMetrics(threadCount, couchbaseMetrics);
    }
}
