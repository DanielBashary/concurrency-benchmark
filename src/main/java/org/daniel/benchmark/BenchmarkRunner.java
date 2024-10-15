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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for orchestrating the benchmark runs.
 * Manages multiple runs, collects metrics, and prints results.
 */
public class BenchmarkRunner {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkRunner.class);

    private final CouchbaseClientManager couchbaseClientManager;
    private final CouchbaseMetricsRetriever metricsRetriever;
    private final List<Integer> threadCounts;
    private final boolean useVirtualThreads;
    private final int processDurationSeconds;
    private final int runsPerThreadCount;
    private final int sleepBetweenRunsSeconds;
    private final List<Path> jsonFilePaths;

    /**
     * Constructs a BenchmarkRunner with the specified configuration and Couchbase client manager.
     *
     * @param config                 Application configuration.
     * @param couchbaseClientManager Manages Couchbase connections.
     * @throws IOException If JSON files cannot be loaded.
     */
    public BenchmarkRunner(AppConfig config, CouchbaseClientManager couchbaseClientManager) throws IOException {
        this.couchbaseClientManager = couchbaseClientManager;
        this.threadCounts = config.getThreadCounts();
        this.processDurationSeconds = config.getProcessSeconds();
        this.runsPerThreadCount = config.getThreadPoolRuns();
        this.useVirtualThreads = config.isUseVirtualThreads();
        this.sleepBetweenRunsSeconds = config.getSleepBetweenRunsSeconds();
        this.metricsRetriever = new CouchbaseMetricsRetriever(
                couchbaseClientManager.getCluster(),
                couchbaseClientManager.getCollection().bucketName()
        );
        this.jsonFilePaths = JsonUtils.loadJsonFilePaths("json-files");
    }

    /**
     * Executes the benchmark runs based on the configuration.
     */
    public void runBenchmarks() {
        for (int currentThreadCount : threadCounts) {
            logger.info("Starting benchmark with {} threads.", currentThreadCount);
            List<MetricsCollector> runMetricsCollectors = new ArrayList<>();

            // Loop to execute multiple benchmark runs
            for (int run = 1; run <= runsPerThreadCount; run++) {
                logger.info("Starting run {}/{}", run, runsPerThreadCount);
                try {
                    runSingleBenchmark(runMetricsCollectors, currentThreadCount);
                } catch (Exception e) {
                    logger.error("Error during benchmark run {}/{} with {} threads: {}", run, runsPerThreadCount, currentThreadCount, e.getMessage(), e);
                }

                if (run < runsPerThreadCount) {
                    sleepBetweenRuns();
                }
            }

            // Print the results after all runs are completed
            printBenchmarkResults(runMetricsCollectors, currentThreadCount);
        }
    }

    /**
     * Executes a single benchmark run and collects the metrics.
     *
     * @param runMetricsCollectors List to collect metrics from each run.
     * @param threadCountCurrent   Current thread count for the benchmark.
     */
    private void runSingleBenchmark(List<MetricsCollector> runMetricsCollectors, int threadCountCurrent) throws IOException {
        MetricsCollector metricsCollector = new MetricsCollector();
        AtomicInteger documentIdCounter = new AtomicInteger();

        CouchbaseBenchmarkExecutor benchmarkExecutor = new CouchbaseBenchmarkExecutor(
                couchbaseClientManager,
                jsonFilePaths,
                metricsCollector,
                documentIdCounter
        );

        // Run the benchmark with the specified thread count and duration
        benchmarkExecutor.runBenchmarkWithThreadCount(threadCountCurrent, processDurationSeconds, useVirtualThreads);

        // Collect metrics from this run
        runMetricsCollectors.add(metricsCollector);
    }

    /**
     * Sleeps for a specified duration between runs to allow the system to cool down.
     */
    private void sleepBetweenRuns() {
        logger.info("Sleeping for {} seconds before the next run...", sleepBetweenRunsSeconds);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepBetweenRunsSeconds));
        } catch (InterruptedException e) {
            logger.error("Sleep interrupted: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Prints the benchmark results for each run and averages across all runs.
     *
     * @param runMetricsCollectors List of metrics collected from each run.
     * @param threadCountCurrent   Current thread count for the benchmark.
     */
    private void printBenchmarkResults(List<MetricsCollector> runMetricsCollectors, int threadCountCurrent) {
        // Print benchmark results for each run
        for (int run = 1; run <= runsPerThreadCount; run++) {
            logger.info("=== Results for Run {}/{} ===", run, runsPerThreadCount);
            MetricsCollector metricsCollector = runMetricsCollectors.get(run - 1);
            MetricPrinter.printBenchmarkResults(threadCountCurrent, metricsCollector);
        }

        // Print the average metrics over all runs
        MetricPrinter.printAverageBenchmarkResults(threadCountCurrent, runMetricsCollectors);

        // Retrieve and print Couchbase metrics
        Map<String, Double> couchbaseMetrics = metricsRetriever.retrieveMetrics();
        MetricPrinter.printCouchbaseMetrics(threadCountCurrent, couchbaseMetrics);
    }
}
