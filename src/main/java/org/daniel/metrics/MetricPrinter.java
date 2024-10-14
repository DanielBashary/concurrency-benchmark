package org.daniel.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MetricPrinter {
    private static final Logger logger = LoggerFactory.getLogger(MetricPrinter.class);

    private MetricPrinter() {
    }

    public static void printBenchmarkResults(int threadCount, MetricsCollector metricsCollector) {
        long writeOps = metricsCollector.getWriteOperations();
        long readOps = metricsCollector.getReadOperations();
        long writeErrors = metricsCollector.getWriteErrors();
        long readErrors = metricsCollector.getReadErrors();

        logger.info("Thread Count: {}", threadCount);
        logger.info("Total Write Operations: {} (Errors: {})", writeOps, writeErrors);
        logger.info("Total Read Operations: {} (Errors: {})", readOps, readErrors);

        if (writeOps > 0) {
            logger.info("Average Write Latency (ns): {}", metricsCollector.getAverageWriteLatency());
        }
        if (readOps > 0) {
            logger.info("Average Read Latency (ns): {}", metricsCollector.getAverageReadLatency());
        }
    }

    public static void printAverageBenchmarkResults(int threadCount, List<MetricsCollector> metricsCollectors) {
        int runs = metricsCollectors.size();
        long totalWriteOps = 0;
        long totalReadOps = 0;
        long totalWriteErrors = 0;
        long totalReadErrors = 0;
        long totalWriteLatency = 0;
        long totalReadLatency = 0;

        for (MetricsCollector mc : metricsCollectors) {
            totalWriteOps += mc.getWriteOperations();
            totalReadOps += mc.getReadOperations();
            totalWriteErrors += mc.getWriteErrors();
            totalReadErrors += mc.getReadErrors();
            totalWriteLatency += mc.getTotalWriteLatency();
            totalReadLatency += mc.getTotalReadLatency();
        }

        long avgWriteOps = totalWriteOps / runs;
        long avgReadOps = totalReadOps / runs;
        long avgWriteErrors = totalWriteErrors / runs;
        long avgReadErrors = totalReadErrors / runs;
        long avgWriteLatency = avgWriteOps > 0 ? totalWriteLatency / totalWriteOps : 0;
        long avgReadLatency = avgReadOps > 0 ? totalReadLatency / totalReadOps : 0;

        logger.info("=== Average Benchmark Results over {} Runs ===", runs);
        logger.info("Thread Count: {}", threadCount);
        logger.info("Average Total Write Operations: {} (Errors: {})", avgWriteOps, avgWriteErrors);
        logger.info("Average Total Read Operations: {} (Errors: {})", avgReadOps, avgReadErrors);
        logger.info("Average Write Latency (ns): {}", avgWriteLatency);
        logger.info("Average Read Latency (ns): {}", avgReadLatency);
    }

    public static void printCouchbaseMetrics(int threadCount, Map<String, Double> metrics) {
        logger.info("=== Couchbase Metrics for Thread Count: {} ===", threadCount);
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            logger.info("{}: {}", entry.getKey(), entry.getValue());
        }
    }
}
