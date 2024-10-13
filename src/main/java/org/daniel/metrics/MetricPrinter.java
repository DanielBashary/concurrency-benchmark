package org.daniel.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MetricPrinter {
    private static final Logger logger = LoggerFactory.getLogger(MetricPrinter.class);

    public static void printBenchmarkResults(int threadCount, MetricsCollector metricsCollector) {
        long writeOps = metricsCollector.getWriteOperations();
        long readOps = metricsCollector.getReadOperations();
        long writeErrors = metricsCollector.getWriteErrors();
        long readErrors = metricsCollector.getReadErrors();

        logger.info("=== Benchmark Results for Thread Count: {} ===", threadCount);
        logger.info("Total Write Operations: {} (Errors: {})", writeOps, writeErrors);
        logger.info("Total Read Operations: {} (Errors: {})", readOps, readErrors);

        if (writeOps > 0) {
            logger.info("Average Write Latency (ns): {}", metricsCollector.getTotalWriteLatency() / writeOps);
        }
        if (readOps > 0) {
            logger.info("Average Read Latency (ns): {}", metricsCollector.getTotalReadLatency() / readOps);
        }
    }


    public static void printCouchbaseMetrics(int threadCount, Map<String, Double> metrics) {
        logger.info("=== Couchbase Metrics for Thread Count: {} ===", threadCount);
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            logger.info("{}: {}", entry.getKey(), entry.getValue());
        }
    }
}
