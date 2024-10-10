package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResultPrinter {
    private static final Logger logger = LoggerFactory.getLogger(ResultPrinter.class);

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
}
