package org.daniel;

import org.daniel.benchmark.BenchmarkRunner;
import org.daniel.config.AppConfig;
import org.daniel.couchbase.CouchbaseClientManagerFactory;
import org.daniel.couchbase.CouchbaseClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Entry point of the application.
 * Initializes the configuration and starts the benchmark runner.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        try {
            // Load application configuration from properties file
            AppConfig config = new AppConfig("application.properties");

            // Create Couchbase client manager using the configuration
            CouchbaseClientManager clusterManager = CouchbaseClientManagerFactory.create(config.getProperties());

            // Initialize and run the benchmark
            BenchmarkRunner benchmarkRunner = new BenchmarkRunner(config, clusterManager);
            benchmarkRunner.runBenchmarks();
        } catch (IOException e) {
            logger.error("Application properties file not found on classpath.", e);
            throw new RuntimeException("application.properties file not found on classpath.", e);

        } catch (RuntimeException e) {
            logger.error("Error during benchmark execution: {}", e.getMessage(), e);
            throw new RuntimeException("Error during benchmark execution", e);
        }
    }
}
