package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try (CouchbaseClientManager clusterManager = new CouchbaseClientManager("127.0.0.1", "Administrator", "password", "json-store")) {
            // Load all JSON file paths from the specified directory
            List<Path> jsonFilePaths = Files.walk(Paths.get("/Users/danielbashary/dev/fun/couchbase-benchmark/json-files"))
                    .filter(Files::isRegularFile).toList();

            CouchbaseBenchmarkExecutor couchbaseBenchmarkExecutor = new CouchbaseBenchmarkExecutor(clusterManager, jsonFilePaths);

            // Test with various thread counts
            int[] threadCounts = {10};
            for (int threadCount : threadCounts) {
                logger.info("Starting benchmark with {} threads.", threadCount);
                couchbaseBenchmarkExecutor.runBenchmarkWithThreadCount(threadCount, 10);
            }
        } catch (IOException e) {
            logger.error("Error loading JSON file paths: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("Error during benchmark execution: {}", e.getMessage());
        }
    }
}
