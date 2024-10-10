package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        try (CouchbaseClientManager clusterManager = new CouchbaseClientManager("127.0.0.1", "Administrator", "password", "json-store")) {
            Map<String, Object> jsonData = CouchbaseUtils.loadJsonData("/Users/danielbashary/dev/fun/couchbase-benchmark/src/main/resources/sample2.json");
            CouchbaseBenchmarkExecutor couchbaseBenchmarkExecutor = new CouchbaseBenchmarkExecutor(clusterManager, jsonData);
            couchbaseBenchmarkExecutor.runBenchmarkWithThreadCount(30, 60);
        } catch (IOException e) {
            logger.error(String.format("Error loading JSON data: %s", e.getMessage()));
        } catch (Exception e) {
            logger.error(String.format("Error during benchmark execution: %s", e.getMessage()));
        }
    }
}