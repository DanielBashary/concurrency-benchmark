package org.daniel;

import java.io.IOException;

import org.daniel.benchmark.CouchbaseBenchmarkExecutor;
import org.daniel.config.AppConfig;
import org.daniel.config.CouchbaseClientManagerFactory;
import org.daniel.couchbase.CouchbaseClientManager;
import org.daniel.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        AppConfig config = new AppConfig("application.properties");
        try (CouchbaseClientManager clusterManager = CouchbaseClientManagerFactory.create(config.getProperties())) {

            CouchbaseBenchmarkExecutor couchbaseBenchmarkExecutor = new CouchbaseBenchmarkExecutor(clusterManager, JsonUtils.loadJsonFilePaths("json-files"));

            int threadCount = Integer.parseInt(config.getProperty("threadCount"));

            logger.info("Starting benchmark with {} threads.", threadCount);
            clusterManager.getCluster().buckets().flushBucket("json-store");

            couchbaseBenchmarkExecutor.runBenchmarkWithThreadCount(threadCount, Integer.parseInt(config.getProperty("processSeconds")));

        } catch (IOException e) {
            logger.error("Error loading JSON file paths: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("Error during benchmark execution: {}", e.getMessage());
        }
    }
}
