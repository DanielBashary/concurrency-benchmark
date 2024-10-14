package org.daniel;

import org.daniel.benchmark.BenchmarkRunner;
import org.daniel.config.AppConfig;
import org.daniel.couchbase.CouchbaseClientManagerFactory;
import org.daniel.couchbase.CouchbaseClientManager;

import java.io.IOException;


public class Main {

    public static void main(String[] args) throws IOException {
        try {
            AppConfig config = new AppConfig("application.properties");
            CouchbaseClientManager clusterManager = CouchbaseClientManagerFactory.create(config.getProperties());
            BenchmarkRunner benchmarkRunner = new BenchmarkRunner(config, clusterManager);
            benchmarkRunner.runBenchmarks();
        } catch (IOException e) {
            throw new IOException("application.properties file not found on classpath.", e);

        } catch (RuntimeException e) {
            throw new RuntimeException("Error loading JSON file paths", e);
        }
    }
}
