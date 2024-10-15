package org.daniel.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Handles loading and providing access to application configuration properties.
 */
public class AppConfig {
    private final Properties properties;

    public AppConfig(String propertiesFileName) throws IOException {
        properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertiesFileName)) {
            if (inputStream == null) {
                throw new IOException("Properties file '" + propertiesFileName + "' not found in classpath.");
            }
            properties.load(inputStream);
        }
    }

    public Properties getProperties() {
        return properties;
    }

    // Benchmark configuration getters with validation
    public List<Integer> getThreadCounts() {
        String threadCountsStr = properties.getProperty("thread-count");
        return Arrays.stream(threadCountsStr.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .toList();
    }

    public int getThreadPoolRuns() {
        return Integer.parseInt(properties.getProperty("thread-pool-runs"));
    }

    public int getProcessSeconds() {
        return Integer.parseInt(properties.getProperty("process-seconds"));
    }

    public int getSleepBetweenRunsSeconds() {
        return Integer.parseInt(properties.getProperty("sleep-between-runs"));
    }

    public boolean isUseVirtualThreads() {
        return Boolean.parseBoolean(properties.getProperty("virtual-threads"));
    }
}
