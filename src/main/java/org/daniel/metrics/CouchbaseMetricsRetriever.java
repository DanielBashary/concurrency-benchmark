package org.daniel.metrics;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.http.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Retrieves metrics from the Couchbase cluster and bucket.
 */
public class CouchbaseMetricsRetriever {
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseMetricsRetriever.class);

    private static final String CLUSTER_METRICS_URL = "/pools/default";
    private static final String BUCKET_STATS_URL_TEMPLATE = "/pools/default/buckets/%s/stats";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Cluster cluster;
    private final String bucketName;

    public CouchbaseMetricsRetriever(Cluster cluster, String bucketName) {
        this.cluster = cluster;
        this.bucketName = bucketName;
    }

    /**
     * Retrieves various metrics from the Couchbase cluster and bucket.
     *
     * @return A map containing metric names and their values.
     * @throws CouchbaseException If an error occurs while retrieving metrics.
     */
    public Map<String, Double> retrieveMetrics() {
        Map<String, Double> metricsMap = new HashMap<>();
        try {
            metricsMap.putAll(retrieveClusterMetrics());
            metricsMap.putAll(retrieveBucketMetrics());
        } catch (IOException e) {
            logger.error("Failed to parse Couchbase metrics JSON: {}", e.getMessage(), e);
        }
        return metricsMap;
    }

    /**
     * Retrieves cluster-level metrics such as CPU and memory usage.
     *
     * @return A map containing cluster metric names and their values.
     * @throws IOException        If an error occurs while parsing JSON.
     * @throws CouchbaseException If an error occurs while making the HTTP request.
     */
    private Map<String, Double> retrieveClusterMetrics() throws IOException {
        Map<String, Double> clusterMetricsMap = new HashMap<>();

        HttpResponse response = cluster.httpClient().get(HttpTarget.manager(), HttpPath.of(CLUSTER_METRICS_URL), HttpGetOptions.httpGetOptions().header("Content-Type", "application/json"));

        String content = response.contentAsString();
        JsonNode rootNode = OBJECT_MAPPER.readTree(content);

        JsonNode nodes = rootNode.path("nodes");
        if (nodes.isArray() && nodes.size() > 0) {
            JsonNode systemStats = nodes.get(0).path("systemStats");
            double cpuUsage = systemStats.path("cpu_utilization_rate").asDouble(-1);
            double memTotal = systemStats.path("mem_total").asDouble(-1);
            double memFree = systemStats.path("mem_free").asDouble(-1);

            if (cpuUsage >= 0) {
                clusterMetricsMap.put("Cluster CPU Usage (%)", cpuUsage);
            } else {
                logger.warn("CPU usage data is missing or invalid.");
            }

            if (memTotal >= 0 && memFree >= 0) {
                double memUsedMb = (memTotal - memFree) / (1024 * 1024);
                clusterMetricsMap.put("Cluster Memory Used (MB)", memUsedMb);
            } else {
                logger.warn("Memory usage data is missing or invalid.");
            }
        } else {
            logger.warn("No nodes found in cluster metrics.");
        }

        return clusterMetricsMap;
    }

    /**
     * Retrieves bucket-level metrics such as operations per second.
     *
     * @return A map containing bucket metric names and their values.
     * @throws IOException        If an error occurs while parsing JSON.
     * @throws CouchbaseException If an error occurs while making the HTTP request.
     */
    private Map<String, Double> retrieveBucketMetrics() throws IOException {
        Map<String, Double> bucketMetricsMap = new HashMap<>();

        String bucketStatsUrl = String.format(BUCKET_STATS_URL_TEMPLATE, bucketName);

        HttpResponse response = cluster.httpClient().get(HttpTarget.manager(), HttpPath.of(bucketStatsUrl), HttpGetOptions.httpGetOptions().header("Content-Type", "application/json"));

        String content = response.contentAsString();
        JsonNode rootNode = OBJECT_MAPPER.readTree(content);

        JsonNode opSamples = rootNode.at("/op/samples");
        double opsPerSec = getLastSampleValue(opSamples.path("ops"));

        if (opsPerSec >= 0) {
            bucketMetricsMap.put("Bucket Operations per Second", opsPerSec);
        } else {
            logger.warn("Operations per second data is missing or invalid.");
        }

        return bucketMetricsMap;
    }

    /**
     * Retrieves the last sample value from an array of samples.
     *
     * @param samples JsonNode representing an array of sample values.
     * @return The last sample value, or -1 if not available.
     */
    private double getLastSampleValue(JsonNode samples) {
        if (samples.isArray() && samples.size() > 0) {
            return samples.get(samples.size() - 1).asDouble(-1);
        }
        return -1;
    }
}
