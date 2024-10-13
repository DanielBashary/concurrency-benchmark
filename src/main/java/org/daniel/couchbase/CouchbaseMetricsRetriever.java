package org.daniel.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.http.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CouchbaseMetricsRetriever {
    private final Cluster cluster;
    private final String bucketName;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseMetricsRetriever.class);

    public CouchbaseMetricsRetriever(Cluster cluster, String bucketName) {
        this.cluster = cluster;
        this.bucketName = bucketName;
    }

    public Map<String, Double> retrieveMetrics() {
        Map<String, Double> metricsMap = new HashMap<>();
        try {
            // Retrieve cluster-level metrics
            HttpResponse clusterResult = cluster.httpClient().get(
                    HttpTarget.manager(),
                    HttpPath.of("/pools/default"),
                    HttpGetOptions.httpGetOptions().header("Content-Type", "application/json"));

            String clusterContent = clusterResult.contentAsString();
            JsonNode clusterMetrics = objectMapper.readTree(clusterContent);

            JsonNode nodeStats = clusterMetrics.at("/nodes/0/systemStats");
            double cpuUsage = nodeStats.path("cpu_utilization_rate").asDouble();
            double memTotal = nodeStats.path("mem_total").asDouble();
            double memFree = nodeStats.path("mem_free").asDouble();

            metricsMap.put("Cluster CPU Usage (%)", cpuUsage);
            metricsMap.put("Cluster Memory Used (MB)", (memTotal - memFree) / (1024 * 1024));

            HttpResponse bucketResult = cluster.httpClient().get(
                    HttpTarget.manager(),
                    HttpPath.of("/pools/default/buckets/" + bucketName + "/stats"),
                    HttpGetOptions.httpGetOptions().header("Content-Type", "application/json"));

            String bucketContent = bucketResult.contentAsString();
            JsonNode bucketMetrics = objectMapper.readTree(bucketContent);

            JsonNode opSamples = bucketMetrics.at("/op/samples");
            double opsPerSec = getLastSampleValue(opSamples.path("ops"));

            metricsMap.put("Bucket Operations per Second", opsPerSec);
        } catch (Exception e) {
            logger.error("Failed to retrieve Couchbase metrics: {}", e.getMessage());
        }
        return metricsMap;
    }

    private double getLastSampleValue(JsonNode samples) {
        if (samples.isArray() && samples.size() > 0) {
            return samples.get(samples.size() - 1).asDouble();
        }
        return 0.0;
    }
}
