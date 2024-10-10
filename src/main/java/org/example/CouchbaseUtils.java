package org.example;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class CouchbaseUtils {
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseUtils.class);

    private CouchbaseUtils() {}

    public static void clearBucket(Cluster cluster, String bucketName) {
        String deleteQuery = "DELETE FROM `" + bucketName + "`";
        try {
            cluster.query(deleteQuery, QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
            logger.info("Bucket cleared successfully.");
        } catch (Exception e) {
            logger.error(String.format("Error clearing bucket: %s", e.getMessage()));
        }
    }

    public static Map<String, Object> loadJsonData(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(filePath), new TypeReference<Map<String, Object>>() {});
    }
}
