package org.example;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws IOException {
        Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
        Bucket bucket = cluster.bucket("json-store");
        Collection collection = bucket.defaultCollection();
        Map<String, Object> userData = retrieveJsonFile();

        for (int threadCount = 5; threadCount < 100; threadCount = threadCount + 10) {
            clearBucket(cluster);
            System.out.println("bucket cleared");
            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
            long benchmarkDuration = TimeUnit.SECONDS.toMillis(10);
            long startTime = System.currentTimeMillis();
            AtomicInteger documentRead = new AtomicInteger();
            AtomicInteger documentAdd = new AtomicInteger();
            AtomicInteger documentIdOld = new AtomicInteger();
            while (System.currentTimeMillis() - startTime < benchmarkDuration) {
                executorService.submit(() -> {
                    try {
                        Integer documentId = documentIdOld.getAndIncrement();
                        collection.upsert(String.valueOf(documentId), JsonObject.create().put("data", userData));
                        documentRead.getAndIncrement();
                        for (int i = 0; i < 3; i++) {
                            collection.get(String.valueOf(documentId));
                            documentAdd.getAndIncrement();
                        }
                    } catch (Exception e) {
//                        e.printStackTrace();
                    }
                });
            }
            executorService.shutdownNow();
            System.out.println("Current Thread Count: " + threadCount + "Current Document Read: " + documentRead);
            System.out.println("Current Thread Count: " + threadCount + "Current Document Add: " + documentAdd);
        }
        cluster.disconnect();
    }

    public static void clearBucket(Cluster cluster){
        String deleteQuery = "DELETE FROM `json-store`";
        cluster.query(deleteQuery, QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
    }

    public static Map<String, Object> retrieveJsonFile() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        File fileObj = new File("/Users/danielbashary/dev/fun/couchbase-benchmark/src/main/resources/sample2.json"); //todo change to file?
        return mapper.readValue(fileObj, new TypeReference<Map<String, Object>>() {});
    }
}
