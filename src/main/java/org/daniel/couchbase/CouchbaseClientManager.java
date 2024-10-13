package org.daniel.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;

public class CouchbaseClientManager implements AutoCloseable {
    private final Cluster cluster;
    private final Collection collection;

    public CouchbaseClientManager(String host, String username, String password, String bucketName) {
        this.cluster = Cluster.connect(host, username, password);
        Bucket bucket = this.cluster.bucket(bucketName);
        this.collection = bucket.defaultCollection();
    }

    public Collection getCollection() {
        return collection;
    }

    public Cluster getCluster() {
        return cluster;
    }

    @Override
    public void close() {
        cluster.disconnect();
    }
}
