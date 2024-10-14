package org.daniel.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;

/**
 * Manages the Couchbase cluster connection and provides access to the collection.
 */
public class CouchbaseClientManager implements AutoCloseable {
    private final Cluster cluster;
    private final Collection collection;

    /**
     * Connects to the Couchbase cluster and initializes the collection.
     *
     * @param host       Couchbase host address.
     * @param username   Username for authentication.
     * @param password   Password for authentication.
     * @param bucketName Name of the bucket to use.
     */
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
