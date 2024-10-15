package org.daniel.couchbase;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the Couchbase cluster connection and provides access to the collection.
 */
public class CouchbaseClientManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseClientManager.class);

    private final Cluster cluster;
    private final Collection collection;

    /**
     * Connects to the Couchbase cluster and initializes the collection.
     *
     * @param host       Couchbase host address.
     * @param username   Username for authentication.
     * @param password   Password for authentication.
     * @param bucketName Name of the bucket to use.
     * @throws CouchbaseException If connection to Couchbase fails.
     */
    public CouchbaseClientManager(String host, String username, String password, String bucketName) {
        try {
            this.cluster = Cluster.connect(host, username, password);
            Bucket bucket = this.cluster.bucket(bucketName);
            bucket.waitUntilReady(java.time.Duration.ofSeconds(10));
            this.collection = bucket.defaultCollection();
            logger.info("Successfully connected to Couchbase bucket '{}'", bucketName);
        } catch (CouchbaseException e) {
            throw new CouchbaseException("Failed to connect to Couchbase cluster", e);
        }
    }

    /**
     * Gets the default collection from the bucket.
     *
     * @return Couchbase Collection instance.
     */
    public Collection getCollection() {
        return collection;
    }

    /**
     * Gets the Couchbase Cluster instance.
     *
     * @return Couchbase Cluster instance.
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * Closes the Couchbase cluster connection.
     */
    @Override
    public void close() {
        if (cluster != null) {
            cluster.disconnect();
            logger.info("Couchbase cluster connection closed.");
        }
    }
}
