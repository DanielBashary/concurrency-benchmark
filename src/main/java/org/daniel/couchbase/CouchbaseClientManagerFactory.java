package org.daniel.couchbase;

import java.util.Properties;

/**
 * Factory class for creating CouchbaseClientManager instances.
 */
public class CouchbaseClientManagerFactory {
    private CouchbaseClientManagerFactory() {
    }

    // Creates a CouchbaseClientManager using the provided properties.
    public static CouchbaseClientManager create(Properties properties) {
        return new CouchbaseClientManager(
                properties.getProperty("couchbase.host"),
                properties.getProperty("couchbase.username"),
                properties.getProperty("couchbase.password"),
                properties.getProperty("couchbase.bucket")
        );
    }
}
