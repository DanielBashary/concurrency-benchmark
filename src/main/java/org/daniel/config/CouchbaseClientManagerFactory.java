package org.daniel.config;

import org.daniel.couchbase.CouchbaseClientManager;

import java.util.Properties;

public class CouchbaseClientManagerFactory {
    public static CouchbaseClientManager create(Properties properties) {
        return new CouchbaseClientManager(
                properties.getProperty("couchbase.host"),
                properties.getProperty("couchbase.username"),
                properties.getProperty("couchbase.password"),
                properties.getProperty("couchbase.bucket")
        );
    }
}
