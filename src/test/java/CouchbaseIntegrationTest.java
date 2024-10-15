import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import org.daniel.config.AppConfig;
import org.daniel.couchbase.CouchbaseClientManager;
import org.daniel.couchbase.CouchbaseClientManagerFactory;
import org.junit.jupiter.api.*;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CouchbaseIntegrationTest {

    private CouchbaseClientManager couchbaseClientManager;

    @BeforeAll
    void setUp() throws IOException {
        AppConfig appConfig = new AppConfig("application.properties");
        couchbaseClientManager = CouchbaseClientManagerFactory.create(appConfig.getProperties());
    }

    @AfterAll
    void tearDown() {
        couchbaseClientManager.close();
    }

    @Test
    void testCouchbaseConnection() {
        Cluster cluster = couchbaseClientManager.getCluster();
        assertNotNull(cluster);

        Bucket bucket = cluster.bucket(couchbaseClientManager.getCollection().bucketName());
        assertNotNull(bucket);
    }

    @Test
    void testCouchbaseOperations() {
        String docId = "test-doc";
        String content = "{\"name\":\"Test Document\"}";

        // Upsert a document
        couchbaseClientManager.getCollection().upsert(docId, content);

        // Retrieve the document
        String retrievedContent = couchbaseClientManager.getCollection().get(docId).contentAs(String.class);
        assertEquals(content, retrievedContent);
    }
}
