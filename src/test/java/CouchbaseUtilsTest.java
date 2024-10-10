import org.example.CouchbaseUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CouchbaseUtilsTest {

    @Test
    void testLoadJsonData() throws IOException {
        Map<String, Object> jsonData = CouchbaseUtils.loadJsonData("src/test/resources/test.json");
        assertEquals("value", jsonData.get("key"));
    }
}