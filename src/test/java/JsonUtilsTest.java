import org.daniel.util.JsonUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    @Test
    void testLoadJsonData() throws IOException {
        Map<String, Object> jsonData = JsonUtils.loadJsonData("src/test/resources/test.json");
        assertEquals("value", jsonData.get("key"));
    }
}