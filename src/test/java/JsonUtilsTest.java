import org.daniel.util.JsonUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    @Test
    void testLoadJsonData() throws IOException {
        String testFilePath = "src/test/resources/test.json";
        Map<String, Object> jsonData = JsonUtils.loadJsonData(testFilePath);
        assertNotNull(jsonData);
        assertEquals("value", jsonData.get("key"));
    }

    @Test
    void testLoadJsonFilePaths() throws IOException {
        String testDirectory = "src/test/resources";
        List<Path> jsonFilePaths = JsonUtils.loadJsonFilePaths(testDirectory);
        assertNotNull(jsonFilePaths);
        assertFalse(jsonFilePaths.isEmpty());
        assertTrue(jsonFilePaths.stream().allMatch(path -> path.toString().endsWith(".json")));
    }
}
