package org.daniel.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Loads application configuration from a properties file.
 */
public class AppConfig {
    private final Properties properties;

    /**
     * Constructs the AppConfig by loading properties from the specified file.
     *
     * @param propertiesFileName Name of the properties file.
     * @throws IOException if the properties file is not found or cannot be loaded.
     */
    public AppConfig(String propertiesFileName) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(propertiesFileName)) {
            if (inputStream == null) {
                throw new FileNotFoundException("Property file '" + propertiesFileName + "' not found in the classpath");
            }
            properties = new Properties();
            properties.load(inputStream);
        }
    }

    /**
     * Retrieves the value of a property.
     *
     * @param property Name of the property.
     * @return Value of the property.
     */
    public String getProperty(String property) {
        return properties.getProperty(property);
    }

    /**
     * Retrieves all properties.
     *
     * @return Properties object containing all properties.
     */
    public Properties getProperties() {
        return properties;
    }

    // Convert threadCount from a comma-separated string into a list of integers
    public List<Integer> getThreadCounts() {
        String threadCountProperty = getProperty("threadCount");
        return Arrays.stream(threadCountProperty.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .toList();
    }

}
