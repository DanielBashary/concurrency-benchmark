package org.daniel.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private final Properties properties;

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

    public String getProperty(String property){
        return properties.getProperty(property);
    }

    public Properties getProperties(){
        return properties;
    }}
