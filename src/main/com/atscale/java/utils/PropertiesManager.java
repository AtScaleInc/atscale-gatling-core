package com.atscale.java.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.List;
import java.util.Arrays;
import java.nio.file.Paths;

@SuppressWarnings("unused")
public class PropertiesManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesManager.class);
    private final Properties properties = new Properties();
    private final String propertiesFileName;
    private final String differentiator;
    private static final PropertiesManager instance = new PropertiesManager();

    private PropertiesManager(){
        if(System.getProperty("systems.properties.file") != null){
            propertiesFileName = System.getProperty("systems.properties.file");
            LOGGER.info("Loading properties file from system property: {}", propertiesFileName);
            differentiator = "-altprops";
        } else {
            propertiesFileName = "systems.properties";
            differentiator = "";
        }

        // If propertiesFileName looks like an explicit filesystem path, try it first
        Path explicitPath = null;
        try {
            Path candidateExplicit = Paths.get(propertiesFileName);
            if (candidateExplicit.isAbsolute() || propertiesFileName.contains("/") || propertiesFileName.contains("\\")) {
                explicitPath = candidateExplicit;
            }
        } catch (Exception e) {
            // ignore - treat as non-path name
        }

        if (explicitPath != null) {
            if (Files.exists(explicitPath) && Files.isRegularFile(explicitPath)) {
                LOGGER.info("Loading properties file from explicit path: {}", explicitPath);
                try (InputStream input = Files.newInputStream(explicitPath)) {
                    properties.load(input);
                    return;
                } catch (IOException e){
                    LOGGER.error("Error loading properties file: {}", e.getMessage());
                    return;
                }
            } else {
                LOGGER.warn("Explicit properties path provided but file not found: {}", explicitPath);
                // fall through to other lookup methods
            }
        }

        // Prefer on-disk target/classes (main resources) before using classpath to avoid picking up test resources
        String userDir = System.getProperty("user.dir");
        if (userDir != null) {
            Path tcCandidate = Paths.get(userDir, "target", "classes", propertiesFileName);
            try {
                if (Files.exists(tcCandidate) && Files.isRegularFile(tcCandidate)) {
                    LOGGER.info("Loading properties file from target/classes (prefer main resources): {}", tcCandidate.toAbsolutePath());
                    try (InputStream input = Files.newInputStream(tcCandidate)) {
                        properties.load(input);
                        return;
                    } catch (IOException e) {
                        LOGGER.error("Error loading properties file from target/classes: {}", e.getMessage());
                        // fall through to classpath / other lookups
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Error checking target/classes candidate {}: {}", tcCandidate, e.getMessage());
            }
        }

        // Try loading from classpath (works whether resource is in classes or inside jar)
        InputStream classpathStream = getClass().getClassLoader().getResourceAsStream(propertiesFileName);
        if (classpathStream != null) {
            LOGGER.info("Loading properties file from classpath resource: {}", propertiesFileName);
            try (InputStream input = classpathStream) {
                properties.load(input);
                return;
            } catch (IOException e) {
                LOGGER.error("Error loading properties file from classpath: {}", e.getMessage());
                return;
            }
        }

        // Developer-friendly fallbacks: src/main/resources, project root, and current directory
        if (userDir != null) {
            Path srcMain = Paths.get(userDir, "src", "main", "resources", propertiesFileName);
            try {
                if (Files.exists(srcMain) && Files.isRegularFile(srcMain)) {
                    LOGGER.info("Loading properties file from src/main/resources: {}", srcMain.toAbsolutePath());
                    try (InputStream input = Files.newInputStream(srcMain)) {
                        properties.load(input);
                        return;
                    } catch (IOException e) {
                        LOGGER.error("Error loading properties file from src/main/resources: {}", e.getMessage());
                        return;
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Error checking src/main/resources candidate {}: {}", srcMain, e.getMessage());
            }

            Path projectRoot = Paths.get(userDir, propertiesFileName);
            try {
                if (Files.exists(projectRoot) && Files.isRegularFile(projectRoot)) {
                    LOGGER.info("Loading properties file from project root: {}", projectRoot.toAbsolutePath());
                    try (InputStream input = Files.newInputStream(projectRoot)) {
                        properties.load(input);
                        return;
                    } catch (IOException e) {
                        LOGGER.error("Error loading properties file from project root: {}", e.getMessage());
                        return;
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Error checking project root candidate {}: {}", projectRoot, e.getMessage());
            }
        }

        // current directory
        Path currentDir = Paths.get("./", propertiesFileName);
        try {
            if (Files.exists(currentDir) && Files.isRegularFile(currentDir)) {
                LOGGER.info("Loading properties file from current directory: {}", currentDir.toAbsolutePath());
                try (InputStream input = Files.newInputStream(currentDir)) {
                    properties.load(input);
                    return;
                } catch (IOException e) {
                    LOGGER.error("Error loading properties file from current directory: {}", e.getMessage());
                    return;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Error checking current directory candidate {}: {}", currentDir, e.getMessage());
        }

        // If we've reached here the file wasn't found anywhere
        LOGGER.error("Properties file not found: {} (looked on classpath and common filesystem locations)", propertiesFileName);
    }

    public static String getDifferentiator() {
        return instance.differentiator;
    }

    public static List<String> getAtScaleModels(){
        String property = instance.properties.getProperty("atscale.models");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.models is not set in properties file: " + instance.propertiesFileName);
        }
        String[] models = property.split("\\s*,\\s*"); // Splits on commas, trims spaces
        LOGGER.info("AtScale models loaded: {}", Arrays.toString(models));
        return Arrays.asList(models);
    }

    public static Long getAtScaleThrottleMs() {
        return Long.parseLong(getProperty("atscale.gatling.throttle.ms", "5"));
    }

    public static Integer getAtScaleXmlaMaxConnectionsPerHost() {
        return Integer.parseInt(getProperty("atscale.xmla.maxConnectionsPerHost", "20"));
    }

    public static String getJdbcUseAggregates() {
        String prop =  getProperty("atscale.jdbc.useAggregates", "true");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.jdbc.useAggregates: {} expected true or false", prop);
            return String.valueOf(true);
        }
    }

    public static String getJdbcGenerateAggregates() {
        String prop =  getProperty("atscale.jdbc.generateAggregates", "false");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.jdbc.generateAggregates: {} expected true or false", prop);
            return String.valueOf(false);
        }
    }

    public static String getJdbcUseLocalCache() {
        String prop =  getProperty("atscale.jdbc.useLocalCache", "false");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.jdbc.useLocalCache: {} expected true or false", prop);
            return String.valueOf(false);
        }
    }

    public static String getXmlaUseAggregates() {
        String prop =  getProperty("atscale.xmla.useAggregates", "true");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.xmla.useAggregates: {} expected true or false", prop);
            return String.valueOf(true);
        }
    }

    public static String getXmlaGenerateAggregates() {
        String prop =  getProperty("atscale.xmla.generateAggregates", "false");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.xmla.generateAggregates: {} expected true or false", prop);
            return String.valueOf(false);
        }
    }

    public static String getXmlaUseQueryCache() {
        String prop =  getProperty("atscale.xmla.useQueryCache", "false");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.xmla.useQueryCache: {} expected true or false", prop);
            return String.valueOf(false);
        }
    }

    public static String getXmlaUseAggregateCache() {
        String prop = getProperty("atscale.xmla.useAggregateCache", "true");
        if (prop.equals("true") || prop.equals("false")){
            return prop;
        } else{
            LOGGER.error("Invalid boolean value for atscale.xmla.useAggregateCache: {} expected true or false", prop);
            return String.valueOf(true);
        }
    }

    public static String getAtScalePostgresURL() {
        String property = instance.properties.getProperty("atscale.postgres.jdbc.url");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.postgres.jdbc.url is not set in properties file: " + instance.propertiesFileName);
        }
        return property;
    }

    public static String getAtScalePostgresUser() {
        String property = instance.properties.getProperty("atscale.postgres.jdbc.username");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.postgres.jdbc.username is not set in properties file: " + instance.propertiesFileName);
        }
        return property;
    }

    public static String getAtScalePostgresPassword() {
        String property = instance.properties.getProperty("atscale.postgres.jdbc.password");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.postgres.jdbc.password is not set in properties file: " + instance.propertiesFileName);
        }
        return property;
    }

    public static String getAtScaleJdbcConnection(String model) {
        String key = String.format("atscale.%s.jdbc.url", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleJdbcUserName(String model) {
        String key = String.format("atscale.%s.jdbc.username", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleJdbcPassword(String model) {
        String key = String.format("atscale.%s.jdbc.password", clean(model));
        return getProperty(key);
    }

    public static int getAtScaleJdbcMaxPoolSize(String model) {
        String key = String.format("atscale.%s.jdbc.maxPoolSize", clean(model));
        return Integer.parseInt(getProperty(key, "10"));
    }

    public static boolean getRedactRawData(String model) {
        String key = String.format("atscale.%s.redactRawdata", clean(model));
        return Boolean.parseBoolean(getProperty(key, "true"));
    }

    public static String getAtScaleXmlaConnection(String model) {
        String key = String.format("atscale.%s.xmla.url", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleXmlaCubeName(String model) {
        String key = String.format("atscale.%s.xmla.cube", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleXmlaCatalogName(String model) {
        String key = String.format("atscale.%s.xmla.catalog", clean(model));
        return getProperty(key);
    }

    public static boolean getLogSqlQueryRows(String model) {
        String key = String.format("atscale.%s.jdbc.log.resultset.rows", clean(model));
        return Boolean.parseBoolean(getProperty(key, "false"));
    }

    public static boolean getLogXmlaResponseBody(String model) {
        String key = String.format("atscale.%s.xmla.log.responsebody", clean(model));
        return Boolean.parseBoolean(getProperty(key, "false"));
    }

    public static boolean isInstallerVersion(String model) {
       return ! isContainerVersion(model);
    }

    public static boolean isContainerVersion(String model) {
       String xmlaConnection = getAtScaleXmlaConnection(model);
       return xmlaConnection.toLowerCase().contains("/engine/xmla");
    }

    public static String getAtScaleXmlaAuthConnection(String model) {
        String key = String.format("atscale.%s.xmla.auth.url", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleXmlaAuthUserName(String model) {
        String key = String.format("atscale.%s.xmla.auth.username", clean(model));
        return getProperty(key);
    }

    public static String getAtScaleXmlaAuthPassword(String model) {
        String key = String.format("atscale.%s.xmla.auth.password", clean(model));
        return getProperty(key);
    }

    public static void setCustomProperties(java.util.Map<String, String> customProperties) {
        for (String key : customProperties.keySet()) {
            LOGGER.info("Setting custom property: {} of size {}", key, customProperties.get(key).length());
            instance.properties.setProperty(key, customProperties.get(key));
        }
    }

    public static boolean hasProperty(String key) {
        String property = instance.properties.getProperty(key);
        return StringUtils.isNotEmpty(property);
    }

    public static String getCustomProperty(String propertyName) {
        return getProperty(propertyName);
    }

    private static String getProperty(String key) {
        String property = instance.properties.getProperty(key);
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("Property " + key + " is not set in properties file: " + instance.propertiesFileName);
        }
        return property;
    }

    @SuppressWarnings("all")
    private static String getProperty(String key, String defaultValue) {
        if(! instance.properties.containsKey(key)) {
            LOGGER.warn("Using default value for property {}: {}", key, defaultValue);
        }
        return instance.properties.getProperty(key, defaultValue);
    }

    private static String clean(String input) {
        String val = StringUtil.stripQuotes(input);
        return val.replace(" ", "_");
    }
}
