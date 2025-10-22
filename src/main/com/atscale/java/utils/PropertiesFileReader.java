package com.atscale.java.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.List;
import java.util.Arrays;

@SuppressWarnings("unused")
public class PropertiesFileReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesFileReader.class);
    private final Properties properties = new Properties();
    private static final String PROPERTIES_FILE = "systems.properties";
    private static final PropertiesFileReader instance = new PropertiesFileReader();

    private PropertiesFileReader(){
        try{
            URL propertyFileURl = getClass().getClassLoader().getResource(PropertiesFileReader.PROPERTIES_FILE);
            if(null == propertyFileURl){
                LOGGER.error("Properties file not found: {}", PropertiesFileReader.PROPERTIES_FILE);
                return;
            }
            Path path = java.nio.file.Paths.get(propertyFileURl.toURI());
            if (Files.isRegularFile(path)){
                LOGGER.info("Loading properties file from path: {}", path.toString());
                try (InputStream input = getClass().getClassLoader().getResourceAsStream(PropertiesFileReader.PROPERTIES_FILE)) {
                    properties.load(input);
                } catch (IOException e){
                    LOGGER.error("Error loading properties file: {}", e.getMessage());
                }
            } else {
                LOGGER.error("Properties file not found at path: {}", path.toString());
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getAtScaleModels(){
        String property = instance.properties.getProperty("atscale.models");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.models is not set in properties file: " + PROPERTIES_FILE);
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
            throw new RuntimeException("atscale.postgres.jdbc.url is not set in properties file: " + PROPERTIES_FILE);
        }
        return property;
    }

    public static String getAtScalePostgresUser() {
        String property = instance.properties.getProperty("atscale.postgres.jdbc.username");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.postgres.jdbc.username is not set in properties file: " + PROPERTIES_FILE);
        }
        return property;
    }

    public static String getAtScalePostgresPassword() {
        String property = instance.properties.getProperty("atscale.postgres.jdbc.password");
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("atscale.postgres.jdbc.password is not set in properties file: " + PROPERTIES_FILE);
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

    public static String getCustomProperty(String propertyName) {
        return getProperty(propertyName);
    }

    private static String getProperty(String key) {
        String property = instance.properties.getProperty(key);
        if (property == null || property.isEmpty()) {
            throw new RuntimeException("Property " + key + " is not set in properties file: " + PROPERTIES_FILE);
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
