package com.atscale.java.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class JsonUtil {

    public static Map<String, String> asMap(String secretsJson) {
        if(StringUtils.isEmpty(secretsJson) || "null".equalsIgnoreCase(secretsJson)) {
            return java.util.Collections.emptyMap();
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> secretsMap = new HashMap<>();

        try {
            secretsMap = objectMapper.readValue(secretsJson, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Error parsing secrets JSON: " + e.getMessage(), e);
        }
        return flatten(secretsMap);
    }

    private static Map<String, String> flatten(Map<String, String> secretsMap ) {
        try {
            Map<String, String> flattenedMap = new HashMap<>();
            for (String key : secretsMap.keySet()) {
                String value = secretsMap.get(key);
                if (StringUtils.isNotEmpty(value) && value.trim().startsWith("{") && value.trim().endsWith("}")) {
                    JsonNode node = new ObjectMapper().readTree(value);
                    flattenedMap.putAll(asSimpleMap(value));
                } else {
                    flattenedMap.put(key, (String) value);
                }
            }
            return flattenedMap;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error flattening secrets map: " + e.getMessage(), e);
        }
    }

    private static Map<String, String> asSimpleMap(String secretsJson) {
        if(StringUtils.isEmpty(secretsJson) || "null".equalsIgnoreCase(secretsJson)) {
            return java.util.Collections.emptyMap();
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> secretsMap = new HashMap<>();

        try {
            secretsMap = objectMapper.readValue(secretsJson, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Error parsing secrets JSON: " + e.getMessage(), e);
        }
        return secretsMap;
    }

    public static String asJson(Map<String, String> secretsMap) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(secretsMap);
        } catch (Exception e) {
            throw new RuntimeException("Error converting secrets map to JSON: " + e.getMessage(), e);
        }
    }
}
