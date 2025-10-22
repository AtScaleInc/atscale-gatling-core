package com.atscale.java.utils;

import com.fasterxml.jackson.core.type.TypeReference;
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
