package com.atscale.java.utils;

import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.injectionsteps.OpenStep;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

@SuppressWarnings({"LoggingSimilarMessage", "Convert2Diamond"})
public class InjectionStepJsonUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(InjectionStepJsonUtil.class);

    public static String openInjectionStepsAsJson(List<OpenStep> injectionSteps) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.writeValueAsString(injectionSteps);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize injection steps to JSON", e);
        }
    }

    public static String closedInjectionStepsAsJson(List<ClosedStep> injectionSteps) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.writeValueAsString(injectionSteps);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize injection steps to JSON", e);
        }
    }

    public static List<OpenStep> openInjectionStepsFromJson(String json) {
        try{
            if (json == null || json.isEmpty()) {
                LOGGER.warn("Received empty JSON string for deserialization.");
                return List.of();
            }
            json = cleanJson(json);
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.readValue(json, new TypeReference<List<OpenStep>>() {});
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize injection steps from JSON", e);
        }
    }

    public static List<ClosedStep> closedInjectionStepsFromJson(String json) {
        try{
            if (json == null || json.isEmpty()) {
                LOGGER.warn("Received empty JSON string for deserialization.");
                return List.of();
            }
            json = cleanJson(json);
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.readValue(json, new TypeReference<List<ClosedStep>>() {});
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize injection steps from JSON", e);
        }
    }

    private static String cleanJson(String json) {
        if (json.contains("\\\"")) {
            json = json.replace("\\\"", "\"");
            json = json.replaceAll("^\"|\"$", ""); // Remove wrapping quotes if present
        }
        return json;
    }
}
