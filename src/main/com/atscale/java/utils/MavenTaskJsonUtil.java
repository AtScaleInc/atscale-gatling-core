package com.atscale.java.utils;

import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.injectionsteps.OpenStep;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class MavenTaskJsonUtil {

    public static String openStepTasksToJson(List<MavenTaskDto<OpenStep>> tasks) {
        ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        try {
            return objectMapper.writeValueAsString(tasks);
        } catch (Exception e) {
            throw new RuntimeException("Error converting OpenStep tasks to JSON: " + e.getMessage(), e);
        }
    }

    public static String closedStepTasksToJson(List<MavenTaskDto<ClosedStep>> tasks) {
        ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        try {
            return objectMapper.writeValueAsString(tasks);
        } catch (Exception e) {
            throw new RuntimeException("Error converting ClosedStep tasks to JSON: " + e.getMessage(), e);
        }
    }

    public static List<MavenTaskDto<OpenStep>> openStepTasksFromJson(String json) {
        if(StringUtils.isEmpty(json)) {
            return java.util.Collections.emptyList();
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // construct a CollectionType for List<MavenTaskDto<OpenStep>> so Jackson preserves generic type information
            return objectMapper.readValue(
                json,
                objectMapper.getTypeFactory().constructCollectionType(
                    List.class,
                    objectMapper.getTypeFactory().constructParametricType(MavenTaskDto.class, OpenStep.class)
                )
            );
        } catch (Exception e) {
            throw new RuntimeException("Error parsing OpenStep tasks JSON: " + e.getMessage(), e);
        }
    }

    public static List<MavenTaskDto<ClosedStep>> closedStepTasksFromJson(String json) {
        if(StringUtils.isEmpty(json)) {
            return java.util.Collections.emptyList();
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // construct a CollectionType for List<MavenTaskDto<ClosedStep>> so Jackson preserves generic type information
            return objectMapper.readValue(
                json,
                objectMapper.getTypeFactory().constructCollectionType(
                    List.class,
                    objectMapper.getTypeFactory().constructParametricType(MavenTaskDto.class, ClosedStep.class)
                )
            );
        } catch (Exception e) {
            throw new RuntimeException("Error parsing ClosedStep tasks JSON: " + e.getMessage(), e);
        }
    }

    public static List<MavenTaskDto<OpenStep>> readOpenStepTasksFromFile(String fileName) {
        String json = readTaskFromFile(fileName);
        return openStepTasksFromJson(json);
    }

    public static List<MavenTaskDto<ClosedStep>> readClosedStepTasksFromFile(String fileName) {
        String json = readTaskFromFile(fileName);
        return closedStepTasksFromJson(json);
    }

    private static String readTaskFromFile(String fileName) {
        Path path = getTaskFilePath(fileName);
        try {
            return java.nio.file.Files.readString(path);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error reading task file: %s cause: %s ", path, e.getMessage()), e);
        }
    }

    public static void writeOpenStepTasksToFile(String fileName, List<MavenTaskDto<OpenStep>> tasks) {
        String json = openStepTasksToJson(tasks);
        writeTaskFile(fileName, json);
    }

    public static void writeClosedStepTasksToFile(String fileName, List<MavenTaskDto<ClosedStep>> tasks) {
        String json = closedStepTasksToJson(tasks);
        writeTaskFile(fileName, json);
    }

    private static void writeTaskFile(String fileName, String content) {
        Path path = getTaskFilePath(fileName);
        try {
            java.nio.file.Files.createDirectories(path.getParent());
            java.nio.file.Files.writeString(path, content);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error writing task file: %s cause: %s ", path, e.getMessage()), e);
        }
    }

    public static void deleteTaskFile(String fileName) {
        Path path = getTaskFilePath(fileName);
        try {
            java.nio.file.Files.deleteIfExists(path);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error deleting task file: %s cause: %s ", path, e.getMessage()), e);
        }
    }

    public static boolean taskFileExists(String fileName) {
        Path path = getTaskFilePath(fileName);
        return java.nio.file.Files.exists(path);
    }

    private static Path getTaskFilePath(String fileName) {
        return Paths.get(System.getProperty("user.dir"), "executor_tasks", fileName);
    }
}
