package com.atscale.java.utils;

import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.injectionsteps.OpenStep;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Path;
import java.util.List;

public class MavenTaskYamlUtil extends MavenTaskUtil {

    public static String openStepTasksToYaml(List<MavenTaskDto<OpenStep>> tasks) {
        YAMLMapper mapper = new YAMLMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            return mapper.writeValueAsString(tasks);
        } catch (Exception e) {
            throw new RuntimeException("Error converting OpenStep tasks to YAML: " + e.getMessage(), e);
        }
    }

    public static String closedStepTasksToYaml(List<MavenTaskDto<ClosedStep>> tasks) {
        YAMLMapper mapper = new YAMLMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            return mapper.writeValueAsString(tasks);
        } catch (Exception e) {
            throw new RuntimeException("Error converting ClosedStep tasks to YAML: " + e.getMessage(), e);
        }
    }

    public static List<MavenTaskDto<OpenStep>> openStepTasksFromYaml(String yaml) {
        if (StringUtils.isEmpty(yaml)) {
            return java.util.Collections.emptyList();
        }

        try {
            YAMLMapper mapper = new YAMLMapper();
            return mapper.readValue(
                yaml,
                mapper.getTypeFactory().constructCollectionType(
                    List.class,
                    mapper.getTypeFactory().constructParametricType(MavenTaskDto.class, OpenStep.class)
                )
            );
        } catch (Exception e) {
            throw new RuntimeException("Error parsing OpenStep tasks YAML: " + e.getMessage(), e);
        }
    }

    public static List<MavenTaskDto<ClosedStep>> closedStepTasksFromYaml(String yaml) {
        if (StringUtils.isEmpty(yaml)) {
            return java.util.Collections.emptyList();
        }

        try {
            YAMLMapper mapper = new YAMLMapper();
            return mapper.readValue(
                yaml,
                mapper.getTypeFactory().constructCollectionType(
                    List.class,
                    mapper.getTypeFactory().constructParametricType(MavenTaskDto.class, ClosedStep.class)
                )
            );
        } catch (Exception e) {
            throw new RuntimeException("Error parsing ClosedStep tasks YAML: " + e.getMessage(), e);
        }
    }

    public static List<MavenTaskDto<OpenStep>> readOpenStepTasksFromFile(String fileName) {
        String yaml = readTaskFromFile(fileName);
        return openStepTasksFromYaml(yaml);
    }

    public static List<MavenTaskDto<ClosedStep>> readClosedStepTasksFromFile(String fileName) {
        String yaml = readTaskFromFile(fileName);
        return closedStepTasksFromYaml(yaml);
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
        String yaml = openStepTasksToYaml(tasks);
        writeTaskFile(fileName, yaml);
    }

    public static void writeClosedStepTasksToFile(String fileName, List<MavenTaskDto<ClosedStep>> tasks) {
        String yaml = closedStepTasksToYaml(tasks);
        writeTaskFile(fileName, yaml);
    }
}

