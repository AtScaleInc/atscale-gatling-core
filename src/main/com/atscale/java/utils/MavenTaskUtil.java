package com.atscale.java.utils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class MavenTaskUtil{
    static void writeTaskFile(String fileName, String content) {
        Path path = getTaskFilePath(fileName);
        try {
            Path parent = path.getParent();
            if (parent != null) {
                java.nio.file.Files.createDirectories(parent);
            }
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

    public static boolean taskFileNotExists(String fileName) {
        Path path = getTaskFilePath(fileName);
        return !java.nio.file.Files.exists(path);
    }

    static Path getTaskFilePath(String fileName) {
        return Paths.get(System.getProperty("user.dir"), "executor_tasks", fileName);
    }
}
