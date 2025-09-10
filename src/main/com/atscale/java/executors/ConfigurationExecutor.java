package com.atscale.java.executors;

import com.atscale.java.utils.PropertiesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Run this to configure the Hive driver dependency.
 * A compatible version is not available from Maven Repositories and thus needs unique handling.
 * Configures Hive by installing the necessary JDBC driver to the local Maven repository.
 * It performs a Maven clean and compile to ensure the project is set up correctly.
 */
public class ConfigurationExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationExecutor.class);

    public static void main(String[] args) {
        LOGGER.info("ConfigurationExecutor started.");

        ConfigurationExecutor executor = new ConfigurationExecutor();
        executor.execute();

        LOGGER.info("ConfigurationExecutor completed.");
    }

    protected void execute() {
        String heapSize = PropertiesFileReader.getAtScaleHeapSize();
        // Clean up using Maven clean and then install
        // This assumes that the Maven wrapper script (mvnw) is present in the project root directory
        String projectRoot = System.getProperty("user.dir");

        try {
            ProcessBuilder processBuilder = new ProcessBuilder(
                    String.format("%s/mvnw", projectRoot),
                    "install:install-file",
                    String.format("-Dfile=%s/lib/hive-jdbc-uber-2.6.3.0-235.jar", projectRoot),
                    "-DgroupId=veil.hdp.hive",
                    "-DartifactId=hive-jdbc-uber",
                    "-Dversion=2.6.3.0-235",
                    "-Dpackaging=jar"
            );
            Process process = processBuilder.start();
            process.waitFor(); // Wait for the process to complete
            if (process.exitValue() != 0) {
                LOGGER.error("Installation of hive-jdbc-uber-2.6.3.0-235.jar failed with exit code: {}", process.exitValue());
                throw new RuntimeException("Installation of hive-jdbc-uber-2.6.3.0-235.jar failed");
            }
            LOGGER.info("Maven installation of hive-jdbc-uber-2.6.3.0-235.jar completed successfully.  It should be available in your local maven repo under groupId veil.hdp.hive, artifactId hive-jdbc-uber");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to run maven clean compile", e);
        }

        try {
            ProcessBuilder processBuilder = new ProcessBuilder(String.format("%s/mvnw", projectRoot), "clean", "compile");
            Process process = processBuilder.start();
            process.waitFor(); // Wait for the process to complete
            if (process.exitValue() != 0) {
                LOGGER.error("Maven clean compile failed with exit code: {}", process.exitValue());
                throw new RuntimeException("Maven clean compile failed");
            }
            LOGGER.info("Maven clean compile completed successfully.");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to run maven clean compile", e);
        }
    }
}
