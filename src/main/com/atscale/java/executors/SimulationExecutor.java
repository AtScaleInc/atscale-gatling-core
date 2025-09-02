package com.atscale.java.executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.io.File;

@SuppressWarnings("unused")
public abstract class SimulationExecutor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimulationExecutor.class);

    protected void execute() {
        // Clean up using Maven clean and then install
        // This assumes that the Maven wrapper script (mvnw) is present in the project root directory
        String projectRoot = System.getProperty("user.dir");

        try {
            ProcessBuilder processBuilder = new ProcessBuilder(String.format("%s/mvnw", projectRoot), "clean", "compile");
            Process process = processBuilder.start();
            process.waitFor(); // Wait for the process to complete
            if (process.exitValue() != 0) {
                LOGGER.error("Maven clean compile failed with exit code: {}", process.exitValue());
                throw new RuntimeException("Maven clean compile failed");
            }
            LOGGER.info("Maven clean completed successfully.");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to run maven clean compile", e);
        }

        // Get the Gatling simulation tasks and run each simulation in a separate JVM Process
        // that means we have the capability to run multiple simulations in parallel and passing
        // params via -D system properties is safe and isolated per process

        List<Thread> taskThreads = new java.util.ArrayList<>();
        List<MavenTaskDto> tasks = getSimulationTasks();

        for(MavenTaskDto task: tasks) {
            Thread taskThread = new Thread(() -> {
                LOGGER.info("Running task: {}", task.getTaskName());
                LOGGER.info("Maven Command: {}", task.getMavenCommand());
                LOGGER.info("Simulation Class: {}", task.getSimulationClass());
                LOGGER.info("Run Description: {}", task.getRunDescription());

                try {
                    ProcessBuilder processBuilder = new ProcessBuilder(
                            String.format("%s/mvnw", projectRoot),
                            task.getMavenCommand(),
                            task.getSimulationClass(),
                            task.getRunDescription()
                    );
                    for(String key : task.getGatlingProperties().keySet()) {
                        String value = task.getGatlingProperties().get(key);
                        processBuilder.command().add(String.format("-D%s=%s", key, value));
                    }
                    processBuilder.inheritIO(); // This will print output to console
                    Process process = processBuilder.start();
                    process.waitFor(); // Wait for the process to complete
                    if (process.exitValue() != 0) {
                        LOGGER.error("Task {} failed with exit code: {}", task.getTaskName(), process.exitValue());
                        throw new RuntimeException("Task failed");
                    }
                    LOGGER.info("Task {} completed successfully.", task.getTaskName());
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException("Failed to run task: " + task.getTaskName(), e);
                }
        }
        );
            taskThread.start();
            taskThreads.add(taskThread);
        }

        // Have the current thread wait for all task threads to complete
        for(Thread taskThread : taskThreads) {
            try {
                taskThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
            }
        }

        // Because of the way logback initializes early it produces some empty log files
        // Delete all zero-byte files in the run_logs directory
        File runLogsDir = new File(projectRoot, "run_logs");
        if (runLogsDir.exists() && runLogsDir.isDirectory()) {
            File[] files = runLogsDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.length() == 0) {
                        if (file.delete()) {
                            LOGGER.info("Deleted zero-byte file: {}", file.getAbsolutePath());
                        } else {
                            LOGGER.warn("Failed to delete zero-byte file: {}", file.getAbsolutePath());
                        }
                    }
                }
            }
        }
    }

    protected abstract List<MavenTaskDto> getSimulationTasks();

    protected abstract String injectionStepsAsJson(List<T> injectionSteps);
}
