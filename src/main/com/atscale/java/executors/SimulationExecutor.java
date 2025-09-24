package com.atscale.java.executors;

import com.atscale.java.utils.PropertiesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.io.File;

@SuppressWarnings("unused")
public abstract class SimulationExecutor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimulationExecutor.class);

    protected void execute() {
        String heapSize = PropertiesFileReader.getAtScaleHeapSize();

        // This assumes that the Maven wrapper script (mvnw) is present in the project root directory
        String projectRoot = getApplicationDirectory();
        String mavenScript = getMavenWrapperScript();

        // Get the Gatling simulation tasks and run each simulation in a separate JVM Process
        // that means we have the capability to run multiple simulations in parallel and passing
        // params via -D system properties is safe and isolated per process

        List<Thread> taskThreads = new java.util.ArrayList<>();
        List<MavenTaskDto<T>> tasks = getSimulationTasks();

        for (MavenTaskDto<T> task : tasks) {
            Thread taskThread = new Thread(() -> {
                String osName = System.getProperty("os.name").toLowerCase();
                LOGGER.info("OS is {}", osName);
                LOGGER.info("Running task: {}", task.getTaskName());
                LOGGER.info("Maven Command: {}", task.getMavenCommand());
                LOGGER.info("Simulation Class: {}", task.getSimulationClass());
                LOGGER.info("Run Description: {}", task.getRunDescription());

                try {
                    List<String> command = new java.util.ArrayList<>();
                    command.add(mavenScript);

                    // Add -Dgatling.simulationClass and -Dgatling.runDescription (no extra quotes)
                    String simClass = String.format("-D%s=%s", MavenTaskDto.GATLING_SIMULATION_CLASS, task.getSimulationClass());
                    String runDesc = String.format("-D%s=%s", MavenTaskDto.GATLING_RUN_DESCRIPTION, task.getRunDescription());
                    String model = String.format("-D%s=%s", MavenTaskDto.ATSCALE_MODEL, task.getModel());
                    String runId = String.format("-D%s=%s", MavenTaskDto.ATSCALE_RUN_ID, task.getRunId());
                    String logFileName = String.format("-D%s=%s", MavenTaskDto.ATSCALE_LOG_FILE_NAME, task.getRunLogFileName());
                    String logAppend = String.format("-D%s=%s", MavenTaskDto.GATLING_RUN_LOGAPPEND, task.isRunLogAppend());
                    String injectionSteps = String.format("-D%s=%s", MavenTaskDto.GATLING_INJECTIION_STEPS, task.getInjectionSteps());

                  
                    LOGGER.debug("SimEx Using simulation class: {}", simClass);
                    LOGGER.debug("SimEx Using run description: {}", runDesc);
                    LOGGER.debug("SimEx Using model: {}", model);
                    LOGGER.debug("SimEx Using run id: {}", runId);
                    LOGGER.debug("SimEx Using log file name: {}", logFileName);
                    LOGGER.debug("SimEx Logging as append: {}", logAppend);
                    LOGGER.debug("SimEx Using injection steps: {}", injectionSteps);


                    command.add(simClass);
                    command.add(runDesc);
                    command.add(model);
                    command.add(runId);
                    command.add(logFileName);   
                    command.add(logAppend);
                    command.add(injectionSteps);

                    // Add the Maven goal (e.g., gatling:test)
                    command.add(task.getMavenCommand());

                    LOGGER.info("Running process with heap size: {}", String.format("-Xmx%s", heapSize));
                    ProcessBuilder processBuilder = new ProcessBuilder(command);
                    // Set working directory to project root where mvnw(.cmd) exists
                    processBuilder.directory(new File(projectRoot));
                    processBuilder.environment().put("MAVEN_OPTS", String.format("-Xmx%s", heapSize));
                    processBuilder.inheritIO(); // This will print output to console
                    
                    LOGGER.info("Starting the test suite on a separate JVM.  Using command args: {}", command);
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
            });
            taskThread.start();
            taskThreads.add(taskThread);
        }

        // Have the current thread wait for all task threads to complete
        for (Thread taskThread : taskThreads) {
            try {
                taskThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
            }
        }

        // Because of the way logback initializes early it produces some empty log files
        // Delete all zero-byte files in the run_logs directory
        String runLogPath = Paths.get(getApplicationDirectory(), "run_logs").toString();
        LOGGER.info("Run Log Path: {}",  runLogPath);
        File runLogsDir = new File(runLogPath);
        if (runLogsDir.exists() && runLogsDir.isDirectory()) {
            File[] files = runLogsDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.length() == 0) {
                        LOGGER.info("Found zero-byte file: {}", file.getAbsolutePath());
                        if (file.delete()) {
                            LOGGER.info("Deleted zero-byte file: {}", file.getAbsolutePath());
                        } 
                    }
                }
            }
        }
    }

    private String getApplicationDirectory() {
        try {
            String path = Paths.get(System.getProperty("user.dir")).toString();
             File file = new File(path);
            if (! file.isDirectory()){
                throw new RuntimeException("Resolved to path, but is not a valid directory: " + path);
            }
            return path;
            // If running from a JAR, get parent directory; if from classes, go up to project root
            //return file.isFile() ? file.getParent() : file.getParentFile().getParentFile().getParent();
        } catch (Exception e) {
            throw new RuntimeException("Unable to determine application directory", e);
        }
    }

    private String getMavenWrapperScript() {
        String osName = System.getProperty("os.name").toLowerCase();
        return osName.contains("win") ?  "mvnw.cmd" : "./mvnw";
    }

    protected abstract List<MavenTaskDto<T>> getSimulationTasks();
}
