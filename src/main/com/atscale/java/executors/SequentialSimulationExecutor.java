package com.atscale.java.executors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@SuppressWarnings({"unused", "RV_RETURN_VALUE_IGNORED"})
public abstract class SequentialSimulationExecutor<T> extends SimulationExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequentialSimulationExecutor.class);

    protected void execute() {
        try {
            // This assumes that the Maven wrapper script (mvnw) is present in the project root directory
            String projectRoot = getApplicationDirectory();
            String mavenScript = getMavenWrapperScript();

            // Get the Gatling simulation tasks and run each simulation in a separate JVM Process
            // that means we have the capability to run multiple simulations sequentially thus passing
            // params via -D system properties is safe and isolated per process
            List<MavenTaskDto<T>> tasks = getSimulationTasks();

            for (MavenTaskDto<T> task : tasks) {
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
                    String injectionSteps = String.format("-D%s=%s", MavenTaskDto.GATLING_INJECTION_STEPS, task.getInjectionSteps());
                    String ingestFile = String.format("-D%s=%s", MavenTaskDto.ATSCALE_QUERY_INGESTION_FILE, task.getIngestionFileName());
                    String ingestFileHasHeader = String.format("-D%s=%s", MavenTaskDto.ATSCALE_QUERY_INGESTION_FILE_HAS_HEADER, task.getIngestionFileHasHeader());
                    String additionalProperties = String.format("-D%s=%s", MavenTaskDto.ADDITIONAL_PROPERTIES, task.getAdditionalProperties());
                    String alternatePropertiesFileName = task.getAlternatePropertiesFileName();
                    if(StringUtils.isNotEmpty(alternatePropertiesFileName)){
                        throw new UnsupportedOperationException("Sequential executors do not support the use of alternate properties files.  Remove the call(s) to setAlternatePropertiesFileName() in the task definition(s).");
                    }

                    LOGGER.debug("SimEx Using simulation class: {}", simClass);
                    LOGGER.debug("SimEx Using run description: {}", runDesc);
                    LOGGER.debug("SimEx Using model: {}", model);
                    LOGGER.debug("SimEx Using run id: {}", runId);
                    LOGGER.debug("SimEx Using log file name: {}", logFileName);
                    LOGGER.debug("SimEx Logging as append: {}", logAppend);
                    LOGGER.debug("SimEx Using injection steps: {}", injectionSteps);
                    LOGGER.debug("SimEx Using ingestion file: {}", ingestFile);
                    LOGGER.debug("SimEx Ingestion file has header: {}", ingestFileHasHeader);
                    LOGGER.debug("SimEx Using additional properties: {}", additionalProperties);

                    command.add(simClass);
                    command.add(runDesc);
                    command.add(model);
                    command.add(runId);
                    command.add(logFileName);
                    command.add(logAppend);
                    command.add(injectionSteps);
                    command.add(ingestFile);
                    command.add(ingestFileHasHeader);
                    command.add(additionalProperties);

                    // Add the Maven goal (e.g., gatling:test)
                    command.add(task.getMavenCommand());

                    ProcessBuilder processBuilder = new ProcessBuilder(command);
                    // Set working directory to project root where mvnw(.cmd) exists
                    processBuilder.directory(new File(projectRoot));
                    processBuilder.inheritIO(); // This will print output to console
                    processBuilder.redirectErrorStream(true);

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
            }
        } finally {
        // Because of the way logging initializes early it produces some empty log files
        // Delete all zero-byte files in the run_logs directory
        String runLogPath = Paths.get(getApplicationDirectory(), "run_logs").toString();
        LOGGER.info("Run Log Path: {}",  runLogPath);

        org.apache.logging.log4j.LogManager.shutdown();

        File runLogsDir = new File(runLogPath);
        if (runLogsDir.exists() && runLogsDir.isDirectory()) {
            File[] files = runLogsDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.length() == 0) {
                        //noinspection ResultOfMethodCallIgnored
                        file.delete();
                    }
                }
            }
        }
        }
    }

    protected abstract List<MavenTaskDto<T>> getSimulationTasks();
}
