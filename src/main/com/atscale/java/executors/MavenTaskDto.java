package com.atscale.java.executors;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class MavenTaskDto {
    private final String taskName;
    private String mavenCommand;
    private String simulationClass;
    private String runDescription;
    private java.util.Map <String, String> gatlingProperties;

    public MavenTaskDto(String taskName) {
        gatlingProperties = new HashMap<>();
        this.taskName = taskName;
        String runId = generateRunId();
        setRunId(runId);
        setRunLogFileName(String.format("gatling-%s.log", runId));  //default value
        setLoggingAsAppend(false);
    }

    public String getTaskName() {
        return taskName;
    }

    public String getMavenCommand() {
        return mavenCommand;
    }

    public void setMavenCommand(String mavenCommand) {
        this.mavenCommand = mavenCommand;
    }

    public String getSimulationClass() {
        return String.format("-Dgatling.simulationClass=%s", simulationClass);
    }

    public void setSimulationClass(String simulationClass) {
        this.simulationClass = simulationClass;
    }

    public String getRunDescription() {
        return String.format("""
        -Dgatling.runDescription="%s"
        """, runDescription);
    }

    public void setRunDescription(String runDescription) {
        this.runDescription = runDescription;
    }

    public Map<String, String> getGatlingProperties() {
        return gatlingProperties;
    }

    public void setGatlingProperties(Map<String, String> gatlingProperties) {
        this.gatlingProperties = gatlingProperties;
    }
    public void addGatlingProperty(String key, String value) {
        gatlingProperties.put(key, value);
    }

    private String generateRunId() {
        // Generate a timestamp-based run ID combined with a random alphanumeric string
        // This value is picked up in the logback.xml file where it is used to create a unique log file name
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String random = org.apache.commons.lang3.RandomStringUtils.secure().nextAlphanumeric(10);
        return String.format("%s-%s", timestamp, random);
    }

    private void setRunId(String runId) {
        addGatlingProperty("gatling_run_id", runId);
    }

    public void setRunLogFileName(String fileName) {
        addGatlingProperty("gatling_run_logFileName", fileName);
    }

    public void setLoggingAsAppend(boolean append) {
        if(append) {
            setLoggingToAppend();
        } else {
            setLoggingToOverwrite();
        }
    }

    private void setLoggingToAppend() {
        addGatlingProperty("gatling_run_logAppend", "true");
    }

    private void setLoggingToOverwrite() {
        addGatlingProperty("gatling_run_logAppend", "false");
    }
}
