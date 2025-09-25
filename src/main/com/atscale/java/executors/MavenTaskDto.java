package com.atscale.java.executors;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ArrayList;
import com.atscale.java.utils.InjectionStepJsonUtil;
import com.atscale.java.injectionsteps.*;

@SuppressWarnings("unused")
public class MavenTaskDto<T> {
    public static final String GATLING_SIMULATION_CLASS = "gatling.simulationClass";
    public static final String GATLING_RUN_DESCRIPTION = "gatling_run_description"; 
    public static final String GATLING_RUN_ID = "gatling_run_id";
    public static final String GATLING_RUN_LOGFILENAME = "gatling_run_logFileName";
    public static final String GATLING_RUN_LOGAPPEND = "gatling_run_logAppend";
    public static final String GATLING_INJECTIION_STEPS = "atscale.gatling.injection.steps";
    public static final String ATSCALE_MODEL = "atscale.model";
    public static final String ATSCALE_RUN_ID = "atscale.run.id";
    public static final String ATSCALE_LOG_FILE_NAME = "gatling_run_logFileName";
    public static final String ATSCALE_LOG_APPEND = "gatling_run_logAppend";   

    private final String taskName;
    private String mavenCommand;
    private String simulationClass;
    private String runDescription;
    private String model;
    private String runId;
    private String logFileName;
    private boolean runLogAppend;
    private List <T> injectionSteps;

    public MavenTaskDto(String taskName) {
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
        return simulationClass;
    }

    public void setSimulationClass(String simulationClass) {
        this.simulationClass = simulationClass;
    }

    public String getRunDescription() {
        return encode(this.runDescription);
    }

    public void setRunDescription(String runDescription) {
        this.runDescription = runDescription;
    }

    public String getModel() {
        return encode(this.model);
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setInjectionSteps(List<T> injectionSteps) {
        this.injectionSteps = injectionSteps;
    }

    public String getInjectionSteps() {
        String injectionStepsAsJson;
        if (injectionSteps != null && !injectionSteps.isEmpty() && injectionSteps.get(0) instanceof OpenStep) {
            // Safely filter and cast to OpenStep
            List<OpenStep> openSteps = injectionSteps.stream()
                .filter(OpenStep.class::isInstance)
                .map(OpenStep.class::cast)
                .collect(java.util.stream.Collectors.toList());
            injectionStepsAsJson = InjectionStepJsonUtil.openInjectionStepsAsJson(openSteps);
        } else if (injectionSteps != null && !injectionSteps.isEmpty() && injectionSteps.get(0) instanceof ClosedStep) {
            // Safely filter and cast to ClosedStep
            List<ClosedStep> closedSteps = injectionSteps.stream()
                .filter(ClosedStep.class::isInstance)
                .map(ClosedStep.class::cast)
                .collect(java.util.stream.Collectors.toList());
            injectionStepsAsJson = InjectionStepJsonUtil.closedInjectionStepsAsJson(closedSteps);
        } else {
            injectionStepsAsJson = "";
        }
        return encode(injectionStepsAsJson);
    }


    private String generateRunId() {
        // Generate a timestamp-based run ID combined with a random alphanumeric string
        // This value is picked up in the logback.xml file where it is used to create a unique log file name
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String random = org.apache.commons.lang3.RandomStringUtils.secure().nextAlphanumeric(10);
        return String.format("%s-%s", timestamp, random);
    }

    public void setRunId(String runId) {
       // addGatlingProperty("gatling_run_id", runId);
       this.runId = runId;
    }

    public String getRunId() {
        return encode(this.runId);
    }

    public void setRunLogFileName(String fileName) {
        this.logFileName = fileName;
    }

    public String getRunLogFileName() {
         // do not encode this it causes problems with log4j2.xml picking up the encoded value as a literal
        return this.logFileName;
    }

    public void setLoggingAsAppend(boolean append) {
        if(append) {
            setLoggingToAppend();
        } else {
            setLoggingToOverwrite();
        }
    }

    private void setLoggingToAppend() {
        //addGatlingProperty("gatling_run_logAppend", "true");
        this.runLogAppend = true;
    }

    private void setLoggingToOverwrite() {
        //addGatlingProperty("gatling_run_logAppend", "false");
        this.runLogAppend = false;
    }

    public String isRunLogAppend() {
        // do not encode this it causes problems with log4j2.xml picking up the encoded value as a literal
        return String.valueOf(runLogAppend);
    }

    public static String encode(String input) {
        return java.util.Base64.getEncoder().encodeToString(input.getBytes());
    }

    public static String decode(String base64) {
        byte[] decodedBytes = java.util.Base64.getDecoder().decode(base64);
        return new String(decodedBytes);
    }

}
