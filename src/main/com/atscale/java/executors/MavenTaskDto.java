package com.atscale.java.executors;

import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Base64;
import java.nio.file.Path;
import java.util.regex.Pattern;

import com.atscale.java.utils.CsvLoaderUtil;
import com.atscale.java.utils.InjectionStepJsonUtil;
import com.atscale.java.injectionsteps.*;
import com.atscale.java.utils.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;

@SuppressWarnings("unused")
public class MavenTaskDto<T> {
    public static final String GATLING_SIMULATION_CLASS = "gatling.simulationClass"; //Do not change.  Gatling needs this property as defined to pick up the simulation class to run
    public static final String GATLING_RUN_DESCRIPTION = "gatling.runDescription";  //Do not change.  Gatling needs this property as defined to render description on HTML report
    public static final String GATLING_RUN_ID = "gatling_run_id";
    public static final String GATLING_RUN_LOGFILENAME = "gatling_run_logFileName";
    public static final String GATLING_RUN_LOGAPPEND = "gatling_run_logAppend";
    public static final String GATLING_INJECTION_STEPS = "atscale.gatling.injection.steps";
    public static final String ATSCALE_MODEL = "atscale.model";
    public static final String ATSCALE_RUN_ID = "atscale.run.id";
    public static final String ATSCALE_LOG_FILE_NAME = "gatling_run_logFileName";
    public static final String ATSCALE_LOG_APPEND = "gatling_run_logAppend";
    public static final String ATSCALE_QUERY_INGESTION_FILE = "query_ingestion_file";
    public static final String ATSCALE_QUERY_INGESTION_FILE_HAS_HEADER = "query_ingestion_file_has_header";
    public static final String ADDITIONAL_PROPERTIES = "additional_properties";

    private final String taskName;
    private String mavenCommand;
    private String simulationClass;
    private String runDescription;
    private String model;
    private String runId;
    private String logFileName;
    private boolean runLogAppend;
    private List <T> injectionSteps;
    private String ingestionFileName;
    private boolean ingestionFileHasHeader;
    private Map<String, String> additionalProperties = new HashMap<>();
    private String alternatePropertiesFileName;

    public MavenTaskDto() {
        // Delegate to the single-arg constructor so all initialization logic is reused
        this("Default Task Name");
    }

    public MavenTaskDto(String taskName) {
        this.taskName = taskName;
        String rid = generateRunId();
        setRunId(rid);
        setRunLogFileName(String.format("gatling-%s.log", rid));  //default value
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
        return this.runDescription;
    }

    public void setRunDescription(String runDescription) {
        this.runDescription = runDescription;
    }

    public String getModel() {
        return this.model;
    }

    @JsonIgnore
    public String getModelBase64() {
        return encode(this.model);
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setInjectionSteps(List<T> injectionSteps) {
        this.injectionSteps = injectionSteps;
    }

    public List<T> getInjectionSteps() {
        return this.injectionSteps;
    }

    @JsonIgnore
    public String getInjectionStepsAsBase64() {
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
       this.runId = runId;
    }

    public String getRunId() {
        return this.runId;
    }

    @JsonIgnore
    public String getRunIdBase64() {
        return encode(this.runId);
    }

    public void setRunLogFileName(String fileName) {
        this.logFileName = fileName;
    }

    public String getRunLogFileName() {
         // do not encode this it causes problems with log4j2.xml picking up the encoded value as a literal
        return this.logFileName;
    }

    public boolean getRunLogAppend() {
        return this.runLogAppend;
    }

    public void setRunLogAppend(boolean append) {
        this.runLogAppend = append;
    }

    public void setLoggingAsAppend(boolean append) {
        if(append) {
            setLoggingToAppend();
        } else {
            setLoggingToOverwrite();
        }
    }

    private void setLoggingToAppend() {
        this.runLogAppend = true;
    }

    private void setLoggingToOverwrite() {
        this.runLogAppend = false;
    }

    public String isRunLogAppend() {
        // do not encode this it causes problems with log4j2.xml picking up the encoded value as a literal
        return String.valueOf(runLogAppend);
    }

    public void setIngestionFileName(String ingestionFileName, boolean hasHeader) {
        Path path = new CsvLoaderUtil(ingestionFileName, true).getFilePath();

        this.ingestionFileName = ingestionFileName;
        this.ingestionFileHasHeader = hasHeader;
    }

    public String getIngestionFileName() {
        return this.ingestionFileName;
    }

    @JsonIgnore
    public String getIngestionFileNameBase64() {
        return encode(this.ingestionFileName);
    }

    public boolean getIngestionFileHasHeader() {
        return this.ingestionFileHasHeader;
    }

    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public Map<String, String> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonIgnore
    public String getAdditionalPropertiesBase64() {
        String additionalProps = JsonUtil.asJson(additionalProperties);
        return encode(additionalProps);
    }

    public Map<String, String> decodeAdditionalProperties(String additionalProperties) {
        String additionalPropsJson = decode(additionalProperties);
        return JsonUtil.asMap(additionalPropsJson);
    }

    public String getAlternatePropertiesFileName() {
        return alternatePropertiesFileName;
    }

    public void setAlternatePropertiesFileName(String alternatePropertiesFileName) {
        if(StringUtils.isEmpty(alternatePropertiesFileName)) {
            return;
        }

        Pattern validFileNamePattern = Pattern.compile("^[A-Za-z0-9._-]+$");

        if (!validFileNamePattern.matcher(alternatePropertiesFileName).matches()) {
            throw new IllegalArgumentException("Invalid alternate properties file name. File name can only include characters a-z, A-Z, 0-9, periods, dashes, and underscores.");
        }
        URL propertyFileURl = getClass().getClassLoader().getResource(alternatePropertiesFileName);
        if(null == propertyFileURl){
            throw new IllegalArgumentException("Alternate properties file not found in classpath: " + alternatePropertiesFileName);
        }
        this.alternatePropertiesFileName = alternatePropertiesFileName;
    }

    public static String encode(String input) {
        if(StringUtils.isEmpty(input)) {
            return input;
        }
        return java.util.Base64.getEncoder().encodeToString(input.getBytes());
    }

    public static String decode(String base64) {
        if("null".equalsIgnoreCase(base64)) {
            return null;
        } else if(StringUtils.isEmpty(base64)) {
            return base64;
        } else {
            return new String(Base64.getDecoder().decode(base64));
        }
    }

    public MavenTaskDto<T> copy(String newTaskName) {
        MavenTaskDto<T> copy = new MavenTaskDto<>(newTaskName);
        copy.setMavenCommand(this.mavenCommand);
        copy.setSimulationClass(this.simulationClass);
        copy.setRunDescription(this.runDescription);
        copy.setModel(this.model);
        copy.setRunId(this.runId);
        copy.setRunLogFileName(this.logFileName);
        copy.setRunLogAppend(this.runLogAppend);
        copy.setInjectionSteps(this.injectionSteps);
        copy.setIngestionFileName(this.ingestionFileName, this.ingestionFileHasHeader);
        copy.setAdditionalProperties(this.additionalProperties);
        copy.setAlternatePropertiesFileName(this.alternatePropertiesFileName);
        return copy;
    }
}
