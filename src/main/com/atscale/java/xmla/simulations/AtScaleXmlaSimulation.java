package com.atscale.java.xmla.simulations;

import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.utils.JsonUtil;
import com.atscale.java.utils.PropertiesFileReader;
import com.atscale.java.xmla.scenarios.AtScaleXmlaScenario;
import io.gatling.javaapi.core.ScenarioBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.gatling.javaapi.core.Simulation;

/** Abstract base class for AtScale XMLA simulations.
 * The intent here is to have a common base class for AtScale JDBC simulations
 * that handles shared configuration and setup logic.
 * This abstract class reads configuration properties, initializes logging,
 * and builds the common scenario using AtScaleXmlaScenario.
 * Specific simulation implementations can extend this class to define their
 * own injection steps and protocols.
 * This class and it's members are 'package-private' by intent.
 * @author steve.hall@atscale
 */
abstract class AtScaleXmlaSimulation extends Simulation {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleXmlaSimulation.class);
    String model;
    String steps;
    String runId;
    String runLogFileName;
    String loggingAsAppend;
    String ingestionFile;
    String ingestionFileHasHeader;
    String additionalProperties;
    String cube;
    String catalog;
    ScenarioBuilder sb;

    AtScaleXmlaSimulation() {
        model = System.getProperties().getProperty(MavenTaskDto.ATSCALE_MODEL);
        steps = System.getProperties().getProperty(MavenTaskDto.GATLING_INJECTION_STEPS);
        runId = System.getProperties().getProperty(MavenTaskDto.ATSCALE_RUN_ID);
        runLogFileName = System.getProperties().getProperty(MavenTaskDto.ATSCALE_LOG_FILE_NAME);
        loggingAsAppend = System.getProperties().getProperty(MavenTaskDto.ATSCALE_LOG_APPEND);
        ingestionFile = System.getProperties().getProperty(MavenTaskDto.ATSCALE_QUERY_INGESTION_FILE, null);
        ingestionFileHasHeader = System.getProperties().getProperty(MavenTaskDto.ATSCALE_QUERY_INGESTION_FILE_HAS_HEADER);additionalProperties = System.getProperties().getProperty(MavenTaskDto.ADDITIONAL_PROPERTIES);

        model = MavenTaskDto.decode(model);
        steps = MavenTaskDto.decode(steps);
        runId = MavenTaskDto.decode(runId);
        ingestionFile = MavenTaskDto.decode(ingestionFile);
        additionalProperties = MavenTaskDto.decode(additionalProperties);
        java.util.Map<String, String> additionalPropertiesMap = JsonUtil.asMap(additionalProperties);
        PropertiesFileReader.setCustomProperties(additionalPropertiesMap);

        LOGGER.info("Simulation class {} Gatling run ID: {}", this.getClass().getName(), runId);
        LOGGER.info("Using model: {}", model);
        LOGGER.info("Using injection steps: {}", steps);
        LOGGER.info("Using run id: {}", runId);
        LOGGER.info("Using log file name: {}", runLogFileName);
        LOGGER.info("Logging as append: {}", loggingAsAppend);
        LOGGER.info("Using ingestion file: {}", ingestionFile);
        LOGGER.info("Using ingestion file has header: {}", ingestionFileHasHeader);
        LOGGER.info("Using {} additional properties", additionalPropertiesMap.size());

        cube = PropertiesFileReader.getAtScaleXmlaCubeName(model);
        catalog = PropertiesFileReader.getAtScaleXmlaCatalogName(model);

        if (model == null || model.isEmpty()) {
            LOGGER.error("AtScale model is not specified. Please set the 'atscale.model' system property.");
            throw new IllegalArgumentException("AtScale model is required.");
        }

        AtScaleXmlaScenario scn = new AtScaleXmlaScenario();
        sb = scn.buildScenario(model, cube, catalog, runId, ingestionFile, Boolean.parseBoolean(ingestionFileHasHeader));
    }
}
