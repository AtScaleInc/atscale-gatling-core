package com.atscale.java.xmla.simulations;

import com.atscale.java.executors.MavenTaskDto;
import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.utils.InjectionStepJsonUtil;
import com.atscale.java.utils.PropertiesFileReader;
import com.atscale.java.xmla.XmlaProtocol;
import com.atscale.java.xmla.scenarios.AtScaleXmlaScenario;
import io.gatling.javaapi.core.ClosedInjectionStep;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import static io.gatling.javaapi.core.CoreDsl.constantConcurrentUsers;


@SuppressWarnings("unused")
public class AtScaleXmlaClosedInjectionStepSimulation extends Simulation {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleXmlaClosedInjectionStepSimulation.class);

    public AtScaleXmlaClosedInjectionStepSimulation() {
        String model = System.getProperties().getProperty(MavenTaskDto.ATSCALE_MODEL);
        String runDescription = System.getProperties().getProperty(MavenTaskDto.GATLING_RUN_DESCRIPTION);
        String steps = System.getProperties().getProperty(MavenTaskDto.GATLING_INJECTIION_STEPS);
        String runId = System.getProperties().getProperty(MavenTaskDto.ATSCALE_RUN_ID);
        String runLogFileName = System.getProperties().getProperty(MavenTaskDto.ATSCALE_LOG_FILE_NAME);
        String loggingAsAppend = System.getProperties().getProperty(MavenTaskDto.ATSCALE_LOG_APPEND);

        model = MavenTaskDto.decode(model);
        runDescription = MavenTaskDto.decode(runDescription);
        steps = MavenTaskDto.decode(steps);
        runId = MavenTaskDto.decode(runId);

        LOGGER.info("Simulation class {} Gatling run ID: {}", this.getClass().getName(), runId);
        LOGGER.info("Using model: {}", model);
        LOGGER.info("Using run description: {}", runDescription);
        LOGGER.info("Using injection steps: {}", steps);
        LOGGER.info("Using run id: {}", runId);
        LOGGER.info("Using log file name: {}", runLogFileName);
        LOGGER.info("Logging as append: {}", loggingAsAppend);

        String cube = PropertiesFileReader.getAtScaleXmlaCubeName(model);
        String catalog = PropertiesFileReader.getAtScaleXmlaCatalogName(model);
        
        if (model == null || model.isEmpty()) {
            LOGGER.error("AtScale model is not specified. Please set the 'atscale.model' system property.");
            throw new IllegalArgumentException("AtScale model is required.");
        }

        List<ClosedStep> ClosedSteps = InjectionStepJsonUtil.closedInjectionStepsFromJson(steps);
        List<ClosedInjectionStep> injectionSteps = new ArrayList<>();
        for (ClosedStep step : ClosedSteps) {
            if (step == null) {
                LOGGER.warn("Encountered null ClosedStep, skipping.");
                continue;
            }
            ClosedInjectionStep gatlingStep = step.toGatlingStep();
            if (gatlingStep == null) {
                LOGGER.warn("Failed to convert ClosedStep to Gatling step for: {}", step);
                continue;
            }
            injectionSteps.add(gatlingStep);
        }

        if (injectionSteps.isEmpty()) {
            LOGGER.warn("No valid injection steps provided. Defaulting to constantConcurrentUsers(1) for 1 minute");
            injectionSteps.add(constantConcurrentUsers(1).during(java.time.Duration.ofMinutes(1)));
        }

        AtScaleXmlaScenario scn = new AtScaleXmlaScenario();
        ScenarioBuilder sb = scn.buildScenario(model, cube, catalog, runId);
        setUp(sb.injectClosed(injectionSteps).protocols(XmlaProtocol.forXmla(model)));
    }
}
