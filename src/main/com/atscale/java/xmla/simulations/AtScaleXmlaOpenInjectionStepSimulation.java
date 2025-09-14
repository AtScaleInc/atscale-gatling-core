package com.atscale.java.xmla.simulations;

import com.atscale.java.injectionsteps.OpenStep;
import com.atscale.java.utils.InjectionStepJsonUtil;
import com.atscale.java.utils.PropertiesFileReader;
import com.atscale.java.xmla.XmlaProtocol;
import com.atscale.java.xmla.scenarios.AtScaleXmlaScenario;
import io.gatling.javaapi.core.OpenInjectionStep;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import static io.gatling.javaapi.core.OpenInjectionStep.atOnceUsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class AtScaleXmlaOpenInjectionStepSimulation extends Simulation {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleXmlaOpenInjectionStepSimulation.class);

    public AtScaleXmlaOpenInjectionStepSimulation() {
        String model = System.getProperties().getProperty("atscale.model");
        String runId = System.getProperties().getProperty("gatling_run_id");
        String runLogFileName = System.getProperties().getProperty("gatling_run_logFileName");
        String loggingAsAppend = System.getProperties().getProperty("gatling_run_logAppend");

        String cube = PropertiesFileReader.getAtScaleXmlaCubeName(model);
        String catalog = PropertiesFileReader.getAtScaleXmlaCatalogName(model);
        String steps = System.getProperties().getProperty("atscale.gatling.injection.steps");
        if (model == null || model.isEmpty()) {
            LOGGER.error("AtScale model is not specified. Please set the 'atscale.model' system property.");
            throw new IllegalArgumentException("AtScale model is required.");
        }

        LOGGER.info("Simulation class {} Gatling run ID: {}", this.getClass().getName(), runId);
        LOGGER.info("Using model: {}", model);
        LOGGER.info("Using injection steps: {}", steps);
        LOGGER.info("Using log file name: {}", runLogFileName);
        LOGGER.info("Logging as append: {}", loggingAsAppend);

        List<OpenStep> openSteps = InjectionStepJsonUtil.openInjectionStepsFromJson(steps);
        List<io.gatling.javaapi.core.OpenInjectionStep> injectionSteps = new ArrayList<>();
        for (com.atscale.java.injectionsteps.OpenStep step : openSteps) {
            if (step == null) {
                LOGGER.warn("Encountered null OpenStep, skipping.");
                continue;
            }
            OpenInjectionStep gatlingStep = step.toGatlingStep();
            if (gatlingStep == null) {
                LOGGER.warn("Failed to convert OpenStep to Gatling step for: {}", step);
                continue;
            }
            injectionSteps.add(gatlingStep);
        }

        if (injectionSteps.isEmpty()) {
            LOGGER.warn("No valid injection steps provided. Defaulting to atOnceUsers(1)");
            injectionSteps.add(atOnceUsers(1));
        }

        AtScaleXmlaScenario scn = new AtScaleXmlaScenario();
        ScenarioBuilder sb = scn.buildScenario(model, cube, catalog, runId);
        setUp(sb.injectOpen(injectionSteps).protocols(XmlaProtocol.forXmla(model)));
    }
}
