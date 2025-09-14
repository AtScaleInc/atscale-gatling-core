package com.atscale.java.jdbc.simulations;

import com.atscale.java.jdbc.JdbcProtocol;
import com.atscale.java.jdbc.scenarios.AtScaleDynamicQueryBuilderScenario;
import com.atscale.java.utils.InjectionStepJsonUtil;
import io.gatling.javaapi.core.OpenInjectionStep;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.atscale.java.utils.PropertiesFileReader;

import static io.gatling.javaapi.core.OpenInjectionStep.atOnceUsers;

@SuppressWarnings("unused")
public class AtScaleOpenInjectionStepSimulation extends Simulation{
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleOpenInjectionStepSimulation.class);

    public AtScaleOpenInjectionStepSimulation(){
        String model = System.getProperties().getProperty("atscale.model");
        String steps = System.getProperties().getProperty("atscale.gatling.injection.steps");
        String runId = System.getProperties().getProperty("gatling_run_id");
        String runLogFileName = System.getProperties().getProperty("gatling_run_logFileName");
        String loggingAsAppend = System.getProperties().getProperty("gatling_run_logAppend");

        if (model == null || model.isEmpty()) {
            LOGGER.error("AtScale model is not specified. Please set the 'atscale.model' system property.");
            throw new IllegalArgumentException("AtScale model is required.");
        }

        LOGGER.info("Simulation class {} Gatling run ID: {}", this.getClass().getName(), runId);
        LOGGER.info("Using model: {}", model);
        LOGGER.info("Using injection steps: {}", steps);
        LOGGER.info("Using log file name: {}", runLogFileName);
        LOGGER.info("Logging as append: {}", loggingAsAppend);

        String url = PropertiesFileReader.getAtScaleJdbcConnection(model);
        if(StringUtils.isNotEmpty(url) && url.toLowerCase().contains("hive")) {
            //for AtScale Installer Support - ensure Hive JDBC Driver is loaded
            try {
                Class<?> c = Class.forName("org.apache.hive.jdbc.HiveDriver");
                LOGGER.info("Hive JDBC Driver found: {}", c.getName());
            } catch (ClassNotFoundException e) {
                LOGGER.error("Hive JDBC Driver not found in classpath.", e);
            }
        }

        List<com.atscale.java.injectionsteps.OpenStep> openSteps = InjectionStepJsonUtil.openInjectionStepsFromJson(steps);
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

        AtScaleDynamicQueryBuilderScenario scn = new AtScaleDynamicQueryBuilderScenario();
        ScenarioBuilder sb = scn.buildScenario(model, runId);

        if(injectionSteps.isEmpty()) {
            LOGGER.warn("No valid injection steps provided. Defaulting to atOnceUsers(1)");
            injectionSteps.add(atOnceUsers(1));
        }

        setUp(sb.injectOpen(injectionSteps).protocols(JdbcProtocol.forDatabase(model)));
    }
}
