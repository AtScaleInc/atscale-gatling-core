package com.atscale.java.jdbc.simulations;

import com.atscale.java.jdbc.scenarios.AtScaleDynamicQueryBuilderScenario;
import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.jdbc.JdbcProtocol;
import com.atscale.java.utils.InjectionStepJsonUtil;
import com.atscale.java.utils.PropertiesFileReader;
import io.gatling.javaapi.core.ClosedInjectionStep;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import static io.gatling.javaapi.core.CoreDsl.constantConcurrentUsers;

@SuppressWarnings("unused")
public class AtScaleClosedInjectionStepSimulation extends Simulation{
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleClosedInjectionStepSimulation.class);

    public AtScaleClosedInjectionStepSimulation(){
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

        List<ClosedStep> closedSteps = InjectionStepJsonUtil.closedInjectionStepsFromJson(steps);
        List<ClosedInjectionStep> injectionSteps = new ArrayList<>();
        for (ClosedStep step : closedSteps) {
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

        AtScaleDynamicQueryBuilderScenario scn = new AtScaleDynamicQueryBuilderScenario();
        ScenarioBuilder sb = scn.buildScenario(model, runId);

        if(injectionSteps.isEmpty()) {
            LOGGER.warn("No valid injection steps provided. Defaulting to constantConcurrentUsers(1) for 1 minute");
            injectionSteps.add(constantConcurrentUsers(1).during(Duration.ofMinutes(1))); // Default to 1 user for 1 minute
        }

        setUp(sb.injectClosed(injectionSteps).protocols(JdbcProtocol.forDatabase(model)));
    }
}
