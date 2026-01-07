package com.atscale.java.jdbc.simulations;

import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.jdbc.JdbcProtocol;
import com.atscale.java.jdbc.scenarios.AtScaleDynamicQueryBuilderScenario;
import com.atscale.java.utils.InjectionStepJsonUtil;
import io.gatling.javaapi.core.ClosedInjectionStep;
import io.gatling.javaapi.core.ScenarioBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import static io.gatling.javaapi.core.CoreDsl.constantConcurrentUsers;

@SuppressWarnings("unused")
public class AtScaleClosedInjectionStepSimulation extends AtScaleSimulation{
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleClosedInjectionStepSimulation.class);

    public AtScaleClosedInjectionStepSimulation(){
        super();
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

        if(injectionSteps.isEmpty()) {
            LOGGER.warn("No valid injection steps provided. Defaulting to constantConcurrentUsers(1) for 1 minute");
            injectionSteps.add(constantConcurrentUsers(1).during(Duration.ofMinutes(1))); // Default to 1 user for 1 minute
        }

        AtScaleDynamicQueryBuilderScenario scn = new AtScaleDynamicQueryBuilderScenario();
        ScenarioBuilder sb = scn.buildScenario(model, runId, ingestionFile, Boolean.parseBoolean(ingestionFileHasHeader));

        setUp(sb.injectClosed(injectionSteps)).protocols(JdbcProtocol.forDatabase(model));
    }
}
