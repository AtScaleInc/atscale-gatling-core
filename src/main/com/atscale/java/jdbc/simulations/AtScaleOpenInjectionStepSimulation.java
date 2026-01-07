package com.atscale.java.jdbc.simulations;

import com.atscale.java.jdbc.JdbcProtocol;
import com.atscale.java.jdbc.scenarios.AtScaleDynamicQueryBuilderScenario;
import com.atscale.java.utils.InjectionStepJsonUtil;
import io.gatling.javaapi.core.OpenInjectionStep;
import java.util.List;
import java.util.ArrayList;
import io.gatling.javaapi.core.ScenarioBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.gatling.javaapi.core.OpenInjectionStep.atOnceUsers;

@SuppressWarnings("unused")
public class AtScaleOpenInjectionStepSimulation extends AtScaleSimulation{
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleOpenInjectionStepSimulation.class);

    public AtScaleOpenInjectionStepSimulation(){
        super();

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

        if(injectionSteps.isEmpty()) {
            LOGGER.warn("No valid injection steps provided. Defaulting to atOnceUsers(1)");
            injectionSteps.add(atOnceUsers(1));
        }

        AtScaleDynamicQueryBuilderScenario scn = new AtScaleDynamicQueryBuilderScenario();
        ScenarioBuilder sb = scn.buildScenario(model, runId, ingestionFile, Boolean.parseBoolean(ingestionFileHasHeader));

        setUp(sb.injectOpen(injectionSteps)).protocols(JdbcProtocol.forDatabase(model));
    }
}
