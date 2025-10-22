package com.atscale.java.xmla.simulations;

import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.utils.InjectionStepJsonUtil;
import com.atscale.java.xmla.XmlaProtocol;
import io.gatling.javaapi.core.ClosedInjectionStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import static io.gatling.javaapi.core.CoreDsl.constantConcurrentUsers;


@SuppressWarnings("unused")
public class AtScaleXmlaClosedInjectionStepSimulation extends AtScaleXmlaSimulation {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleXmlaClosedInjectionStepSimulation.class);

    public AtScaleXmlaClosedInjectionStepSimulation() {
        super();
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

        setUp(sb.injectClosed(injectionSteps).protocols(XmlaProtocol.forXmla(model)));
    }
}
