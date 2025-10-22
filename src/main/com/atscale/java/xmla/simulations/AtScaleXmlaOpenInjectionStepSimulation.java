package com.atscale.java.xmla.simulations;

import com.atscale.java.injectionsteps.OpenStep;
import com.atscale.java.utils.InjectionStepJsonUtil;
import com.atscale.java.xmla.XmlaProtocol;
import io.gatling.javaapi.core.OpenInjectionStep;
import static io.gatling.javaapi.core.OpenInjectionStep.atOnceUsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class AtScaleXmlaOpenInjectionStepSimulation extends AtScaleXmlaSimulation {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleXmlaOpenInjectionStepSimulation.class);

    public AtScaleXmlaOpenInjectionStepSimulation() {
        super();

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

        setUp(sb.injectOpen(injectionSteps).protocols(XmlaProtocol.forXmla(model)));
    }
}
