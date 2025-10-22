package com.atscale.java.jdbc.simulations;

import com.atscale.java.jdbc.JdbcProtocol;
import com.atscale.java.utils.InjectionStepJsonUtil;
import io.gatling.javaapi.core.OpenInjectionStep;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.gatling.javaapi.core.OpenInjectionStep.atOnceUsers;

@SuppressWarnings("unused")
public class AtScaleOpenInjectionStepSimulation extends AtScaleSimulation{
    private static final Logger LOGGER = LoggerFactory.getLogger(AtScaleOpenInjectionStepSimulation.class);

    
    //Full Maven command: mvnw.cmd -Dgatling.simulationClass=com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation -Dgatling.runDescription=VFBDRFMgWE1MQSBNb2RlbCBUZXN0cw== -Datscale.model=VFBDLURTIEJlbmNobWFyayBNb2RlbA== -Datscale.run.id=MjAyNS0wOS0yMi1QSWdka3VRcjhD -Datscale.log.fileName=dHBjZHNfYmVuY2htYXJrX3htbGEubG9n -Dgatling_run_logAppend=ZmFsc2U= gatling:test

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

        setUp(sb.injectOpen(injectionSteps).protocols(JdbcProtocol.forDatabase(model)));
    }
}
