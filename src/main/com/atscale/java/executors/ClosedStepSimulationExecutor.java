package com.atscale.java.executors;

import com.atscale.java.injectionsteps.IncrementConcurrentUsersClosedInjectionStep;
import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.utils.InjectionStepJsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

public class ClosedStepSimulationExecutor extends SimulationExecutor<ClosedStep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClosedStepSimulationExecutor.class);

    public static void main(String[] args) {
        LOGGER.info("SimulationExecutor started.");

        ClosedStepSimulationExecutor executor = new ClosedStepSimulationExecutor();
        executor.execute();
        LOGGER.info("SimulationExecutor completed.");
    }

    protected List<MavenTaskDto> getSimulationTasks() {
        List<MavenTaskDto> tasks = new ArrayList<>();

        List<ClosedStep> t1InjectionSteps = new ArrayList<>();
        t1InjectionSteps.add(new IncrementConcurrentUsersClosedInjectionStep(1, 3, 1,1,1));

        List<ClosedStep> t2InjectionSteps = new ArrayList<>();
        t2InjectionSteps.add(new IncrementConcurrentUsersClosedInjectionStep(1, 3,1,1, 1));

        List<ClosedStep> t3InjectionSteps = new ArrayList<>();
        t3InjectionSteps.add(new IncrementConcurrentUsersClosedInjectionStep(1, 3, 1, 1, 1));


        MavenTaskDto task1 = new MavenTaskDto("Internet Sales XMLA Stepped User Simulation");
        tasks.add(task1);
        task1.setMavenCommand("gatling:test");
        task1.setSimulationClass("com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation");
        task1.setRunDescription("Internet Sales XMLA Model Tests");
        task1.addGatlingProperty("atscale.model", "internet_sales");
        task1.addGatlingProperty("atscale.gatling.injection.steps", injectionStepsAsJson(t1InjectionSteps));

        MavenTaskDto task2 = new MavenTaskDto("Internet Sales JDBC Stepped User Simulation");
        tasks.add(task2);
        task2.setMavenCommand("gatling:test");
        task2.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation");
        task2.setRunDescription("Internet Sales JDBC Model Tests");
        task2.addGatlingProperty("atscale.model", "internet_sales"); // specify the AtScale model to use
        task2.addGatlingProperty("atscale.gatling.injection.steps", injectionStepsAsJson(t2InjectionSteps));

        MavenTaskDto task3 = new MavenTaskDto("TPC-DS JDBC Stepped User Simulation");
        tasks.add(task3);
        task3.setMavenCommand("gatling:test");
        task3.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation");
        task3.setRunDescription("TPCDS JDBC Model Tests");
        task3.addGatlingProperty("atscale.model", "tpcds_benchmark_model");
        task3.addGatlingProperty("atscale.gatling.injection.steps", injectionStepsAsJson(t3InjectionSteps));
        return tasks;
    }

    protected String injectionStepsAsJson(List<ClosedStep> injectionSteps ) {
            return InjectionStepJsonUtil.closedInjectionStepsAsJson(injectionSteps);
    }
}
