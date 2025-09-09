package com.atscale.java.executors;

import com.atscale.java.injectionsteps.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import com.atscale.java.utils.InjectionStepJsonUtil;

public class OpenStepSimulationExecutor extends SimulationExecutor<OpenStep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenStepSimulationExecutor.class);

    public static void main(String[] args) {
        LOGGER.info("SimulationExecutor started.");

        OpenStepSimulationExecutor executor = new OpenStepSimulationExecutor();
        executor.execute();
        LOGGER.info("SimulationExecutor completed.");
    }

    protected List<MavenTaskDto> getSimulationTasks() {
        List<MavenTaskDto> tasks = new ArrayList<>();

        List<OpenStep> t1InjectionSteps = new ArrayList<>();
        t1InjectionSteps.add(new AtOnceUsersOpenInjectionStep(2));
        //t1InjectionSteps.add(new NothingForOpenInjectionStep(1));
        //t1InjectionSteps.add(new RampUsersOpenInjectionStep(15, 3));

        List<OpenStep> t2InjectionSteps = new ArrayList<>();
        t2InjectionSteps.add(new AtOnceUsersOpenInjectionStep(2));

        List<OpenStep> t3InjectionSteps = new ArrayList<>();
        t3InjectionSteps.add(new AtOnceUsersOpenInjectionStep(2));
        //t3InjectionSteps.add(new RampUsersPerSecOpenInjectionStep(10, 35, 2));


        MavenTaskDto task1 = new MavenTaskDto("Internet Sales XMLA Ramped User Simulation");
        tasks.add(task1);
        task1.setMavenCommand("gatling:test");
        task1.setSimulationClass("com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation");
        task1.setRunDescription("Internet Sales XMLA Model Tests");
        task1.addGatlingProperty("atscale.model", "internet_sales");
        task1.addGatlingProperty("atscale.gatling.injection.steps", injectionStepsAsJson(t1InjectionSteps));

        MavenTaskDto task2 = new MavenTaskDto("Internet Sales JDBC 10 User Simulation");
        tasks.add(task2);
        task2.setMavenCommand("gatling:test");
        task2.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation");
        task2.setRunDescription("Internet Sales JDBC Model Tests");
        task2.addGatlingProperty("atscale.model", "internet_sales");
        task2.addGatlingProperty("atscale.gatling.injection.steps", injectionStepsAsJson(t2InjectionSteps));

        MavenTaskDto task3 = new MavenTaskDto("TPC-DS JDBC 10 Initial Users Ramping to 35 Simulation");
        tasks.add(task3);
        task3.setMavenCommand("gatling:test");
        task3.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation");
        task3.setRunDescription("TPCDS JDBC Model Tests");
        task3.addGatlingProperty("atscale.model", "tpcds_benchmark_model");
        task3.addGatlingProperty("atscale.gatling.injection.steps", injectionStepsAsJson(t3InjectionSteps));
        return tasks;
    }

    protected String injectionStepsAsJson(List<OpenStep> injectionSteps ) {
            return InjectionStepJsonUtil.openInjectionStepsAsJson(injectionSteps);
    }
}
