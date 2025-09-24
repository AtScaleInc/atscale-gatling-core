package com.atscale.java.executors;

import com.atscale.java.injectionsteps.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

public class OpenStepSimulationExecutor extends SimulationExecutor<OpenStep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenStepSimulationExecutor.class);

    public static void main(String[] args) {
        LOGGER.info("SimulationExecutor started.");

        OpenStepSimulationExecutor executor = new OpenStepSimulationExecutor();
        executor.execute();
        LOGGER.info("SimulationExecutor completed.");
    }

    protected List<MavenTaskDto<OpenStep>> getSimulationTasks() {
        List<MavenTaskDto<OpenStep>> tasks = new ArrayList<>();

        List<OpenStep> t1InjectionSteps = new ArrayList<>();
        t1InjectionSteps.add(new AtOnceUsersOpenInjectionStep(1));

        List<OpenStep> t2InjectionSteps = new ArrayList<>();
        t2InjectionSteps.add(new AtOnceUsersOpenInjectionStep(1));

        List<OpenStep> t3InjectionSteps = new ArrayList<>();
        t3InjectionSteps.add(new AtOnceUsersOpenInjectionStep(1));
        //t3InjectionSteps.add(new RampUsersPerSecOpenInjectionStep(1, 5, 1));

        List<OpenStep> atOnceInjectionSteps = new ArrayList<>();
        atOnceInjectionSteps.add(new AtOnceUsersOpenInjectionStep(1));


        // Three example tasks for the Container Version. Uncomment tasks.add as needed.
        MavenTaskDto<OpenStep> task1 = new MavenTaskDto<OpenStep>("Internet Sales XMLA Simulation");
        tasks.add(task1);
        task1.setMavenCommand("gatling:test");
        task1.setRunLogFileName("internet_sales_xmla.log");
        task1.setLoggingAsAppend(true);
        task1.setSimulationClass("com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation");
        task1.setRunDescription("Internet Sales XMLA Model Tests");
        task1.setModel( "internet_sales");
        task1.setInjectionSteps(t1InjectionSteps);

        MavenTaskDto<OpenStep> task2 = new MavenTaskDto<OpenStep>("Internet Sales JDBC Simulation");
        tasks.add(task2);
        task2.setMavenCommand("gatling:test");
        task2.setRunLogFileName("internet_sales_jdbc.log");
        task2.setLoggingAsAppend(true);
        task2.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation");
        task2.setRunDescription("Internet Sales JDBC Model Tests");
        task2.setModel("internet_sales");
        task2.setInjectionSteps(t2InjectionSteps);

        MavenTaskDto<OpenStep> task3 = new MavenTaskDto<OpenStep>("TPC-DS JDBC Simulation");
        tasks.add(task3);
        task3.setMavenCommand("gatling:test");
        task3.setRunLogFileName("tpcds_benchmark_jdbc.log");
        task3.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation");
        task3.setRunDescription("TPCDS JDBC Model Tests");
        task3.setModel("tpcds_benchmark_model");
        task3.setInjectionSteps(t3InjectionSteps);
        
        // Two example tasks for the Installer Version. Exclude by removing tasks.add as needed.
        MavenTaskDto<OpenStep> task4 = new MavenTaskDto<OpenStep>("Installer TPC-DS JDBC Simulation");
        //tasks.add(task4);
        task4.setMavenCommand("gatling:test");
        task4.setRunLogFileName("tpcds_benchmark_hive.log");
        task4.setLoggingAsAppend(false);
        task4.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation");
        task4.setRunDescription("TPCDS JDBC Model Tests");
        task4.setModel("TPC-DS Benchmark Model");
        task4.setInjectionSteps(atOnceInjectionSteps);
          

        MavenTaskDto<OpenStep> task5 = new MavenTaskDto<OpenStep>("Installer TPC-DS XMLA Simulation");
        //tasks.add(task5);
        task5.setMavenCommand("gatling:test");
        task5.setRunLogFileName("tpcds_benchmark_xmla.log");
        task5.setLoggingAsAppend(false);
        task5.setSimulationClass("com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation");
        task5.setRunDescription("TPCDS XMLA Model Tests");
        task5.setModel("TPC-DS Benchmark Model");
        task5.setInjectionSteps(atOnceInjectionSteps);

        return tasks;
    }
}
