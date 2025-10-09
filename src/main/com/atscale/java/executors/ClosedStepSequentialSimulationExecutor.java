package com.atscale.java.executors;

import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.injectionsteps.ConstantConcurrentUsersClosedInjectionStep;
import com.atscale.java.injectionsteps.IncrementConcurrentUsersClosedInjectionStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ClosedStepSequentialSimulationExecutor extends SequentialSimulationExecutor<ClosedStep> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClosedStepSequentialSimulationExecutor.class);

    public static void main(String[] args) {
        LOGGER.info("SequentialSimulationExecutor started.");

        ClosedStepSequentialSimulationExecutor executor = new ClosedStepSequentialSimulationExecutor();
        executor.execute();
        LOGGER.info("SequentialSimulationExecutor completed.");
    }

    @Override
    protected List<MavenTaskDto<ClosedStep>> getSimulationTasks() {
        List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();

        List<ClosedStep> t1InjectionSteps = new ArrayList<>();
        t1InjectionSteps.add(new IncrementConcurrentUsersClosedInjectionStep(1, 3, 1,1,1));

        List<ClosedStep> t2InjectionSteps = new ArrayList<>();
        t2InjectionSteps.add(new IncrementConcurrentUsersClosedInjectionStep(1, 3,1,1, 1));

        List<ClosedStep> t3InjectionSteps = new ArrayList<>();
        t3InjectionSteps.add(new IncrementConcurrentUsersClosedInjectionStep(1, 3, 1, 1, 1));

        List<ClosedStep> constantUsersInjectionSteps = new ArrayList<>();
        constantUsersInjectionSteps.add(new ConstantConcurrentUsersClosedInjectionStep(1,1));

        MavenTaskDto<ClosedStep> task1 = new MavenTaskDto<> ("Internet Sales XMLA Stepped User Simulation");
        tasks.add(task1);
        task1.setMavenCommand("gatling:test");
        task1.setRunId("Helter Skelter");
        task1.setRunLogFileName("a.log");
        task1.setLoggingAsAppend(true);
        task1.setSimulationClass("com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation");
        task1.setRunDescription("Internet Sales XMLA Model Tests");
        task1.setModel( "internet_sales");
        task1.setInjectionSteps(t1InjectionSteps);
        //task1.setIngestionFileName("internet_sales_xmla_queries.csv", true);


        MavenTaskDto<ClosedStep> task2 = new MavenTaskDto<>("Internet Sales JDBC User Simulation");
        //tasks.add(task2);
        task2.setMavenCommand("gatling:test");
        task2.setRunId("Gimme Shelter");
        task2.setRunLogFileName("b.log");
        task2.setLoggingAsAppend(false);
        task2.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation");
        task2.setRunDescription("Internet Sales JDBC Model Tests");
        task2.setModel( "internet_sales");
        task2.setInjectionSteps(t2InjectionSteps);

        MavenTaskDto<ClosedStep> task3 = new MavenTaskDto<>("TPC-DS JDBC Stepped User Simulation");
        //tasks.add(task3);
        task3.setMavenCommand("gatling:test");
        task3.setRunLogFileName("c.log");
        task3.setLoggingAsAppend(true);
        task3.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation");
        task3.setRunDescription("TPCDS JDBC Model Tests");
        task3.setModel("tpcds_benchmark_model");
        task3.setInjectionSteps(t3InjectionSteps);

         // Two example tasks for the Installer Version. Exclude by removing tasks.add as needed.
        MavenTaskDto<ClosedStep> task4 = new MavenTaskDto<>("Installer TPC-DS JDBC Simulation");
        //tasks.add(task4);
        task4.setMavenCommand("gatling:test");
        task4.setRunLogFileName("tpcds_benchmark_hive.log");
        task4.setLoggingAsAppend(false);
        task4.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation");
        task4.setRunDescription("TPCDS JDBC Model Tests");
        task4.setModel("TPC-DS Benchmark Model");
        task4.setInjectionSteps(constantUsersInjectionSteps);
          

        MavenTaskDto<ClosedStep> task5 = new MavenTaskDto<>("Installer TPC-DS XMLA Simulation");
        //tasks.add(task5);
        task5.setMavenCommand("gatling:test");
        task5.setRunLogFileName("tpcds_benchmark_xmla.log");
        task5.setLoggingAsAppend(false);
        task5.setSimulationClass("com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation");
        task5.setRunDescription("TPCDS XMLA Model Tests");
        task5.setModel("TPC-DS Benchmark Model");
        task5.setInjectionSteps(constantUsersInjectionSteps);

        return tasks;
    }
}
