package com.atscale.java.executors;

import com.atscale.java.injectionsteps.ClosedStep;
import com.atscale.java.injectionsteps.ConstantConcurrentUsersClosedInjectionStep;
import com.atscale.java.injectionsteps.RampConcurrentUsersClosedInjectionStep;
import com.atscale.java.utils.MavenTaskJsonUtil;
import com.atscale.java.utils.MavenTaskYamlUtil;

import java.util.ArrayList;
import java.util.List;

public class ClosedStepTaskFactory {

    public static void main(String[] args) {
        if(args.length == 0) {
            MavenTaskJsonUtil.writeClosedStepTasksToFile("example_closedtasks.json", ClosedStepTaskFactory.getTasks());
            MavenTaskYamlUtil.writeClosedStepTasksToFile("example_closedtasks.yaml", ClosedStepTaskFactory.getTasks());
        } else if (args.length == 1 && args[0].equalsIgnoreCase("json")) {
            MavenTaskJsonUtil.writeClosedStepTasksToFile("example_closedtasks.json", ClosedStepTaskFactory.getTasks());
        } else if (args.length == 1 && args[0].equalsIgnoreCase("yaml")) {
            MavenTaskYamlUtil.writeClosedStepTasksToFile("example_closedtasks.yaml", ClosedStepTaskFactory.getTasks());
        }
    }

    public static List<MavenTaskDto<ClosedStep>> getTasks() {
        List<MavenTaskDto<ClosedStep>> tasks = new ArrayList<>();

        List<ClosedStep> t1InjectionSteps = new ArrayList<>();
        t1InjectionSteps.add(new ConstantConcurrentUsersClosedInjectionStep(1, 1));

        List<ClosedStep> t2InjectionSteps = new ArrayList<>();
        t2InjectionSteps.add(new ConstantConcurrentUsersClosedInjectionStep(1, 1));


        List<ClosedStep> t3InjectionSteps = new ArrayList<>();
        t3InjectionSteps.add(new RampConcurrentUsersClosedInjectionStep(1,3, 2));


        // Three example tasks for the Container Version. Uncomment tasks.add as needed.
        MavenTaskDto<ClosedStep> task1 = new MavenTaskDto<>("Internet Sales XMLA Simulation");
        tasks.add(task1);
        task1.setMavenCommand("gatling:test");
        task1.setRunLogFileName("internet_sales_xmla.log");
        task1.setLoggingAsAppend(true);
        task1.setSimulationClass("com.atscale.java.xmla.simulations.AtScaleXmlaClosedInjectionStepSimulation");
        task1.setRunDescription("Internet Sales XMLA Model Tests");
        task1.setModel( "internet_sales");
        task1.setInjectionSteps(t1InjectionSteps);
        task1.setIngestionFileName("internet_sales_xmla_queries.csv", true);

        MavenTaskDto<ClosedStep> task2 = new MavenTaskDto<>("Internet Sales JDBC Simulation");
        tasks.add(task2);
        task2.setMavenCommand("gatling:test");
        task2.setRunLogFileName("internet_sales_jdbc.log");
        task2.setLoggingAsAppend(true);
        task2.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation");
        task2.setRunDescription("Internet Sales JDBC Model Tests");
        task2.setModel("internet_sales");
        task2.setInjectionSteps(t2InjectionSteps);

        MavenTaskDto<ClosedStep> task3 = new MavenTaskDto<>("TPC-DS JDBC Simulation");
        tasks.add(task3);
        task3.setMavenCommand("gatling:test");
        task3.setRunLogFileName("tpcds_benchmark_jdbc.log");
        task3.setLoggingAsAppend(true);
        task3.setSimulationClass("com.atscale.java.jdbc.simulations.AtScaleClosedInjectionStepSimulation");
        task3.setRunDescription("TPCDS JDBC Model Tests");
        task3.setModel("tpcds_benchmark_model");
        task3.setInjectionSteps(t3InjectionSteps);
        task3.setIngestionFileName("tpcds_benchmark_jdbc_queries.csv", true);

        return tasks;
    }
}
