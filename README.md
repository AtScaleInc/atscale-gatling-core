Overview

Runs Gatling Tests. Project uses Maven to manage compilation, and testing.

The core goal of this project is to fully automate the testing process by pulling queries from the AtScale database where each query is logged.  Then to automate running those queries before and after version upgrades to build confidence that the upgrade will go smoothly in customer environments.

This project was built using temurin-21 open jdk.  It provides Gatling tests for both the AtScale JDBC endpoint and the AtScale XMLA endpoint. 
For additional information on Gatling JDBC see: https://github.com/galax-io/gatling-jdbc-plugin/tree/main/src/test

Gatling is generating an HTML formatted report, which can be found under: target/gatling.  In the report, Gatling test results are categorized as OK or KO with KO being the opposite of OK meaning not OK.

Prerequisites

Add a properties file named system.properties to the src/test/resources directory modeled like the example_system.properties file in the same directory.  

This file should contain the following properties:
1. A list of models
2. The JDBC URL to connect to the Atscale Postgres Database where system configuration data and query data is stored.  The database is named atscale.
3. The username and password to connect to the Atscale Postgres Database
4. A set of properties to connect to the AtScale JDBC endpoint for each model.  AtScale emulates Postgres so the JDBC URL looks similar to the AtScale database where system configuration is stored.
5. A set of properties to connect to the AtScale XMLA endpoint for each model.

```
atscale.models=model1,model2,model3
atscale.jdbc.url=jdbc:postgresql://your_host:your_port/atscale
atscale.jdbc.username=your_username
atscale.jdbc.password=your_password
atscale.model1.jdbc.url=jdbc:postgresql://your_host:your_port/your_catalog
atscale.model1.jdbc.username=your_keycloak_username
atscale.model1.jdbc.password=your_keycloak_password
atscale.model1.jdbc.maxPoolSize=10
atscale.model2.jdbc.url=jdbc:postgresql://your_host:your_port/your_catalog
atscale.model2.jdbc.username=your_keycloak_username
atscale.model2.jdbc.password=your_keycloak_password
atscale.model2.jdbc.maxPoolSize=10
atscale.model3.jdbc.url=jdbc:postgresql://your_host:your_port/your_catalog
atscale.model3.jdbc.username=your_keycloak_username
atscale.model3.jdbc.password=yuor_keycloak_password
atscale.model3.jdbc.maxPoolSize=10
atscale.model1.xmla.url=your_atscale_xmla_url_with_access_token
atscale.model1.xmla.cube=cube_name_for_model1
atscale.model1.xmla.catalog=catalog_name_for_model1
```  

Run this command to extract queries from the Atscale database into a files:
```shell
 ./mvnw clean install exec:java -Dexec.mainClass="com.atscale.java.executors.QueryExtractExecutor"
```
There is also a maven goal defined in the pom.xml file.  The same command can be run using:
```shell
 ./mvnw clean install exec:java@query-extract
```
where query-extract is the id of the execution to be run.

For details refer to the pom.xml file and look for:  <artifactId>exec-maven-plugin</artifactId>

If run successfully, there will be two files created in the directory
src/test/resources/queries 
for each model defined in the atscale.models property


The easiest way to run Gatling tests is to create an Executor under src/test/java/com/atscale/java/executors.  The project includes open and closed step executors.  These classes run Gatling Simulations using open steps or closed steps.  Simulations can be run using one of the following commands:
```shell
 ./mvnw clean install exec:java@open-step-simulation-executor 
````
```shell
 ./mvnw clean install exec:java@closed-step-simulation-executor 
````
or
```shell
 ./mvnw clean install exec:java -Dexec.mainClass="com.atscale.java.executors.OpenStepSimulationExecutor"
```

SimulationExecutor can be extended to run custom Gatling simulations.  For example:

```java
package com.atscale.java.executors;

import java.util.ArrayList;
import java.util.List;


public class CustomSimulationExecutor extends OpenStepSimulationExecutor {

    public static void main(String[] args) {
        CustomSimulationExecutor executor = new CustomSimulationExecutor();
        executor.execute();
    }

    @Override
    protected List<MavenTaskDto> getSimulationTasks() {
        // Custom execution logic can be added here
        System.out.println("Executing custom simulation with task");

        List<MavenTaskDto> tasks = new ArrayList<>();

        MavenTaskDto task1 = new MavenTaskDto("XMLA Ten User Simulation");
        tasks.add(task1);
        task1.setMavenCommand("gatling:test");
        task1.setSimulationClass("com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation");
        task1.setRunDescription("Internet Sales XMLA Model Tests");
        task1.addGatlingProperty("atscale.model", "internet_sales");

        return tasks;
    }
}
```
Consider modification to the maven pom.xml file to add a new goal for the custom simulation executor.

As the Executors are directly runnable they're also a good way to run a debugger to step through the code.

Can clean, build and run an individual test as follows
Java:
```shell
 ./mvnw clean install & ./mvnw gatling:test -Dgatling.simulationClass=com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation  -Dgatling.runDescription="Internet Sales Model Test" -Datscale.model="internet_sales"
```
Scala:
```shell
 ./mvnw clean install & ./mvnw gatling:test -Dgatling.simulationClass=com.atscale.scala.jdbc.simulations.JdbcSingleUserSimulation  -Dgatling.runDescription="Internet Sales Model Tests" -Datscale.model="internet_sales"
```


Gatling provides extensive capabilities to shape our tests.  For instance, we can simulate various numbers of concurrent users ramping up load and ramping down load over time.  These capabilities are defined in Gatling simulation classes. It's assumed users of this utility will shape their tests by writing custom simulations and adding them to a SimulationExecutor. The Gatling documentation provides extensive information on how to use these capabilities.  See: https://gatling.io/docs/gatling/reference/current/general/simulation_structure/