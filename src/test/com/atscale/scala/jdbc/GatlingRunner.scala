package com.atscale.scala.jdbc

import io.gatling.app.Gatling

/*
We need this JVM option to avoid IllegalAccessErrors when running Gatling with Java 17+
just paste it into the VM properties for the IDE run configuration
exactly as shown including the starting characters --
--add-opens=java.base/java.lang=ALL-UNNAMED
 */
object GatlingRunner {

  def main(args: Array[String]): Unit = {
    //Using command args that I grabbed from mvn command line execution in the application.log
    // [./mvnw,
    // -Dgatling.simulationClass=com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation,
    // -Dgatling.runDescription=TPCDS JDBC Model Tests,
    // -Datscale.model=dHBjZHNfYmVuY2htYXJrX21vZGVs,
    // -Datscale.run.id=MjAyNS0xMi0xNi1HcFk0eG1QZ1lq,
    // -Dgatling_run_logFileName=tpcds_benchmark_jdbc.log,
    // -Dgatling_run_logAppend=false,
    // -Datscale.gatling.injection.steps=W3sidXNlcnMiOjEsInR5cGUiOiJBdE9uY2VVc2Vyc09wZW5JbmplY3Rpb25TdGVwIn1d,
    // -Dquery_ingestion_file=null,
    // -Dquery_ingestion_file_has_header=false,
    // -Dadditional_properties=e30=,
    // gatling:test]

    // emulate the system properties that would be passed via command line
    // from the maven executor to the Simulation
    // Some of these properties are base64 encoded
    System.setProperty("atscale.model", "dHBjZHNfYmVuY2htYXJrX21vZGVs")
    System.setProperty("atscale.run.id", "MjAyNS0xMi0xNi1HcFk0eG1QZ1lq")
    System.setProperty("gatling_run_logFileName", "tpcds_benchmark_jdbc.log")
    System.setProperty("gatling_run_logAppend", "false")
    System.setProperty("atscale.gatling.injection.steps", "W3sidXNlcnMiOjEsInR5cGUiOiJBdE9uY2VVc2Vyc09wZW5JbmplY3Rpb25TdGVwIn1d")
    System.setProperty("query_ingestion_file", "null")
    System.setProperty("query_ingestion_file_has_header", "false")
    System.setProperty("additional_properties", "e30=")



    val gatlingArgs = Array(
      "--simulation", "com.atscale.java.jdbc.simulations.AtScaleOpenInjectionStepSimulation", // Selects the specific simulation class
      "--results-folder", "./target/gatling-results", // Specifies where to write reports
      "--binaries-folder", "./target/classes" // Specifies where compiled classes are located
      // Add other options as needed
    )


    Gatling.main(gatlingArgs)
  }
}
