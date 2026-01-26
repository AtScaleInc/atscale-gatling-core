package com.atscale.scala.xmla

import io.gatling.app.Gatling

/*
We need this JVM option to avoid IllegalAccessErrors when running Gatling with Java 17+
just paste it into the VM properties for the IDE run configuration
exactly as shown including the starting characters --
--add-opens=java.base/java.lang=ALL-UNNAMED
 */
object GatlingRunner_OpenStepSimulation {

  def main(args: Array[String]): Unit = {
    //Using command args that I grabbed from mvn command line execution in the application.log
    // [./mvnw,
    // -Dgatling.simulationClass=com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation,
    // -Dgatling.runDescription=Internet Sales XMLA Model Tests,
    // -Datscale.model=aW50ZXJuZXRfc2FsZXM=,
    // -Datscale.run.id=MjAyNi0wMS0xMy14eHFFWjRhc2p3,
    // -Dgatling_run_logFileName=internet_sales_xmla.log,
    // -Dgatling_run_logAppend=true,
    // -Datscale.gatling.injection.steps=W3sidXNlcnMiOjEsInR5cGUiOiJBdE9uY2VVc2Vyc09wZW5JbmplY3Rpb25TdGVwIn1d,
    // -Dquery_ingestion_file=aW50ZXJuZXRfc2FsZXNfeG1sYV9xdWVyaWVzLmNzdg==,
    // -Dquery_ingestion_file_has_header=true,
    // -Dadditional_properties=e30=,
    // gatling:test
    // ]

    // emulate the system properties that would be passed via command line
    // from the maven executor to the Simulation
    // Some of these properties are base64 encoded
    System.setProperty("atscale.model", "aW50ZXJuZXRfc2FsZXM=")
    System.setProperty("atscale.run.id", "MjAyNi0wMS0xMy14eHFFWjRhc2p3")
    System.setProperty("gatling_run_logFileName", "internet_sales_xmla.log")
    System.setProperty("gatling_run_logAppend", "true")
    System.setProperty("atscale.gatling.injection.steps", "W3sidXNlcnMiOjEsInR5cGUiOiJBdE9uY2VVc2Vyc09wZW5JbmplY3Rpb25TdGVwIn1d")
    System.setProperty("query_ingestion_file", "aW50ZXJuZXRfc2FsZXNfeG1sYV9xdWVyaWVzLmNzdg==")
    System.setProperty("query_ingestion_file_has_header", "true")
    System.setProperty("additional_properties", "e30=")



    val gatlingArgs = Array(
      "--simulation", "com.atscale.java.xmla.simulations.AtScaleXmlaOpenInjectionStepSimulation", // Selects the specific simulation class
      "--results-folder", "./target/gatling-results", // Specifies where to write reports
      "--binaries-folder", "./target/classes", // Specifies where compiled classes are located
      "--run-description", "Gatling Runner Test Simulation"
      // Add other options as needed
    )


    Gatling.main(gatlingArgs)
  }
}
