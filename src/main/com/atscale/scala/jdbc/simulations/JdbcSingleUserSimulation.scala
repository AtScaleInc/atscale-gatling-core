package com.atscale.scala.jdbc.simulations

import com.atscale.scala.jdbc.scenarios.JdbcBasicScenario
import io.gatling.core.Predef._
import org.galaxio.gatling.jdbc.Predef._
import io.gatling.core.structure.ScenarioBuilder
import org.galaxio.gatling.jdbc.protocol.JdbcProtocolBuilder


class JdbcSingleUserSimulation extends Simulation{

  val scenario: ScenarioBuilder = JdbcBasicScenario.scn()
  val db: JdbcProtocolBuilder = atscale_db

  setUp(
    scenario.inject(atOnceUsers(1)),
  ).protocols(db)
    .maxDuration(60)
    .assertions(
      global.failedRequests.percent.is(0.0),
    )
}
