package com.atscale.scala.jdbc.scenarios

import com.atscale.scala.jdbc.cases.JdbcActions
import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import scala.concurrent.duration._

object JdbcBasicScenario {

  def scn(): ScenarioBuilder = scenario("Basic")
    .exec(JdbcActions.selectTest)

  def scnForever(description: String): ScenarioBuilder = scenario(description)
    .forever(
      exec(JdbcActions.selectTest).pause(100.millisecond),
    )

}
