package com.atscale.scala.jdbc

import com.atscale.java.utils.PropertiesManager
import org.galaxio.gatling.jdbc.Predef._
import org.galaxio.gatling.jdbc.protocol._
import scala.concurrent.duration._

package object simulations {
  val model = "internet_sales"
  val atscale_db: JdbcProtocolBuilder = DB
    .url(PropertiesManager.getAtScaleJdbcConnection(model))
    .username(PropertiesManager.getAtScaleJdbcUserName(model))
    .password(PropertiesManager.getAtScaleJdbcPassword(model))
    .maximumPoolSize(50)
    .connectionTimeout(2.minute)
}
