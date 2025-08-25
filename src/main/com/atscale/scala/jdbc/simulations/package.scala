package com.atscale.scala.jdbc

import org.galaxio.gatling.jdbc.Predef._
import org.galaxio.gatling.jdbc.protocol._
import scala.concurrent.duration._
import com.atscale.java.utils.PropertiesFileReader

package object simulations {
  val model = "internet_sales"
  val atscale_db: JdbcProtocolBuilder = DB
    .url(PropertiesFileReader.getAtScaleJdbcConnection(model))
    .username(PropertiesFileReader.getAtScaleJdbcUserName(model))
    .password(PropertiesFileReader.getAtScaleJdbcPassword(model))
    .maximumPoolSize(50)
    .connectionTimeout(2.minute)
}
