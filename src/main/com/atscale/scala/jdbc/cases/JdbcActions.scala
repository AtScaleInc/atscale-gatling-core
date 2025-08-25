package com.atscale.scala.jdbc.cases

import io.gatling.core.Predef._
import org.galaxio.gatling.jdbc.Predef._
import org.galaxio.gatling.jdbc.actions.actions._


object JdbcActions {

    def selectTest: QueryActionBuilder =
      jdbc("Select Test")
        .query(
          """
            |SELECT CAST("internet_sales"."Product Line" AS TEXT) AS "Product Line",
            | SUM("internet_sales"."Order Quantity") AS "sum:Order Quantity:ok"
            | FROM "internet_sales_catalog_Databricks"."internet_sales" "internet_sales"
            | GROUP BY 1
            """.stripMargin)
        .check(
          simpleCheck (x => x.nonEmpty),
          allResults.saveAs("R"),
        )
}
