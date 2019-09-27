package com.paytm.datachallenge.preprocessing

import com.paytm.datachallenge.configuration.Configuration._
import com.paytm.datachallenge.logs.ELBLog
import org.apache.spark.sql.{Column, DataFrame, Dataset, RelationalGroupedDataset, functions => F}

object SessionizeLogs {

  val timeWindow: Column            = F.window(F.col("timestamp"), sessionWindowDuration).as("session")
  val dimensions: List[Column]      = List("request_ip").map(F.col)
//  val aggregations: List[Column]    = List("url").map(s => F.count(F.col(s)).as(s.concat("_count")))
//  val ordering: List[Column]        = List("request_ip").map(F.col).map(_.asc)
//  val selectedColumns: List[Column] = List("request_ip", "start_session", "end_session", "url_count").map(F.col)

  def fromIPs(ds: Dataset[ELBLog]): RelationalGroupedDataset = ds.groupBy(dimensions :+ timeWindow: _*)
//      .agg(aggregations.head, aggregations.tail: _*)
//      .orderBy(ordering: _*)
//      .withColumn("start_session", F.col("session").getField("start"))
//      .withColumn("end_session", F.col("session").getField("end"))
//      .select(selectedColumns: _*)
}
