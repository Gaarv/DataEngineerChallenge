package com.paytm.datachallenge.preprocessing

import java.sql.Timestamp
import java.time.LocalDateTime

import com.paytm.datachallenge.logs.ELBLog
import org.apache.spark.sql.expressions.{ Window, WindowSpec }
import org.apache.spark.sql.{ Column, DataFrame, Dataset, functions => F }
import org.apache.spark.storage.StorageLevel

object Preprocessing {

  val duration: Int                = 15 * 60 // 15 minutes
  val default_timestamp: Timestamp = Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0))

  val window: WindowSpec = Window.orderBy(List("request_ip", "user_agent", "timestamp").map(F.col): _*)
  val session_expression: Column = F.coalesce(
    F.when(
      F.col("request_ip") === F.col("previous_request_ip") &&
      F.col("user_agent") === F.col("previous_user_agent") &&
      F.unix_timestamp(F.col("previous_timestamp")) - F.unix_timestamp(F.col("timestamp")) <= duration,
      F.lag("session_id", 1, 0).over(window)
    ),
    F.col("session_id")
  )

  def sessionizeLogs(ds: Dataset[ELBLog]): DataFrame = {
    val df = ds.persist(StorageLevel.DISK_ONLY)
    df.withColumn("session_id", F.monotonically_increasing_id())
      .withColumn("previous_request_ip", F.coalesce(F.lag("request_ip", 1).over(window), F.col("request_ip")))
      .withColumn("previous_user_agent", F.coalesce(F.lag("user_agent", 1).over(window), F.col("user_agent")))
      .withColumn("previous_timestamp", F.coalesce(F.lag("timestamp", 1).over(window), F.col("timestamp")))
      .withColumn("session_group_id", session_expression)
  }

}
