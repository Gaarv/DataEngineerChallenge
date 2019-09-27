package com.paytm.datachallenge

import com.paytm.datachallenge.AnalyticsApp._
import com.paytm.datachallenge.configuration.Configuration._
import com.paytm.datachallenge.logs.ELBLog
import com.paytm.datachallenge.preprocessing.SessionizeLogs
import org.apache.spark.sql.{ Column, DataFrame, Dataset, SaveMode, functions => F }

import scala.util.{ Failure, Success, Try }

object Analytics extends Logging {

  def run(): Unit =
    analyticsTasks(DataSources.readELBLogs(elbLogsPath)(spark))
      .foreach { task =>
        Try(task) match {
          case Success(_)         => logger.info(s"Task $task successfully completed.")
          case Failure(exception) => logger.error(s"Task $task failed with error message ${exception.getMessage}")
        }
      }

  def writeResults(df: DataFrame, path: String): Unit =
    df.write
      .mode(SaveMode.Overwrite)
      .csv(path)

  def averageSessionTime(logs: Dataset[ELBLog]): DataFrame = {
    val aggregations: List[Column]    = List("url").map(s => F.count(F.col(s)).as(s.concat("_count")))
    val ordering: List[Column]        = List("request_ip").map(F.col).map(_.asc)
    val selectedColumns: List[Column] = List("request_ip", "start_session", "end_session", "url_count").map(F.col)

    SessionizeLogs
      .fromIPs(logs)
      .agg(aggregations.head, aggregations.tail: _*)
      .orderBy(ordering: _*)
      .withColumn("start_session", F.col("session").getField("start"))
      .withColumn("end_session", F.col("session").getField("end"))
      .select(selectedColumns: _*)
  }
//  val urlVisitsPerSession: Dataset[ELBLog] => DataFrame = ???

//  val mostEngagedUsers: Dataset[ELBLog] => DataFrame = ???

  val analyticsTasks: Dataset[ELBLog] => List[Unit] = logs =>
    List(
      writeResults(averageSessionTime(logs), "average-session-time-by-ip.csv")
    )

}
