package com.paytm.datachallenge

import com.paytm.datachallenge.AnalyticsApp._
import com.paytm.datachallenge.Configuration._
import com.paytm.datachallenge.DataSources.readELBLogs
import com.paytm.datachallenge.Preprocessing.sessionize
import org.apache.spark.sql.{ DataFrame, Dataset, SaveMode, functions => F }

import scala.util.{ Failure, Success, Try }

object Analytics extends Logging {

  /**
    *
    */
  def run(): Unit = {
    val logs            = readELBLogs(elbLogsPath)(spark)
    val sessionizedLogs = sessionize(logs, sessionTimeout).cache()
    analyticsTasks(sessionizedLogs).par.foreach { task =>
      Try(writeResults(task._1, outputPath.concat(task._2))) match {
        case Success(_)         => logger.info(s"Task $task successfully completed.")
        case Failure(exception) => logger.error(s"Task $task failed with error message ${exception.getMessage}")
      }
    }
  }

  /**
    *
    * @param df
    * @param path
    */
  def writeResults(df: DataFrame, path: String): Unit =
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path)

  /**
    *
    */
  val sessionizeLogs: Dataset[Session] => DataFrame = sessions => sessions.drop(F.col("urls"))

  /**
    *
    */
  val averageSessionTime: Dataset[Session] => DataFrame = sessions =>
    sessions.select(
      F.count("duration").as("sessions"),
      (F.avg(F.col("duration")) / 60).as("average_session_time_in_minutes")
    )

  val uniqueUrlsPerSession: Dataset[Session] => DataFrame = sessions =>
    sessions
      .withColumn("url", F.explode(F.col("urls")))
      .groupBy(F.col("session_id"))
      .agg(F.countDistinct(F.col("url")).as("unique_urls_count"))
      .select(F.col("session_id"), F.col("unique_urls_count"))

  //  val mostEngagedUsers: Dataset[Session] => DataFrame = ???

  /**
    *
    */
  val analyticsTasks: Dataset[Session] => List[(DataFrame, String)] = logs =>
    List(
      (sessionizeLogs(logs), "sessionize-logs-by-ip.csv"),
      (averageSessionTime(logs), "average-session-time.csv"),
      (uniqueUrlsPerSession(logs), "unique-urls-per-session.csv")
    )

}
