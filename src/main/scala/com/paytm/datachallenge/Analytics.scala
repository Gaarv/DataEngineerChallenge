package com.paytm.datachallenge

import com.paytm.datachallenge.AnalyticsApp._
import com.paytm.datachallenge.Configuration._
import com.paytm.datachallenge.DataSources.readELBLogs
import com.paytm.datachallenge.Preprocessing.sessionize
import org.apache.spark.sql.{ DataFrame, Dataset, SaveMode, functions => F }

import scala.util.{ Failure, Success, Try }

object Analytics extends Logging {

  /**
    *  This is the entry function that is called from [[AnalyticsApp.main()]].
    *  Allow to have a main function that only parse arguments or configuration
    *  and run specific job based on pattern matching.
    *
    *  Tasks are defined using expressions and can be combined / composed.
    *  Tasks do not include I/O operations so:
    *    1. they can be tested easily
    *    2. different I/Os can be used in tests
    *
    */
  def run(): Unit = {

    /**
      * sessionized logs are used in all tasks so we only read the raw file once,
      * sessionize logs with [[Preprocessing.sessionize()]] and persist to memory.
      */
    val logs            = readELBLogs(elbLogsPath)(spark)
    val sessionizedLogs = sessionize(logs, sessionTimeout).cache()

    /**
      * We run tasks defined in [[analyticsTasks]] in parallel
      */
    analyticsTasks(sessionizedLogs).par.foreach {
      case (taskOP, taskName) =>
        Try(writeResults(taskOP, outputPath.concat(taskName))) match {
          case Success(_)         => logger.info(s"Task ${taskName} successfully completed.")
          case Failure(exception) => logger.error(s"Task ${taskName} failed with error message ${exception.getMessage}")
        }
    }
  }

  /**
    * I/O operation that write a dataframe as a CSV output to a given path
    *
    * @param df a Dataframe
    * @param path a string representing a path where to write csv output
    */
  def writeResults(df: DataFrame, path: String): Unit =
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path)

  /**
    * Task that takes a Dataset of [[Session]] and return a Dataframe.
    * The urls column is dropped as it's not useful in this output,
    * ordering is optional, but useful to those who would be using this result,
    * like users doing analytics from a Hive table.
    *
    */
  val sessionizeLogs: Dataset[Session] => DataFrame = sessions =>
    sessions
      .drop(F.col("urls"))
      .orderBy(F.col("request_ip"), F.col("user_agent"), F.col("start_timestamp"))

  /**
    * Task that takes a Dataset of [[Session]] and return a Dataframe.
    * Report the number of sessions and the average session time in minutes from those sessions
    */
  val averageSessionTime: Dataset[Session] => DataFrame = sessions =>
    sessions.select(
      F.count("duration").as("sessions"),
      (F.avg(F.col("duration")) / 60).as("average_session_time_in_minutes")
    )

  /**
    * Task that takes a Dataset of [[Session]] and return a Dataframe.
    * Report session_id and unique urls count associated to each session_id
    * by descending order
    */
  val uniqueUrlsPerSession: Dataset[Session] => DataFrame = sessions =>
    sessions
      .withColumn("url", F.explode(F.col("urls")))
      .groupBy(F.col("session_id"))
      .agg(F.countDistinct(F.col("url")).as("unique_urls_count"))
      .select(F.col("session_id"), F.col("unique_urls_count"))
      .orderBy(F.col("unique_urls_count").desc)

  /**
    * Task that takes a Dataset of [[Session]] and return a Dataframe.
    * Report user_id and total duration in seconds of all sessions associated to
    * that user_id by descending order, limited to top 100.
    */
  val mostEngagedUsersBySessionDuration: Dataset[Session] => DataFrame = sessions =>
    sessions
      .groupBy(F.col("user_id"))
      .agg(F.sum(F.col("duration")).as("total_duration_seconds"))
      .orderBy(F.col("total_duration_seconds").desc)
      .limit(100)

  /**
    * Task that takes a Dataset of [[Session]] and return a Dataframe.
    * Report user_id and total unique urls count associated to that user_id
    * across all sessions from that user_id by descending order;
    * limited to top 100.
    */
  val mostEngagedUsersByUniqueUrlsCount: Dataset[Session] => DataFrame = sessions =>
    sessions
      .withColumn("url", F.explode(F.col("urls")))
      .groupBy(F.col("user_id"))
      .agg(F.countDistinct(F.col("url")).as("unique_urls_count"))
      .groupBy(F.col("user_id"))
      .agg(F.sum(F.col("unique_urls_count")).as("total_unique_urls_count"))
      .select(F.col("user_id"), F.col("total_unique_urls_count"))
      .orderBy(F.col("total_unique_urls_count").desc)
      .limit(100)

  /**
    * A list that group all tasks to run, alone with a task name that will be used
    * for output directory name containing results.
    *
    * Nothing is run at this point since lazily evaluated.
    * Tasks are actually run in [[Analytics.run()]].
    *
    */
  val analyticsTasks: Dataset[Session] => List[(DataFrame, String)] = logs =>
    List(
      (sessionizeLogs(logs), "sessionize-logs-by-ip"),
      (averageSessionTime(logs), "average-session-time"),
      (uniqueUrlsPerSession(logs), "unique-urls-per-session"),
      (mostEngagedUsersBySessionDuration(logs), "most-engaged-users-by-session-duration"),
      (mostEngagedUsersByUniqueUrlsCount(logs), "most-engaged-users-by-unique-urls-count")
    )

}
