package com.paytm.datachallenge

import com.paytm.datachallenge.Analytics._
import com.paytm.datachallenge.AnalyticsApp._
import com.paytm.datachallenge.Configuration._
import com.paytm.datachallenge.DataSources.readELBLogs
import com.paytm.datachallenge.Preprocessing.sessionize
import org.apache.spark.sql.DataFrame

import scala.util.{ Failure, Success, Try }
import spark.implicits._

object AnalyticsAppTest extends App with SparkLocal with Logging {

  /**
    *
    */
  def run(): Unit = {
    val logs            = readELBLogs(elbLogsPath)(spark)
    val sessionizedLogs = sessionize(logs, sessionTimeout).cache()
    analyticsTasks(sessionizedLogs).par.foreach {
      case (taskOP, taskName) =>
        Try(writeResults(taskOP, outputPath.concat(taskName))) match {
          case Success(_)         => logger.info(s"Task ${taskName} successfully completed.")
          case Failure(exception) => logger.error(s"Task ${taskName} failed with error message ${exception.getMessage}")
        }
        printToConsole(taskOP)
    }
  }

  /**
    *
    */
  val printSchema: DataFrame => DataFrame = df => { df.printSchema(); df }

  /**
    *
    */
  val printToConsole: DataFrame => Unit = df => df.show(25, truncate = false)

  /**
    *
    */
  val explainPlan: DataFrame => DataFrame = df => { df.explain(extended = true); df }

  /**
    *
    */
  AnalyticsAppTest.run()

}
