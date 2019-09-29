package com.paytm.datachallenge

import com.paytm.datachallenge.configuration.Configuration._
import com.paytm.datachallenge.preprocessing.Preprocessing
import org.apache.spark.sql.DataFrame

import scala.util.{ Failure, Success, Try }

object AnalyticsAppTest extends App with SparkLocal with Logging {

  def run(): Unit =
    analyticsTasks.foreach { task =>
      Try(task) match {
        case Success(_)         => logger.info(s"Task $task successfully completed.")
        case Failure(exception) => logger.error(s"Task $task failed with error message ${exception.getMessage}")
      }
    }

  val printSchema: DataFrame => DataFrame = df => { df.printSchema(); df }

  val printToConsole: DataFrame => Unit = df => df.show(25, truncate = false)

  val explainPlan: DataFrame => DataFrame = df => { df.explain(extended = true); df }

  val analyticsTasks = {
    val logs = DataSources.readELBLogs(elbLogsPath)(spark).cache()
    List(
      printToConsole(Preprocessing.sessionizeLogs(logs))
    )
  }

  AnalyticsAppTest.run()

}
