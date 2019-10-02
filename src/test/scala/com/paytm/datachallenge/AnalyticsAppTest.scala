package com.paytm.datachallenge

import com.paytm.datachallenge.Analytics._
import com.paytm.datachallenge.AnalyticsApp._
import com.paytm.datachallenge.Configuration._
import com.paytm.datachallenge.DataSources.readELBLogs
import com.paytm.datachallenge.Preprocessing.sessionize
import org.apache.spark.sql.DataFrame

import scala.util.{ Failure, Success, Try }
import spark.implicits._

/**
  * An AppTest that can run implemented code from [[Analytics]] but with different I/Os
  * (ie. different input file, print to console rather than writing to a table).
  *
  * It can be used as an integration test or simply for visual testing.
  * In the present challenge, it allows the reviewer to run the spark job without
  * having to use spark-submit.
  *
  * Results are printed to the console and outputed as CSV in defined output directory
  * from [[Configuration.outputPath]].
  *
  */
object AnalyticsAppTest extends App with SparkLocal with Logging {

  /**
    * Function that call values and functions implemented in source.
    * In this case, we use the same I/Os except that we also print results to console.
    * We also could have print the schema, explain the execution plan, or even compose those together.
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
    * Prints a dataframe schema to the console
    */
  val printSchema: DataFrame => DataFrame = df => { df.printSchema(); df }

  /**
    * Prints content of a dataframe to the console
    */
  val printToConsole: DataFrame => Unit = df => df.show(25, truncate = false)

  /**
    * Prints the execution plan to the console
    */
  val explainPlan: DataFrame => DataFrame = df => { df.explain(extended = true); df }

  /**
    * Run the test application.
    */
  AnalyticsAppTest.run()

}
