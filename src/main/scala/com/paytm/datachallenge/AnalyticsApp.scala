package com.paytm.datachallenge

object AnalyticsApp extends SparkApplication {

  def main(args: Array[String]): Unit = args(0) match {
    case _ => Analytics.run()
  }

}
