package com.paytm.datachallenge

import org.apache.spark.sql.SparkSession

trait SparkApplication {

  private val appName = "data-engineer-challenge"

  val spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .getOrCreate()

}
