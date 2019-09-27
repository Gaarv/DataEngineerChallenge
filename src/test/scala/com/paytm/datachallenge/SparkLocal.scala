package com.paytm.datachallenge

import org.apache.spark.sql.SparkSession

trait SparkLocal {

  private val master = "local[*]"
  private val appName = "data-engineer-challenge"

  val spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .getOrCreate()

}
