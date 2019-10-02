package com.paytm.datachallenge

/**
  * In a real case scenario, this object would be a case class populated
  *  with values passed as configuration to main class with pureconfig or
  *  as arguments with scopt. Those could be paths on S3, Hive tables,
  *  application settings suchs as timeouts, etc.
  *
  */
object Configuration {

  val elbLogsPath: String = "./data/2015_07_22_mktplace_shop_web_log_sample.log.gz"

  val sessionTimeout: Int = 15 * 60 // 15 minutes

  // output directory for CSV files with a trailing slash
  val outputPath: String = "./output/"

}
