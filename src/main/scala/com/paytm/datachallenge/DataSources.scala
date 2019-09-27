package com.paytm.datachallenge

import com.paytm.datachallenge.logs.ELBLog
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{ Column, DataFrame, Dataset, SparkSession, functions => F }

object DataSources {

  val rawColumns = List(
    "timestamp",
    "elb_name",
    "request_ip_port",
    "backend_ip_port",
    "request_processing_time",
    "backend_processing_time",
    "client_response_time",
    "elb_response_code",
    "backend_response_code",
    "received_bytes",
    "sent_bytes",
    "method_protocol_url",
    "user_agent",
    "ssl_cipher",
    "ssl_protocol"
  )

  def readELBLogs(path: String)(spark: SparkSession): Dataset[ELBLog] = {
    import spark.implicits._
    val rawDf = spark.read
      .option("header", "false")
      .schema(elbLogsSchema)
      .option("delimiter", " ")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)
      .toDF(rawColumns: _*)
    parseElbLogs(rawDf).as[ELBLog]
  }

  def parseElbLogs(df: DataFrame): DataFrame =
    df.withColumn("request_ip", F.split(F.col("request_ip_port"), ":")(0))
      .withColumn("request_port", F.split(F.col("request_ip_port"), ":")(1).cast(IntegerType))
      .withColumn("backend_ip", F.split(F.col("backend_ip_port"), ":")(0))
      .withColumn("backend_port", F.split(F.col("backend_ip_port"), ":")(1).cast(IntegerType))
      .withColumn("request_verb", parseMethod(F.col("method_protocol_url")))
      .withColumn("protocol", parseProtocol(F.col("method_protocol_url")))
      .withColumn("url", parseUrl(F.col("method_protocol_url")))
      .drop(List("request_ip_port", "backend_ip_port", "method_protocol_url"): _*)

  private def parseMethod(c: Column): Column = F.split(c, " ")(0)

  private def parseUrl(c: Column): Column = F.split(c, " ")(1)

  private def parseProtocol(c: Column): Column = F.upper(F.split(parseUrl(c), "://")(0))
}
