package com.paytm

import org.apache.spark.sql.types._

package object datachallenge {

  /**
    * Taken from https://docs.aws.amazon.com/athena/latest/ug/elasticloadbalancer-classic-logs.html
    *  and modified as necessary according to given raw data
    *
    */
  val elbLogsSchema: StructType = new StructType()
    .add(StructField("timestamp", TimestampType, nullable = false))
    .add(StructField("elb_name", StringType, nullable = false))
    .add(StructField("request_ip_port", StringType, nullable = false))
    .add(StructField("backend_ip_port", StringType, nullable = false))
    .add(StructField("request_processing_time", DoubleType, nullable = false))
    .add(StructField("backend_processing_time", DoubleType, nullable = false))
    .add(StructField("client_response_time", DoubleType, nullable = false))
    .add(StructField("elb_response_code", StringType, nullable = false))
    .add(StructField("backend_response_code", StringType, nullable = false))
    .add(StructField("received_bytes", LongType, nullable = false))
    .add(StructField("sent_bytes", LongType, nullable = false))
    .add(StructField("method_protocol_url", StringType, nullable = false))
    .add(StructField("user_agent", StringType, nullable = false))
    .add(StructField("ssl_cipher", StringType, nullable = false))
    .add(StructField("ssl_protocol", StringType, nullable = false))

}
