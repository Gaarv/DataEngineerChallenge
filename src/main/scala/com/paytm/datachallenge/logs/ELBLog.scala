package com.paytm.datachallenge.logs

import java.sql.Timestamp

case class ELBLog(
    timestamp: Timestamp,
    elb_name: String,
    request_ip: String,
    request_port: Int,
    backend_ip: String,
    backend_port: Int,
    request_processing_time: Double,
    backend_processing_time: Double,
    client_response_time: Double,
    elb_response_code: String,
    backend_response_code: String,
    received_bytes: Long,
    sent_bytes: Long,
    request_verb: String,
    url: String,
    protocol: String,
    user_agent: String,
    ssl_cipher: String,
    ssl_protocol: String
)
