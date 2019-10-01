package com.paytm.datachallenge

import java.sql.Timestamp

case class Session(
    user_id: String,
    request_ip: String,
    user_agent: String,
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
    duration: Long,
    hits: Long,
    urls: List[String]
)
