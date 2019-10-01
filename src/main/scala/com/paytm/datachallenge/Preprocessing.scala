package com.paytm.datachallenge

import java.util.UUID.randomUUID

import com.paytm.datachallenge.AnalyticsApp._
import org.apache.spark.sql.Dataset
import spark.implicits._

object Preprocessing {

  /**
    * Function that aggregate ELB logs hits and session duration
    * by using (request_ip, user_agent) as aggregation key
    *
    * @param logs a Dataset of [[ELBLog]]
    * @param sessionTimeout number of seconds after which to consider the session has expired
    * @return a Dataset of [[Session]]
    */
  def sessionize(logs: Dataset[ELBLog], sessionTimeout: Int): Dataset[Session] = {
    val sessions = logs.rdd
      .groupBy(log => (log.request_ip, log.user_agent))
      .flatMapValues(aggregateLogsBySession(_, sessionTimeout))
      .values
    spark.createDataset(sessions)
  }

  /**
    * Function that aggregate values from a grouped RDD.
    * Emits a new [[Session]] when timestamps from two logs entries are spaced with  more than sessionTimeout.
    * Aggregate values from the current processed session to the previous session when
    * two logs entries are spaced with less than sessionTimeout.
    *
    * @param logs a Iterable of [[ELBLog]]
    * @param sessionTimeout number of seconds after which to consider the session has expired
    * @return a List of [[Session]]
    */
  private def aggregateLogsBySession(logs: Iterable[ELBLog], sessionTimeout: Int): List[Session] =
    logs.toList.sortBy(_.timestamp.toInstant.getEpochSecond).foldLeft(List.empty[Session]) { (sessions, log) =>
      lazy val duration = sessionDuration(log, sessions.head)
      if (sessions.nonEmpty && duration < sessionTimeout) {
        val previousSession = sessions.head
        val sameSession = previousSession.copy(
          end_timestamp = log.timestamp,
          duration = previousSession.duration + duration,
          hits = previousSession.hits + 1,
          urls = previousSession.urls :+ log.url
        )
        sameSession +: sessions.tail
      } else {
        val newSession = Session(randomUUID().toString, log.request_ip, log.user_agent, log.timestamp, log.timestamp, 0, 1, List(log.url))
        newSession +: sessions
      }
    }

  /**
    * Function to calculate number of seconds between two timestamps
    *
    * @param log a [[ELBLog]] entry
    * @param session a session to compare with the log entry
    * @return number of seconds between timestamp from the log and end_timestamp from the session
    */
  private def sessionDuration(log: ELBLog, session: Session): Long =
    log.timestamp.toInstant.getEpochSecond - session.end_timestamp.toInstant.getEpochSecond

}
