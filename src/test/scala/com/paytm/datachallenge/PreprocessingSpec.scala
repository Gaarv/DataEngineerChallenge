package com.paytm.datachallenge

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.{ Dataset, functions => F }
import org.scalatest.{GivenWhenThen, Matchers, WordSpec}

class PreprocessingSpec extends WordSpec with Matchers with SparkLocal with GivenWhenThen {


  "Preprocessing" should {

    "sessionize ELB logs containing one user and one session according to a given timeout duration" in {

      Given("a dataset of ELB logs and a session timeout duration in seconds")
      val sessionTimeout = 16 * 60 // 15 minutes

      When("ELB logs are sessionized")
      val sessionizedLogs = Preprocessing.sessionize(givenELBLogsWithOneUserOneSession, sessionTimeout)

      Then("result should contain only one user_id and one session_id")
      val expected = Array(Session("0", "0", "1.1.1.1", "A", Timestamp.valueOf("2019-10-01 22:00:00"), Timestamp.valueOf("2019-10-01 22:20:00"), 20 * 60, 5, List("index", "account", "article1", "index", "checkout")))
      val result = sessionizedLogs.collect().map(_.copy(session_id = "0", user_id = "0")) // we fix session_id and user_id since they're randomly generated

      result should contain theSameElementsAs expected
    }

    "sessionize ELB logs containing one user and two sessions according to a given timeout duration" in {

      Given("a dataset of ELB logs and a session timeout duration in seconds")
      val sessionTimeout = 16 * 60 // 15 minutes

      When("ELB logs are sessionized")
      val sessionizedLogs = Preprocessing.sessionize(givenELBLogsWithOneUserTwoSessions, sessionTimeout)

      Then("result should contain only one user_id and two session_id")
      val expected = Array(
        Session("0", "0", "1.1.1.1", "A", Timestamp.valueOf("2019-10-01 22:00:00"), Timestamp.valueOf("2019-10-01 22:05:00"), 5 * 60, 2, List("index", "account")),
        Session("0", "1", "1.1.1.1", "A", Timestamp.valueOf("2019-10-01 22:25:00"), Timestamp.valueOf("2019-10-01 22:35:00"), 10 * 60, 3, List("article1", "index", "checkout"))
      )
      val result = sessionizedLogs.sort(F.col("start_timestamp")).collect().zipWithIndex.map { case (session, i) =>
        (session.copy(session_id = s"$i", user_id = "0")) // we fix session_id and user_id since they're randomly generated
      }

      result should contain theSameElementsAs expected
    }

    "sessionize ELB logs containing two users and two sessions according to a given timeout duration" in {

      Given("a dataset of ELB logs and a session timeout duration in seconds")
      val sessionTimeout = 16 * 60 // 15 minutes

      When("ELB logs are sessionized")
      val sessionizedLogs = Preprocessing.sessionize(givenELBLogsWithTwoUsersThreeSessions, sessionTimeout)

      Then("result should contain two users and three sessions")
      val expected = Array(
        Session("", "", "1.1.1.1", "A", Timestamp.valueOf("2019-10-01 22:00:00"), Timestamp.valueOf("2019-10-01 22:00:00"), 0, 1, List("index")),
        Session("", "", "2.2.2.2", "A", Timestamp.valueOf("2019-10-01 22:20:00"), Timestamp.valueOf("2019-10-01 22:30:00"), 10 * 60, 2, List("account", "index")),
        Session("", "", "1.1.1.1", "A", Timestamp.valueOf("2019-10-01 22:25:00"), Timestamp.valueOf("2019-10-01 22:35:00"), 10 * 60, 2, List("article1", "checkout"))
      )
      val result = sessionizedLogs.sort(F.col("start_timestamp")).collect().zipWithIndex.map { case (session, i) =>
        (session.copy(session_id = "", user_id = "")) // we drop session_id and user_id since they're randomly generated
      }

      result should contain theSameElementsAs expected
    }
  }

  private def givenELBLogsWithOneUserOneSession: Dataset[ELBLog] = {
    import spark.implicits._
    val logs = Seq(
      ELBLog(Timestamp.valueOf("2019-10-01 22:00:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "index", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:05:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "account", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:10:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "article1", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:15:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "index", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:20:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "checkout", "HTTP", "A", "", "")
    )
    spark.createDataset(logs)
  }

  private def givenELBLogsWithOneUserTwoSessions: Dataset[ELBLog] = {
    import spark.implicits._
    val logs = Seq(
      ELBLog(Timestamp.valueOf("2019-10-01 22:00:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "index", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:05:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "account", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:25:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "article1", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:30:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "index", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:35:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "checkout", "HTTP", "A", "", "")
    )
    spark.createDataset(logs)
  }

  private def givenELBLogsWithTwoUsersThreeSessions: Dataset[ELBLog] = {
    import spark.implicits._
    val logs = Seq(
      ELBLog(Timestamp.valueOf("2019-10-01 22:00:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "index", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:20:00"), "elb_name", "2.2.2.2", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "account", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:25:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "article1", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:30:00"), "elb_name", "2.2.2.2", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "index", "HTTP", "A", "", ""),
      ELBLog(Timestamp.valueOf("2019-10-01 22:35:00"), "elb_name", "1.1.1.1", 80, "10.1.1.1", 80, 0.0, 0.0, 0.0, "200", "200", 0, 0, "GET", "checkout", "HTTP", "A", "", "")
    )
    spark.createDataset(logs)
  }

}
