import sbt._

object Dependencies {

  val sparkVersion   = "2.3.2"
  val logbackVersion = "1.2.3"
  val slf4jVersion   = "1.7.21"

  lazy val sparkCore      = "org.apache.spark" %% "spark-core"     % sparkVersion % "provided"
  lazy val sparkSQL       = "org.apache.spark" %% "spark-sql"      % sparkVersion % "provided"
  lazy val logbackCore    = "ch.qos.logback"   % "logback-core"    % logbackVersion
  lazy val slf4j          = "org.slf4j"        % "slf4j-api"       % slf4jVersion
  lazy val scalaTest      = "org.scalatest"    %% "scalatest"      % "3.0.8"

}
