import Dependencies._

lazy val root = (project in file(".")).settings(
  organization := "com.paytm",
  scalaVersion := "2.11.12",
  version := "0.1.0-SNAPSHOT",
  name := "data-engineer-challenge",
  mainClass in assembly := Some("com.paytm.AnalyticsApp"),
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case "reference.conf"              => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _                             => MergeStrategy.first
  },
  test in assembly := {},
  libraryDependencies += sparkCore,
  libraryDependencies += sparkSQL,
  libraryDependencies += logbackCore,
  libraryDependencies += logbackClassic,
  libraryDependencies += slf4j,
  libraryDependencies += scalaTest % Test
)
