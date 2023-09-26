ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "fleet-monitoring"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"