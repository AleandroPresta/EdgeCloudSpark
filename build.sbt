ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "EdgeCloudSpark",
    idePackagePrefix := Some("com.example")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.apache.spark" %% "spark-mllib" % "3.4.0",
  "com.github.wookietreiber" %% "scala-chart" % "latest.integration"
)

mainClass := Some("com.example.Main")
