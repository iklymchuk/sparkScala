name := "SBT"

version := "1.0"

organization := "com.klymchuk"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
)
