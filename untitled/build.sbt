name := "Project-Auto"

version := "0.1"
sbtVersion:="0.13"
scalaVersion := "2.11.11"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"