ThisBuild / scalaVersion := "2.11.12"



resolvers ++= Seq(
  "osgeo" at "https://repo.osgeo.org/repository/release",
  "confluent" at "https://packages.confluent.io/maven"
)
// Fixes Dependency
dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"

// https://mvnrepository.com/artifact/org.locationtech.geomesa/geomesa-tools
libraryDependencies += "org.locationtech.geomesa" %% "geomesa-gt-spark-runtime" % "3.3.0"






