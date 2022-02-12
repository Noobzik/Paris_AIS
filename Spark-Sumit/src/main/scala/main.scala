import org.apache.spark.sql.{Column, SparkSession, functions}

object main extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Hello_World")
    .getOrCreate();
}