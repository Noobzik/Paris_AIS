import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Hello World").getOrCreate()
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("data.csv")

    val df_arrondissement

    df.printSchema()

    df.show()


    //     Fichier d'entré de données, et de sortie retravaillé
 /*   val url = "https://opendata.paris.fr/api/v2"
    val dataset_name = "/catalog/datasets/comptages-routiers-permanents/exports/csv?limit=-1&offset=0&"
    val where = "refine=t_1h%3A%272021-12-13%27&timezone=UTC"

    val res = scala.io.Source.fromURL(url+dataset_name+where).mkString.stripMargin.lines.toList
    val csvData: RDD[String] = spark.sparkContext.parallelize(res)
*/

  }
}
