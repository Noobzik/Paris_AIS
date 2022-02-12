package Transformation


import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator
import org.locationtech.geomesa.spark.jts._
import org.apache.spark.sql.functions.{broadcast, col, expr, from_json, split, udf}
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]) : Unit = {

    // Emplacement de hadoop
   //  System.setProperty("hadoop.home.dir", "C:/Users/raki_/Downloads/hadoop-3.2.2")

    /* Définition d'une SparkSession */

    val spark: SparkSession = SparkSession.builder()
      .appName("testSpark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registration", classOf[GeoMesaSparkKryoRegistrator].getName)
      .master("local[*]")
      .getOrCreate()
      .withJTS

    import spark.implicits._

    /* Chargement des Datasets */

    val df_raw = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema","true")
      .load("data.csv")

    val df_arrondissement = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema","true")
      .option("geomesa.feature", "geom_x_y")
      .load("arrondissements.csv")

    val df_ref = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema","true")
      .option("geomesa.feature", "geom_x_y")
      .load("referentiel-comptages-routiers.csv")


    val df_raw_altered = df_raw.withColumn("Lon", split($"geo_point_2d",",").getItem(0))
      .withColumn("Lat", split($"geo_point_2d",",").getItem(1))
      .drop("geo_point_2d", "geo_shape")
      .withColumn("Point", st_makePoint($"Lon", $"Lat"))
    df_raw_altered.printSchema()
    df_raw_altered.show(false)



    val removeQuotesEntry = udf( (x:String) => x.replace("{\"", "{"))
    val removeQuotesExit = udf( (x:String) => x.replace("}\"", "}"))
    val removeDoubleQuotes = udf ((x:String) => x.replace("\"\"", "\""))

    val df_ref_altered = df_ref
      .withColumn("Entry", expr("substring(geo_shape, 2, length(geo_shape) - 2)"))
      .withColumn("Cleaned", removeDoubleQuotes(removeQuotesEntry(removeQuotesExit($"Entry"))))
      .withColumn("Geo", st_geomFromGeoJSON($"Cleaned"))
      .drop("Entry", "Cleaned", "geo_shape")

    df_ref_altered.show(false)

    val df_poly = df_arrondissement
      .withColumn("Entry", expr("substring(geom, 2, length(geom) - 2)"))
      .withColumn("Cleaned",removeDoubleQuotes(removeQuotesExit(removeQuotesEntry($"Entry"))))
      .drop("Entry")

    val df_1 = df_poly.withColumn("Parsed", st_geomFromGeoJSON($"Cleaned"))
      .drop("geom", "Cleaned")
   // df_1.show(false)




    val join = df_ref_altered.join(broadcast(df_1), st_contains($"Parsed", $"Geo"))
    join.show(false)

    val join_2 = df_raw_altered.join(join, $"iu_ac" === $"Identifiant arc", "full")
    join_2.show()
    //df_raw.show(6, false)

  //  df_raw.select($"geo_point_2d").show(false)
  //  df_arrondissement.select($"geom_x_y").show(false)

    //val s1 = df_raw.select($"iu_ac",$"libelle", $"t_1h", $"etat_trafic", split($"geo_point_2d",",").as("Test-df"))
    //val s2 = df_arrondissement.select($"n_sq_ar", split($"geom_x_y",",").as("Test-arrond"))
    //s1.show(false)
    //val joindedDf = df_raw.join(df_ref, $"iu_ac" === $"Identifiant arc", "full")

    //joindedDf.show(false)

    //val casted = df_raw.select(st_pointFromText($"geo_point_2d").as("test"))
    //casted.show(false)

    //val joinedDF = df_raw.join(broadcast(df_arrondissement), st_contains(st_mPointFromText($"geo_point_2d"), st_mPointFromText($"geom_x_y")))
    //joinedDF.show(false)
    //val joinedDF = gdeltDF.join(broadcast(fipsDF), st_contains($"geo_point_2d", $"geom"))
    /* Etape de la transformation avec des jointures */



    /* Ecriture du résultat vers un Hadoop */

  }

}
