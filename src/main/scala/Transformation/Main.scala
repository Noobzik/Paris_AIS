package Transformation

import org.apache.spark.sql.{SparkSession, functions}
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator
import org.locationtech.geomesa.spark.jts._
import org.apache.spark.sql.functions.{broadcast, col, dayofmonth, expr, from_json, hour, month, split, udf, year, asc, sum}
import org.apache.spark.sql.types._


object Main {


  def main(args: Array[String]): Unit = {

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
      .option("inferSchema", "true")
      .load("data.csv")

    df_raw.printSchema()

    val df_arrondissement = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .option("geomesa.feature", "geom_x_y")
      .load("arrondissements.csv")

    val df_ref = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .option("geomesa.feature", "geom_x_y")
      .load("referentiel-comptages-routiers.csv")

    /* Transformation des datasets pour réaliser la jointure */

    /* Eclatement des coordonnés et cast en point geomesa*/
    val df_raw_altered = df_raw.withColumn("Lon", split($"geo_point_2d", ",").getItem(0))
      .withColumn("Lat", split($"geo_point_2d", ",").getItem(1))
      .drop("geo_point_2d", "geo_shape")
      .withColumn("Point", st_makePoint($"Lon", $"Lat"))
    df_raw_altered.printSchema()
    //    df_raw_altered.show(false)


    /* Fonction pour clean la colonne des shapes */

    val removeQuotesEntry = udf((x: String) => x.replace("{\"", "{"))
    val removeQuotesExit = udf((x: String) => x.replace("}\"", "}"))
    val removeDoubleQuotes = udf((x: String) => x.replace("\"\"", "\""))

    /* Clean des colonnes Shapes et chargement en geom par GeoJSON */
    val df_ref_altered = df_ref
      .withColumn("Entry", expr("substring(geo_shape, 2, length(geo_shape) - 2)"))
      .withColumn("Cleaned", removeDoubleQuotes(removeQuotesEntry(removeQuotesExit($"Entry"))))
      .withColumn("Geo", st_geomFromGeoJSON($"Cleaned"))
      .drop("Entry", "Cleaned", "geo_shape")


    val df_poly = df_arrondissement
      .withColumn("Entry", expr("substring(geom, 2, length(geom) - 2)"))
      .withColumn("Cleaned", removeDoubleQuotes(removeQuotesExit(removeQuotesEntry($"Entry"))))
      .drop("Entry")
      .withColumn("Parsed", st_geomFromGeoJSON($"Cleaned"))
      .drop("geom", "Cleaned")

    /* Réalisation de la première jointure entre Arrondissement et référentiel */
    val join_1 = df_ref_altered.join(broadcast(df_poly), st_contains($"Parsed", $"Geo"))

    /* Réalisation de la jointure final avec le résultat et les données brutes */
    val join_2 = df_raw_altered.join(join_1, $"iu_ac" === $"Identifiant arc", "full").na.drop()

    join_2.show()


    /* Partie Analyse */

    val groupedArrondissement = join_2.groupBy($"t_1h", $"etat_trafic", $"n_sq_ar").count()
    groupedArrondissement.show(5000)
    val hourly_raw = groupedArrondissement
      .withColumn("hour", hour($"t_1h"))
      .withColumn("day", dayofmonth($"t_1h"))
      .withColumn("month", month($"t_1h"))
      .withColumn("year", year($"t_1h"))

    // Requête pour une journée agrégé pour tout paris trié par heure et par jour
    hourly_raw.groupBy($"year", $"day", $"hour", $"month", $"etat_trafic")
      .agg(functions.sum($"count"))
      .sort(asc("month"),
        asc("day"),
        asc("hour"),
        asc("etat_trafic"))
      .show(200)

    // Requete pour une journée par arrondissement

    hourly_raw.groupBy($"year", $"day", $"hour", $"month", $"etat_trafic", $"n_sq_ar")
      .agg(sum($"count"))
      .sort(asc("month"),
        asc("day"),
        asc("hour"),
        asc("n_sq_ar"),
        asc("etat_trafic"))
      .show(200)

    /* Ecriture du résultat vers un Hadoop */


  }

}
