package com.example.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

object RoadNameEtl {
  case class RoadNameTemp(road_name: String)
  case class RoadName(road_name_id: BigInt, road_name: String)

  def main(args: Array[String]) {
    // to chyba do usunięcia
    val conf: SparkConf = new SparkConf().setAppName("RoadName")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("RoadName").getOrCreate()
    import spark.implicits._
    //

    spark.sql("""DROP TABLE IF EXISTS `w_road_name`""")
    spark.sql("""CREATE TABLE `w_road_name` (
      `road_name` string,
      `road_name_id` int)
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    val northRoadName = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataNorthEngland.csv").
      select($"road_name").
      as[RoadNameTemp]

    val southRoadName = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataSouthEngland.csv").
      select($"road_name").
      as[RoadNameTemp]

    val scotlandRoadName = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataScotland.csv").
      select($"road_name").
      as[RoadNameTemp]


    val allRoadName = northRoadName.union(southRoadName).union(scotlandRoadName).distinct().
      withColumn("road_name_id", functions.row_number().over(Window.orderBy(functions.monotonically_increasing_id()))).
      as[RoadName]

    allRoadName.write.insertInto("w_road_name")
  }
}

