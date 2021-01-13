package com.example.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

object RoadCategoryEtl {
  case class RoadCategoryTemp(road_type: String, road_category: String)
  case class RoadCategory(road_category_id: BigInt, road_type: String, road_category: String)

  def main(args: Array[String]) {
    // to chyba do usunięcia
    val conf: SparkConf = new SparkConf().setAppName("RoadCategory")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("RoadCategory").getOrCreate()
    import spark.implicits._
    //

    spark.sql("""DROP TABLE IF EXISTS `w_road_category`""")
    spark.sql("""CREATE TABLE `w_road_category` (
      `road_type` string,
      `road_category` string,
      `road_category_id` int)
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    val northRoadCategory = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataNorthEngland.csv").
      select($"road_type", $"road_category").
      as[RoadCategoryTemp]

    val southRoadCategory = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataSouthEngland.csv").
      select($"road_type", $"road_category").
      as[RoadCategoryTemp]

    val scotlandRoadCategory = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataScotland.csv").
      select($"road_type", $"road_category").
      as[RoadCategoryTemp]

    val allRoadCategory = northRoadCategory.union(southRoadCategory).union(scotlandRoadCategory).distinct().
      withColumn("road_category_id", functions.row_number().over(Window.orderBy(functions.monotonically_increasing_id()))).
      as[RoadCategory]

    allRoadCategory.write.insertInto("w_road_category")
  }
}

