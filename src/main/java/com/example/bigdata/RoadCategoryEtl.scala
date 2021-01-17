package com.example.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

object RoadCategoryEtl {
  case class RoadCategory(road_category_id: BigInt, road_type: String, road_category: String)

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("RoadCategory")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("RoadCategory").getOrCreate()
    val path = args(0)
    import spark.implicits._

    spark.sql("""DROP TABLE IF EXISTS `w_road_category`""")
    spark.sql("""CREATE TABLE `w_road_category` (
      `road_type` string,
      `road_category` string,
      `road_category_id` bigint)
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    val northRoadCategory = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/mainDataNorthEngland.csv").
      select($"road_type", $"road_category")

    val southRoadCategory = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/mainDataSouthEngland.csv").
      select($"road_type", $"road_category")

    val scotlandRoadCategory = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/mainDataScotland.csv").
      select($"road_type", $"road_category")

    val allRoadCategory = northRoadCategory.union(southRoadCategory).union(scotlandRoadCategory).distinct().
      withColumn("road_category_id", functions.monotonically_increasing_id()).
      as[RoadCategory]

    allRoadCategory.write.insertInto("w_road_category")
  }
}

