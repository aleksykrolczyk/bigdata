import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

case class RoadCategory(road_category_id: BigInt, road_type: String, road_category: String)

val path = "project/data"

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

System.exit(0)
