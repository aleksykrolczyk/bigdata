import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

case class RoadName(road_name_id: BigInt, road_name: String)

val path = "project/data"

val northRoadName = spark.read.format("csv").
  option("header", true).option("inferSchema", true).
  load(path + s"/mainDataNorthEngland.csv").
  select($"road_name")

val southRoadName = spark.read.format("csv").
  option("header", true).option("inferSchema", true).
  load(path + s"/mainDataSouthEngland.csv").
  select($"road_name")

val scotlandRoadName = spark.read.format("csv").
  option("header", true).option("inferSchema", true).
  load(path + s"/mainDataScotland.csv").
  select($"road_name")


val allRoadName = northRoadName.union(southRoadName).union(scotlandRoadName).distinct().
  withColumn("road_name_id", functions.monotonically_increasing_id()).
  as[RoadName]

allRoadName.write.insertInto("w_road_name")

System.exit(0)
