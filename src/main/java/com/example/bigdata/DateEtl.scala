package com.example.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DateEtl {
  case class Date(date_id: BigInt, year: Int, month: Int, day: Int, week_count: Int, hour: Int)

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("Date")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Date").getOrCreate()
    val path = args(0)
    import spark.implicits._

    val northDate = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/mainDataNorthEngland.csv").
      select(
        year($"count_date").as("year"),
        month($"count_date").as("month"),
        dayofmonth($"count_date").as("day"),
        weekofyear($"count_date").as("week_count"),
        $"hour",
      (unix_timestamp($"count_date") + lit(3600) * $"hour").as("unix_timestamp")
      )

    val southDate = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/mainDataSouthEngland.csv").
      select(
        year($"count_date").as("year"),
        month($"count_date").as("month"),
        dayofmonth($"count_date").as("day"),
        weekofyear($"count_date").as("week_count"),
        $"hour",
      (unix_timestamp($"count_date") + lit(3600) * $"hour").as("unix_timestamp")
      )

    val scotlandDate = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/mainDataScotland.csv").
      select(
        year($"count_date").as("year"),
        month($"count_date").as("month"),
        dayofmonth($"count_date").as("day"),
        weekofyear($"count_date").as("week_count"),
        $"hour",
        (unix_timestamp($"count_date") + lit(3600) * $"hour").as("unix_timestamp")
      )


    val allDate = northDate.union(southDate).union(scotlandDate).distinct().
      withColumn("date_id", functions.monotonically_increasing_id())

    allDate.write.insertInto("w_date")
  }
}

