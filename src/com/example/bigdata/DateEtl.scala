package com.example.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DateEtl {
  case class DateTemp(year: Int, month: Int, day: Int, week_count: Int, hour: Int)
  case class Date(date_id: BigInt, year: Int, month: Int, day: Int, week_count: Int, hour: Int)

  def main(args: Array[String]) {
    // to chyba do usunięcia
    val conf: SparkConf = new SparkConf().setAppName("Date")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Date").getOrCreate()
    import spark.implicits._
    //

    spark.sql("""DROP TABLE IF EXISTS `w_date`""")
    spark.sql("""CREATE TABLE `w_date` (
      `year` int,
      `month` int,
      `day` int,
      `week_count` int,
      `hour` int,
      `date_id` int)
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    val northDate = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataNorthEngland.csv").
      select(
        year($"count_date").as("year"),
        month($"count_date").as("month"),
        dayofmonth($"count_date").as("day"),
        weekofyear($"count_date").as("week_count"),
        $"hour"
      ).as[DateTemp]

    val southDate = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataSouthEngland.csv").
      select(
        year($"count_date").as("year"),
        month($"count_date").as("month"),
        dayofmonth($"count_date").as("day"),
        weekofyear($"count_date").as("week_count"),
        $"hour"
      ).as[DateTemp]

    val scotlandDate = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(s"mainDataScotland.csv").
      select(
        year($"count_date").as("year"),
        month($"count_date").as("month"),
        dayofmonth($"count_date").as("day"),
        weekofyear($"count_date").as("week_count"),
        $"hour"
      ).as[DateTemp]


    val allDate = northDate.union(southDate).union(scotlandDate).distinct().
      withColumn("date_id", functions.row_number().over(Window.orderBy(functions.monotonically_increasing_id()))).
      as[Date]

    allDate.write.insertInto("w_date")
  }
}
