package com.example.bigdata

import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object WeatherEtl {
  case class Weather(weather_id: Int, weather_conditions: String, weather_category: String)

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Weather")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Weather").getOrCreate()
    import spark.implicits._


    val file = spark.sparkContext.textFile("uk-trafic/weather.txt")
    val rdd = file.map(
      line => (line.split(" ")(4), line.split(" ")(6) + " " + line.split(" ")(8))
      )
    val df = spark.createDataFrame(rdd)

    val weather = df
        .select(
            $"_1".as("ons"),
          unix_timestamp(to_timestamp($"_2", "dd/MM/yyyy HH:mm")).as("time")
          )

    val windowSpec = Window.partitionBy("ons").orderBy("time")
    val weather_from_to = weather.withColumn("prev", lag("time", 1) over windowSpec)
        .withColumn("next", lead("time", 1) over windowSpec)
        .withColumn("from", $"time" - ($"time" - $"prev")/2)
        .withColumn("to", $"time" + ($"next" - $"time")/2)
    weather_from_to.show()


  }
}

