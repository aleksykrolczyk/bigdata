package com.example.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object WeatherEtl {
  case class Weather(weather_id: BigInt, weather_conditions: String, weather_category: String)

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("Weather")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Weather").getOrCreate()
    val path = args(0)
    import spark.implicits._

    val allWeather = spark.read.format("text").
      option("header", false).option("inferSchema", false).
      load(path + s"/weather.txt").
      select(trim(split($"value", ":").getItem(2)).as("weather_conditions")).
      where(not($"weather_conditions" === "null" or $"weather_conditions" === "Unknown")).
      withColumn("weather_category", when($"weather_conditions".contains("Fine"), "favorable").otherwise("unfavorable")).
      withColumn("weather_id", functions.monotonically_increasing_id()).
      as[Weather]

    allWeather.write.insertInto("w_weather")
  }
}

