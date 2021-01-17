package com.example.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object LocationEtl {
  case class Location(local_authority_ons_code: String, local_authority_name: String, region_name: String)
  case class Region(region_name: String, region_ons_code: String)
  case class LocalAuthority(local_authority_ons_code: String, local_authority_name: String, region_ons_code: String)

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("Location")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Location").getOrCreate()
    val path = args(0)
    import spark.implicits._

    spark.sql("""DROP TABLE IF EXISTS `w_location`""")
    spark.sql("""CREATE TABLE `w_location` (
      `local_authority_ons_code` string,
      `local_authority_name` string,
      `region_name` string)
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

    val northRegion = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/regionsNorthEngland.csv").
      select($"region_name", $"region_ons_code").
      as[Region]

    val northLocalAuthority = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/authoritiesNorthEngland.csv").
      select($"local_authority_ons_code", $"local_authority_name", $"region_ons_code").
      as[LocalAuthority]

    val northLocation = northRegion.join(northLocalAuthority, northRegion("region_ons_code") === northLocalAuthority("region_ons_code")).
      select("local_authority_ons_code", "local_authority_name", "region_name").
      as[Location]

    val southRegion = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/regionsSouthEngland.csv").
      select($"region_name", $"region_ons_code").
      as[Region]

    val southLocalAuthority = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/authoritiesSouthEngland.csv").
      select($"local_authority_ons_code", $"local_authority_name", $"region_ons_code").
      as[LocalAuthority]

    val southLocation = southRegion.join(southLocalAuthority, southRegion("region_ons_code") === southLocalAuthority("region_ons_code")).
      select("local_authority_ons_code", "local_authority_name", "region_name").
      as[Location]

    val scotlandRegion = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/regionsScotland.csv").
      select($"region_name", $"region_ons_code").
      as[Region]

    val scotlandLocalAuthority = spark.read.format("csv").
      option("header", true).option("inferSchema", true).
      load(path + s"/authoritiesScotland.csv").
      select($"local_authority_ons_code", $"local_authority_name", $"region_ons_code").
      as[LocalAuthority]

    val scotlandLocation = scotlandRegion.join(scotlandLocalAuthority, scotlandRegion("region_ons_code") === scotlandLocalAuthority("region_ons_code")).
      select("local_authority_ons_code", "local_authority_name", "region_name").
      as[Location]

    val allLocation = northLocation.union(southLocation).union(scotlandLocation).distinct()

    allLocation.write.insertInto("w_location")
  }
}

