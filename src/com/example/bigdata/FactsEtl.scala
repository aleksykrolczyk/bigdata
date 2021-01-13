/*

LAG, LEAD functions
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-functions-windows.html#lag

 */


package com.example.bigdata

import com.example.bigdata.DateEtl.DateTemp
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{dayofmonth, month, weekofyear, year}

/*

local_authority_ons_code: Text
date_id:                  Number FK w_date
weather_id:               Number FK w_weather
road_category_id:         Number FK w_road_category
road_name_id:             Number FK w_road_name
vehicle_type_id:          Number FK w_vehicle_type
count:                    Number

 */

object FactsEtl {
    case class FactType(
                       local_authority_ons_code: String,
                       date_id: BigInt,
                       weather_id: BigInt,
                       road_category_id: BigInt,
                       road_name_id: BigInt,
                       vehicle_type_id: BigInt,
                       count: Int
                       )

    def main(args: Array[String]): Unit = {

        val APP_NAME = "Fact"

        val conf: SparkConf = new SparkConf().setAppName(APP_NAME)
        val sc: SparkContext = new SparkContext(conf)
        val spark = SparkSession.builder().appName(APP_NAME).getOrCreate()

        spark.sql("""DROP TABLE IF EXISTS f_facts""")
        spark.sql(
        """
            CREATE TABLE f_facts (
                 local_authority_ons_code string,
                 date_id int,
                 weather_id int,
                 road_category_id int,
                 road_name_id int,
                 vehicle_type_id int,
                 count int
            )
            ROW FORMAT SERDE
            'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
            STORED AS INPUTFORMAT
            'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
            OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
            """
        )

//        val mainDataNorth = spark.read.format("csv").
//            option("header", true).option("inferSchema", true).
//            load(s"mainDataNorthEngland.csv").
//            select(
//                ???
//            ).as[DateTemp]

//        print(Text)

    }
}


