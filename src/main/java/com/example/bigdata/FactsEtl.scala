package com.example.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{lag, lead, lit, to_timestamp, trim, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object FactsEtl {

    case class FactType(
                           local_authority_ons_code: String,
                           date_id: BigInt,
                           weather_id: BigInt,
                           road_category_id: BigInt,
                           road_name_id: BigInt,
                           vehicle_type_id: BigInt,
                           count: BigInt
                       )

    def main(args: Array[String]) {
        val conf: SparkConf = new SparkConf().setAppName("Facts")
        val sc: SparkContext = new SparkContext(conf)
        val spark: SparkSession = SparkSession.builder().appName("Facts").getOrCreate()
        val path = args(0)
        import spark.implicits._


        // load all necessary dimension tables
        val weather_df = spark.table("w_weather")
        val date_df = spark.table("w_date").select($"date_id", $"unix_timestamp")
        val road_category_df = spark.table("w_road_category")
        val road_name_df = spark.table("w_road_name")
        val vehicle_types_df = spark.table("w_vehicle_type")

        //GET DATA FROM MAIN* FILES

        val facts_raw_north_england = spark.read.format("csv").
            option("header", value = true).option("inferSchema", value = true).
            load(path + s"/mainDataNorthEngland.csv")
            .select(
                (unix_timestamp($"count_date") + lit(3600) * $"hour").as("unix_timestamp"),
                $"local_authoirty_ons_code".as("local_authority_ons_code"),
                $"road_name",
                $"road_type",
                $"road_category",
                $"pedal_cycles",
                $"two_wheeled_motor_vehicles",
                $"cars_and_taxis",
                $"buses_and_coaches",
                $"lgvs",
                $"hgvs_2_rigid_axle",
                $"hgvs_3_rigid_axle",
                $"hgvs_4_or_more_rigid_axle",
                $"hgvs_3_or_4_articulated_axle",
                $"hgvs_5_articulated_axle",
                $"hgvs_6_articulated_axle"
            )

            val facts_raw_scotland = spark.read.format("csv").
                option("header", true).option("inferSchema", true).
                load(path + s"/mainDataScotland.csv")
                .select(
                (unix_timestamp($"count_date") + lit(3600) * $"hour").as("unix_timestamp"),
                $"local_authoirty_ons_code".as("local_authority_ons_code"),
                $"road_name",
                $"road_type",
                $"road_category",
                $"pedal_cycles",
                $"two_wheeled_motor_vehicles",
                $"cars_and_taxis",
                $"buses_and_coaches",
                $"lgvs",
                $"hgvs_2_rigid_axle",
                $"hgvs_3_rigid_axle",
                $"hgvs_4_or_more_rigid_axle",
                $"hgvs_3_or_4_articulated_axle",
                $"hgvs_5_articulated_axle",
                $"hgvs_6_articulated_axle"
                )

            val facts_raw_south_england = spark.read.format("csv").
                option("header", true).option("inferSchema", true).
                load(path + s"/mainDataSouthEngland.csv")
                .select(
                (unix_timestamp($"count_date") + lit(3600) * $"hour").as("unix_timestamp"),
                $"local_authoirty_ons_code".as("local_authority_ons_code"),
                $"road_name",
                $"road_type",
                $"road_category",
                $"pedal_cycles",
                $"two_wheeled_motor_vehicles",
                $"cars_and_taxis",
                $"buses_and_coaches",
                $"lgvs",
                $"hgvs_2_rigid_axle",
                $"hgvs_3_rigid_axle",
                $"hgvs_4_or_more_rigid_axle",
                $"hgvs_3_or_4_articulated_axle",
                $"hgvs_5_articulated_axle",
                $"hgvs_6_articulated_axle"
                )

        val facts_raw_df = facts_raw_north_england.union(facts_raw_scotland).union(facts_raw_south_england)

        // DATE_ID
        val facts_with_date_df = facts_raw_df
            .join(date_df, date_df("unix_timestamp") === facts_raw_df("unix_timestamp"))
            .drop(date_df("unix_timestamp"))


        // WEATHER_ID
        val file = spark.sparkContext.textFile(path + "/weather.txt")
        val rdd = file.map(
            line => (
                line.split(" ")(4),
                line.split(" ")(6) + " " + line.split(" ")(8),
                if (line.split(":").length > 2) line.split(":")(2) else null
            )
        )

        val weather = spark.createDataFrame(rdd)
            .select(
                $"_1".as("local_authority_ons_code"),
                unix_timestamp(to_timestamp($"_2", "dd/MM/yyyy HH:mm")).as("unix_timestamp"),
                $"_3".as("condition")
            )


        val windowSpec = Window.partitionBy("local_authority_ons_code").orderBy("unix_timestamp")
        val weather_from_to = weather
            .withColumn("prev", lag("unix_timestamp", 1) over windowSpec)
            .withColumn("next", lead("unix_timestamp", 1) over windowSpec)
            .withColumn("from", $"unix_timestamp" - ($"unix_timestamp" - $"prev") / 2)
            .withColumn("to", $"unix_timestamp" + ($"next" - $"unix_timestamp") / 2)

        val weather_from_to_df = weather_from_to.where($"condition".isNotNull).withColumn("condition", trim($"condition")).drop("prev", "next")

        val weather_df_joined = weather_df
            .join(weather_from_to_df, weather_df("weather_conditions") === weather_from_to_df("condition"))
            .drop("unix_timestamp")

        val facts_with_weather_df = facts_with_date_df
            .join(weather_df_joined,
                weather_df_joined("local_authority_ons_code") === facts_with_date_df("local_authority_ons_code") &&
                    facts_with_date_df("unix_timestamp").between(weather_df_joined("from"), weather_df_joined("to"))
            )
            .drop(weather_df_joined("local_authority_ons_code"))
            .drop("weather_conditions", "condition", "weather_category", "from", "to")

        // ROAD CATEGORY
        val facts_with_road_category_df = facts_with_weather_df
            .join(road_category_df, road_category_df("road_type") === facts_with_weather_df("road_type") && road_category_df("road_category") === facts_with_weather_df("road_category"))
            .drop("road_type", "road_category")


        // ROAD NAME
        val facts_with_road_name_df = facts_with_road_category_df
            .join(road_name_df, road_name_df("road_name") === facts_with_road_category_df("road_name"))
            .drop("road_name")


        // TABLE TRANSFORM TO TARGET TABLE SCHEMA + VEHICLE TYPE ID
        val vehicle_iter = vehicle_types_df.select($"vehicle_type_name", $"vehicle_type_id".cast("int")).as[(String, Int)].collect()


        vehicle_iter.foreach(veh => {
            val veh_type = veh._1
            val veh_id = veh._2

            facts_with_road_name_df
                .where(facts_with_road_name_df(veh_type) > 0)
                .select(
                    $"local_authority_ons_code",
                    $"date_id",
                    $"weather_id",
                    $"road_category_id",
                    $"road_name_id",
                    lit(veh_id).as("vehicle_type_id"),
                    facts_with_road_name_df(veh_type).as("count")
                )
                .write.insertInto("facts")
        })

    }

}
