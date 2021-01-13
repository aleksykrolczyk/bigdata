package com.example.bigdata

import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}

object VehicleTypeEtl {
  case class VehicleType(vehicle_type_id: Int, vehicle_type_name: String, vehicle_type_category: String, vehicle_type_genre: String)

  def main(args: Array[String]) {
    // to chyba do usunięcia
//    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkWordCount")
    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("VehicleType").getOrCreate()
    import spark.implicits._

    //

    spark.sql("""DROP TABLE IF EXISTS `w_vehicle_type`""")
    spark.sql("""CREATE TABLE `w_vehicle_type` (
      `vehicle_type_name` string,
      `vehicle_type_category` string,
      `vehicle_type_genre` string,
      `vehicle_type_id` int)
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

//    Vehicle category
//    M - vehicles carrying passengers
//    N - vehicles carrying goods
//    L - 2- and 3-wheel vehicles and quadricycles
//    T - agricultural and forestry tractors and their trailers

    val allVehicleType = List(
      ("pedal_cycles", "L", "human_powered"),
      ("two_wheeled_motor_vehicles", "L", "motor_powered"),
      ("cars_and_taxis", "M", "not_more_than_eight_seats"),
      ("buses_and_coaches", "M", "more_than_eight_seats"),
      ("lgvs", "N", "rigid_axle"),
      ("hgvs_2_rigid_axle", "N", "rigid_axle"),
      ("hgvs_3_rigid_axle", "N", "rigid_axle"),
      ("hgvs_4_or_more_rigid_axle", "N", "rigid_axle"),
      ("hgvs_3_or_4_articulated_axle", "N", "articulated_axle"),
      ("hgvs_5_articulated_axle", "N", "articulated_axle"),
      ("hgvs_6_articulated_axle", "N", "articulated_axle")
    ).toDF(
      "vehicle_type_name",
      "vehicle_type_category",
      "vehicle_type_genre"
    ).withColumn("vehicle_type_id", functions.row_number().over(Window.orderBy(functions.monotonically_increasing_id()))).
      as[VehicleType]

    allVehicleType.write.insertInto("w_vehicle_type")
  }
}

