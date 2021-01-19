import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}


case class VehicleType(vehicle_type_id: BigInt, vehicle_type_name: String, vehicle_type_category: String, vehicle_type_genre: String)

val path = "project/data"

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
).withColumn("vehicle_type_id", functions.monotonically_increasing_id()).
  as[VehicleType]

allVehicleType.write.insertInto("w_vehicle_type")

System.exit(0)
