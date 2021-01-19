import org.apache.spark.sql.{SparkSession}

val spark = SparkSession.builder().appName("Weather").getOrCreate()

spark.sql("""DROP TABLE IF EXISTS `w_weather`""")
spark.sql("""CREATE TABLE `w_weather` (
  `weather_conditions` string,
  `weather_category` string,
  `weather_id` bigint)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""DROP TABLE IF EXISTS `w_date`""")
spark.sql("""CREATE TABLE `w_date` (
  `year` int,
  `month` int,
  `day` int,
  `week_count` int,
  `hour` int,
  `unix_timestamp` int,
  `date_id` bigint)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")
  
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

spark.sql("""DROP TABLE IF EXISTS `w_road_category`""")
spark.sql("""CREATE TABLE `w_road_category` (
  `road_type` string,
  `road_category` string,
  `road_category_id` bigint)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")
  
spark.sql("""DROP TABLE IF EXISTS `w_road_name`""")
spark.sql("""CREATE TABLE `w_road_name` (
  `road_name` string,
  `road_name_id` bigint)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")
  
spark.sql("""DROP TABLE IF EXISTS `w_vehicle_type`""")
spark.sql("""CREATE TABLE `w_vehicle_type` (
  `vehicle_type_name` string,
  `vehicle_type_category` string,
  `vehicle_type_genre` string,
  `vehicle_type_id` bigint)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")  

spark.sql("""DROP TABLE IF EXISTS facts""")
spark.sql(
"""
    CREATE TABLE facts (
         local_authority_ons_code string,
         date_id bigint,
         weather_id bigint,
         road_category_id bigint,
         road_name_id bigint,
         vehicle_type_id bigint,
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

System.exit(0)