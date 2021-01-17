spark-submit --class com.example.bigdata.DateEtl date-etl.jar project/data
spark-submit --class com.example.bigdata.LocationEtl location-etl.jar project/data
spark-submit --class com.example.bigdata.RoadCategoryEtl road_category-etl.jar project/data
spark-submit --class com.example.bigdata.RoadNameEtl road_name-etl.jar project/data
spark-submit --class com.example.bigdata.VehicleTypeEtl vehicle_type-etl.jar project/data
spark-submit --class com.example.bigdata.WeatherEtl weather-etl.jar project/data
