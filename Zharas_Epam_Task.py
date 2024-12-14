from pyspark.sql import SparkSession 
from pyspark.sql.functions import udf, col 
from pyspark.sql.types import DoubleType, StringType 
import requests 
import pygeohash 
 
# Step 1: Create Spark Session 
spark = ( 
    SparkSession.builder.appName("Spark_ETL") 
    .master("local[*]") 
    .getOrCreate() 
) 
 
spark.sparkContext.setLogLevel("WARN") 
 
# Step 2: Load the data 
# Path to the local data files 
restaurant_path = "/data/restaurant/*.csv" 
weather_path = "/data/weather/*.parquet" 
 
# Read restaurant data (CSV) 
restaurant_df = spark.read.csv(restaurant_path, header=True, inferSchema=True) 
 
# Read weather data (Parquet) 
weather_df = spark.read.parquet(weather_path) 
 
 
# Step 3: null latitude and longitude values 
# create a function to fetch coordinates using OpenCage Geocoding API 
def fetch_coordinates(city): 
    api_key = "c499057939144008b5e5a21a21885dec"  # my OpenCage API key 
    url = f"https://api.opencagedata.com/geocode/v1/json?q={city}&key={api_key}" 
    response = requests.get(url) 
    if response.status_code == 200: 
        result = response.json() 
        if result["results"]: 
            coordinates = result["results"][0]["geometry"] 
            return coordinates["lat"], coordinates["lng"] 
    return None, None 
 
 
# Define UDFs to fetch latitude and longitude 
fetch_lat_udf = udf(lambda city: fetch_coordinates(city)[0], DoubleType()) 
fetch_lon_udf = udf(lambda city: fetch_coordinates(city)[1], DoubleType()) 
 
# Update incorrect latitude and longitude 
updated_location_df = ( 
    restaurant_df.filter(col("lat").isNull() | col("lng").isNull()) 
    .withColumn("lat", fetch_lat_udf(col("city"))) 
    .withColumn("lng", fetch_lon_udf(col("city"))) 
) 
 
# Merge corrected records back into the restaurant dataframe 
restaurant_df = restaurant_df.filter( 
    col("lat").isNotNull() & col("lng").isNotNull() 
).union(updated_location_df) 
 
 
restaurant_df = restaurant_df.withColumnRenamed("lat", "latitude").withColumnRenamed( 
    "lng", "longitude" 
) 
 
# Step 4: Generate geohash 
# Define a UDF for geohash generation 
geohash_udf = udf( 
    lambda lat, lon: pygeohash.encode(lat, lon, precision=4), StringType() 
) 
restaurant_df = restaurant_df.withColumn( 
    "geohash", geohash_udf(col("latitude"), col("longitude")) 
) 
 
geohash_udf_weather = udf( 
    lambda lat, lon: pygeohash.encode(lat, lon, precision=4), StringType() 
) 
weather_df = weather_df.withColumn( 
    "geohash", geohash_udf_weather(col("lat"), col("lng")) 
) 
# Step 5: Join weather and restaurant data 
# Deduplicate weather data to avoid data multiplication 
weather_df = weather_df.dropDuplicates(["geohash"]) 
 
# Perform a left join on geohash 
joined_df = restaurant_df.join(weather_df, on="geohash", how="left") 
 
# Step 6: Store the enriched data 
output_path = "/data/output/enriched_data" 
joined_df.write.mode("overwrite").partitionBy("geohash").parquet(output_path) 
 
# Step 7: Final message 
print(f"Enriched data has been saved to {output_path}") 
 
# Stop the Spark session 
spark.stop()