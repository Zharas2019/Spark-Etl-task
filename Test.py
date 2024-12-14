import unittest 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col 
 
 
class TestETLProcess(unittest.TestCase): 
    @classmethod 
    def setUpClass(cls): 
        cls.spark = ( 
            SparkSession.builder.appName("TestETLProcess") 
            .master("local[*]") 
            .getOrCreate() 
        ) 
 
        # Sample restaurant data (mocked) 
        cls.restaurant_data = [ 
            (1, "Franchise A", None, None, "CityX"), 
            (2, "Franchise B", 40.7128, -74.0060, "CityY"),  # Valid lat/lon 
        ] 
        cls.restaurant_schema = [ 
            "id", 
            "franchise_name", 
            "latitude", 
            "longitude", 
            "city", 
        ] 
        cls.restaurant_df = cls.spark.createDataFrame( 
            cls.restaurant_data, schema=cls.restaurant_schema 
        ) 
 
        # Sample weather data (mocked) 
        cls.weather_data = [("City", 78.0, 25.5, "2024-12-12")] 
        cls.weather_schema = ["geohash", "avg_tmpr_f", "avg_tmpr_c", "wthr_date"] 
        cls.weather_df = cls.spark.createDataFrame( 
            cls.weather_data, schema=cls.weather_schema 
        ) 
 
    @classmethod 
    def tearDownClass(cls): 
        cls.spark.stop() 
 
    def test_null_values_updated(self): 
        updated_df = self.restaurant_df.fillna( 
            {"latitude": 40.7306, "longitude": -73.9352} 
        ) 
        null_values = updated_df.filter( 
            col("latitude").isNull() | col("longitude").isNull() 
        ).count() 
        self.assertEqual( 
            null_values, 0, "There should be no null latitude or longitude" 
        ) 
 
    def test_geohash_generation(self): 
        from pygeohash import encode 
 
        updated_df = self.restaurant_df.withColumn("geohash", col("city").substr(0, 4)) 
        geohashes = updated_df.select("geohash").rdd.map(lambda row: row[0]).collect() 
        for geohash in geohashes: 
            if geohash:  # Check only non-null geohashes 
                self.assertEqual(len(geohash), 4, "Geohash must be 4 characters long") 
 
    def test_join_operation(self): 
        restaurant_with_geohash = self.restaurant_df.withColumn( 
            "geohash", col("city").substr(0, 4) 
        ) 
        weather_with_geohash = self.weather_df.withColumn("geohash", col("geohash")) 
 
        joined_df = restaurant_with_geohash.join( 
            weather_with_geohash, on="geohash", how="left" 
        ) 
        count = joined_df.count() 
        self.assertGreaterEqual( 
            count, self.restaurant_df.count(), "Join operation failed" 
        ) 
 
 
if name == "__main__": 
    unittest.main()