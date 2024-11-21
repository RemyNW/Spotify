from  pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

spark = SparkSession.builder \
    .appName("Spotify Database") \
    .config("spark.jars", "/usr/share/java/mysql-connector-java-9.1.0.jar") \
    .getOrCreate()

mysql_url = "jdbc:mysql://localhost:3306/spotify"
mysql_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

playlist_schema = StructType([
    StructField("id", StringType(), True),
    StructField("playlist_name", StringType(), True),
    StructField("collaborative", BooleanType(), True),
    StructField("modified_at", StringType(), True),
    StructField("num_followers", IntegerType(), True)
])

playlist_df = spark.createDataFrame([], playlist_schema)
playlist_df.write.jdbc(url=mysql_url, table="Playlist", mode="overwrite", properties=mysql_properties)
