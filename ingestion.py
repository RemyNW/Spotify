from  pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
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

file_path = 'spotify_million_playlist_dataset/data/mpd.slice.0-999.json'

data = spark.read.format("json").option("multiline", True).load(file_path)

playlists_df = data.selectExpr("explode(playlists) as playlist").select(
    col("playlist.name").alias("playlist_name"),
    col("playlist.collaborative").cast(BooleanType()).alias("collaborative"),
    (col("playlist.modified_at")).cast(StringType()).alias("modified_at"),
    col("playlist.num_followers").alias("num_followers"),
    col("playlist.pid").alias("id")
)

tracks_df = data.selectExpr("explode(playlists) as playlist").select(
    explode(col("playlist.tracks")).alias("track")
).select(
    col("track.track_uri"),
    col("track.artist_uri"),
    col("track.album_uri"),
    col("track.track_name"),
    col("track.duration_ms")
)

association_df = data.selectExpr("explode(playlists) as playlist").select(
    col("playlist.pid").alias("playlist_id"),
    explode(col("playlist.tracks")).alias("track")
).select(
    col("playlist_id"),
    col("track.track_uri")
)

playlists_df.write.jdbc(url=mysql_url, table="Playlist", mode="append", properties=mysql_properties)
tracks_df.write.jdbc(url=mysql_url, table="Track", mode="append", properties=mysql_properties)
association_df.write.jdbc(url=mysql_url, table="association_playlist_track", mode="append", properties=mysql_properties)
