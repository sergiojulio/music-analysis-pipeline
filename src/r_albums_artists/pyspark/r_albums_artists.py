from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, ByteType, BooleanType, ByteType, DoubleType, StringType, IntegerType, TimestampType
import os
import datetime

if __name__ == "__main__":

    spark = SparkSession.builder.appName("homologacion").getOrCreate()
    sc = spark.sparkContext

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("acousticness", DoubleType(), True),
        StructField("analysis_url", StringType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("duration", IntegerType(), True),
        StructField("energy", StringType(), True),
        StructField("instrumentalness", DoubleType(), True),
        StructField("key", ByteType(), True),
        StructField("liveness", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("mode", BooleanType(), True),
        StructField("speechiness", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", ByteType(), True),
        StructField("valence", DoubleType(), True)
    ])

    dirname = os.path.dirname(os.path.abspath(__file__))

    df = spark.read.option("header", True).format("csv").schema(schema).option("delimiter", ',').load(
        "/home/sergio/dev/python/music"
        "-analysis-pipeline/datalake"
        "/raw/audio_features.csv")

    df = df.withColumn('process_date',
                       func.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))

    parquetfilename = os.path.join(dirname, 'output.parquet')

    df.write.mode('overwrite').parquet(
        '/home/sergio/dev/python/music-analysis-pipeline/datalake/parquet/audio_features.parquet')

