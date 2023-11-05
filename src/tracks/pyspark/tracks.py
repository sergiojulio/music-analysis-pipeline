from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
import os
import datetime

"""
def get_current_datetime():
    now = datetime.datetime.now()
    return '123456'
udf_get_date = func.udf(get_current_datetime, returnType=StringType())
"""

if __name__ == "__main__":

    spark = SparkSession.builder.appName("homologacion").getOrCreate()
    sc = spark.sparkContext

    """
    id, disc_number, duration, explicit, auidio_feture_id, name
    preview_url, '
    'track_numer, popularity, is_playable
    """

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("disc_number", IntegerType(), True),
        StructField("duration", IntegerType(), True),
        StructField("explicit", IntegerType(), True),
        StructField("auidio_feture_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("preview_url", StringType(), True),
        StructField("track_numer", StringType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("is_playable", StringType(), True)
    ])

    dirname = os.path.dirname(os.path.abspath(__file__))

    df = spark.read.option("header", True).format("csv").schema(schema).option("delimiter", ',').load(
        "/home/sergio/dev/python/music"
        "-analysis-pipeline/datalake"
        "/raw/tracks.csv")

    df = df.withColumn('process_date',
                       func.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))

    parquetfilename = os.path.join(dirname, 'output.parquet')

    df.write.mode('overwrite').parquet(
        '/home/sergio/dev/python/music-analysis-pipeline/datalake/parquet/tracks.parquet')

    df.show()
