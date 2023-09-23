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
    # sc = SparkContext(appName="CSV2Parquet")
    # sqlContext = SQLContext(sc)
    spark = SparkSession.builder.appName("homologacion").getOrCreate()
    sc = spark.sparkContext

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("album_group", StringType(), True),
        StructField("album_type", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("popularity", DecimalType(12, 0), True)
    ])

    dirname = os.path.dirname(os.path.abspath(__file__))

    # csvfilename = os.path.join(dirname, 'Temp.csv')

    df = spark.read.option("header", True).format("csv").schema(schema).option("delimiter", ',').load(
        "/home/sergio/dev/python/music"
        "-analysis-pipeline/datalake"
        "/raw/tracks.csv")

    # change column type

    df = df.withColumn('process_date',
                       func.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))
    df = df.withColumn("_release_date", func.to_timestamp(func.col("release_date") / 1000).cast(TimestampType()))
    df = df.drop('release_date')
    df = df.withColumnRenamed("_release_date", "release_date")

    parquetfilename = os.path.join(dirname, 'output.parquet')

    df.write.mode('overwrite').parquet(
        '/home/sergio/dev/python/music-analysis-pipeline/datalake/parquet/tracks.parquet')

    df.show()
