from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
import os
import datetime

if __name__ == "__main__":

    spark = SparkSession.builder.appName("homologacion").getOrCreate()
    sc = spark.sparkContext

    schema = StructType([
        StructField("id", StringType(), True)
    ])

    dirname = os.path.dirname(os.path.abspath(__file__))

    df = spark.read.option("header", True).format("csv").schema(schema).option("delimiter", ',').load(
        "/home/sergio/dev/python/music"
        "-analysis-pipeline/datalake"
        "/raw/genres.csv")

    # hash id
    # table['id'] = abs(table['id'].str.decode('utf8').apply(hash))
    df = df.withColumn('name', func.col("id"))
    df = df.withColumn("_id", func.abs(func.hash(func.col("id"))))
    df = df.drop('id')
    df = df.withColumnRenamed("_id", "id")

    df = df.withColumn('process_date',
                       func.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))

    parquetfilename = os.path.join(dirname, 'output.parquet')

    df.write.mode('overwrite').parquet(
        '/home/sergio/dev/python/music-analysis-pipeline/datalake/parquet/genres.parquet')

    df.show()
