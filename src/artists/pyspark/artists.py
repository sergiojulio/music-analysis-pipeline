from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
import os
import datetime


if __name__ == "__main__":
    # sc = SparkContext(appName="CSV2Parquet")
    # sqlContext = SQLContext(sc)
    # sc = SparkContext(appName="CSV2Parquet")
    # sqlContext = SQLContext(sc)
    spark = SparkSession.builder.appName("homologacion").getOrCreate()
    sc = spark.sparkContext

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("followers", IntegerType(), True),
    ])

    dirname = os.path.dirname(os.path.abspath(__file__))

    df = spark.read.option("header", True).format("csv").schema(schema).option("delimiter", ',').load(
        "/home/sergio/dev/python/music"
        "-analysis-pipeline/datalake"
        "/raw/artists.csv")

    df = df.withColumn('process_date',
                       func.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))

    parquetfilename = os.path.join(dirname, 'output.parquet')

    df.write.mode('overwrite').parquet(
        '/home/sergio/dev/python/music-analysis-pipeline/datalake/parquet/artists.parquet')

    df.show()
