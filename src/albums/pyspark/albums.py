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
                StructField("release_date", StringType(), True),  # change to date time
                StructField("popularity", DecimalType(12, 0), True)
            ])
    
    dirname = os.path.dirname(os.path.abspath(__file__))

    # csvfilename = os.path.join(dirname, 'Temp.csv')

    df = spark.read.option("header", True).format("csv").schema(schema).option("delimiter", ',').load("/home/sergio/dev/python/music"
                                                                                       "-analysis-pipeline/datalake"
                                                                                       "/raw/albums.csv")

    # change column type
    """
    ERROR Utils: Aborting task
    org.apache.spark.SparkUpgradeException: You may get a different result due to the upgrading of Spark 3.0: 
    writing dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z into Parquet INT96
    files can be dangerous, as the files may be read by Spark 2.x or legacy versions of Hive
    later, which uses a legacy hybrid calendar that is different from Spark 3.0+'s Proleptic
    Gregorian calendar. See more details in SPARK-31404. You can set spark.sql.parquet.int96RebaseModeInWrite to 'LEGACY' to
    rebase the datetime values w.r.t. the calendar difference during writing, to get maximum
    interoperability. Or set spark.sql.parquet.int96RebaseModeInWrite to 'CORRECTED' to write the datetime values as it is,
    if you are 100% sure that the written files will only be read by Spark 3.0+ or other
    systems that use Proleptic Gregorian calendar.    
    """

    df = df.withColumn('process_date', func.lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))
    df = df.withColumn("_release_date", func.to_timestamp(func.col("release_date") / 1000).cast(TimestampType()))
    df = df.drop('release_date')
    df = df.withColumnRenamed("_release_date", "release_date")

    parquetfilename = os.path.join(dirname, 'output.parquet')

    df.write.mode('overwrite').parquet('/home/sergio/dev/python/music-analysis-pipeline/datalake/parquet/albums.parquet')

    df.show()
