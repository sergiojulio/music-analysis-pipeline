from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
import os
import datetime


def get_current_datetime():
    now = datetime.datetime.now()
    return '123456'


udf_get_date = func.udf(get_current_datetime, returnType=StringType())


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
                StructField("release_date", IntegerType(), True),  # change to date time
                StructField("popularity", IntegerType(), True)
            ])
    
    dirname = os.path.dirname(os.path.abspath(__file__))

    # csvfilename = os.path.join(dirname, 'Temp.csv')

    df = spark.read.option("header", True).format("csv").schema(schema).option("delimiter", ',').load("/home/sergio/dev/python/music"
                                                                                       "-analysis-pipeline/datalake"
                                                                                       "/raw/albums_100.csv")

    # create a colum with process date
    print(datetime.datetime.now())

    df = df.withColumn('process_date', lit(datetime.datetime.now()).cast(StringType()))

    parquetfilename = os.path.join(dirname, 'output.parquet')

    df.write.mode('overwrite').parquet('/home/sergio/dev/python/music-analysis-pipeline/datalake/parquet/albums.parquet')

    df.show()

# PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON