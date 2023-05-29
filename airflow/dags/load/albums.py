from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import os


if __name__ == "__main__":

    # sc = SparkContext(appName="CSV2Parquet")
    # sqlContext = SQLContext(sc)
    spark = SparkSession.builder.appName("homologacion").getOrCreate()
    sc = spark.sparkContext

    schema = StructType([
                StructField("col1", StringType(), True),
                StructField("col2", StringType(), True),
                StructField("col3", StringType(), True),
                StructField("col4", StringType(), True),
                StructField("col5", StringType(), True),
                StructField("col6", StringType(), True)                     
            ])
    
    dirname = os.path.dirname(os.path.abspath(__file__))

    csvfilename = os.path.join(dirname,'Temp.csv')   

    # rdd = sc.textFile('/home/sergio/dev/python/music-analysis-pipeline/datalake/raw/albums.csv').map(lambda line:
    # line.split(",")) df = sqlContext.createDataFrame(rdd, schema)
    df = spark.read.option("header", True).format("csv").option("delimiter", ',').load("/home/sergio/dev/python/music"
                                                                                       "-analysis-pipeline/datalake/raw/albums.csv")

    parquetfilename = os.path.join(dirname,'output.parquet')   

    df.write.mode('overwrite').parquet('/home/sergio/dev/python/music-analysis-pipeline/datalake/parquet/albums.parquet') 
