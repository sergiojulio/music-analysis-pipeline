# -*- coding: utf-8 -*-
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as func  # este alias es para evitar sobre escritura de funciones
from pyspark.sql.types import IntegerType, StringType

spark = SparkSession.builder.appName("homologacion").getOrCreate()
sc = spark.sparkContext

df = spark.read.option("header", True).format("csv").option("delimiter", ',').load("/home/sergio/dev/python/music-analysis-pipeline/inputs/scrobbles-radioheadve-1679014882.csv")

df.groupBy('artist').count().orderBy('count', ascending=False).show()
