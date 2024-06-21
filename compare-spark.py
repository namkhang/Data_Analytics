

import numpy as np
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession , Row
from pyspark.sql.types import *
from pyspark.sql.functions import col , when , lit , udf ,to_timestamp , monotonically_increasing_id 
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-21"
os.environ["SPARK_HOME"] = "C:\spark-3.5.0-bin-hadoop3"



spark_sql = SparkSession.builder \
.appName('spark-df') \
.config("spark.master", "spark://pre-spark-01-swi.hitt.hre.local:7077,pre-spark-02-swi.hitt.hre.local:7077,pre-spark-03-swi.hitt.hre.local:7077") \
.config('spark.driver.memory' , '2g') \
.config('spark.executor.instances' , '5') \
.config('spark.executor.cores' , '1') \
.config('spark.executor.memory' , '1g') \
.getOrCreate() 

data_err_1 = spark_sql.read.option("header",True).csv("product_raw_data_part1.csv")
data_err_2 = spark_sql.read.option("header",True).csv("product_raw_data_part2.csvv")
data = data_err_1.union(data_err_2).distinct()
data.show()