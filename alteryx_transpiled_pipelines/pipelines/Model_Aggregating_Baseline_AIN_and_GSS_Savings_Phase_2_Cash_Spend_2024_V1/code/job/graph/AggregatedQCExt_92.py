from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AggregatedQCExt_92(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .mode("append")\
        .option("separator", "")\
        .option("header", False)\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\Data for Report Builder\\Aggregated\\Aggregated QC Extract with 2024 V1.xlsx|||Sheet1")
