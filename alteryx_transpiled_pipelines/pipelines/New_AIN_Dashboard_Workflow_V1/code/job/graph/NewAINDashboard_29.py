from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def NewAINDashboard_29(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .mode("overwrite")\
        .option("separator", "")\
        .option("header", False)\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\New AIN Dashboard Data\\New AIN Dashboard Data - Alteryx.xlsx|||Asset Status and Impact")
