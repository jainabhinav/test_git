from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AmgenSavingsSum_46(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .mode("overwrite")\
        .option("separator", "")\
        .option("header", False)\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Model Aggregating Baseline, AIN and GSS Savings\\04. Error Detection File\\Phase 2\\Amgen Savings Summarized Report.xlsx|||GSS Savings Granlular$B1:L999999")
