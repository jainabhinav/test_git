from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Amgen_HyperionO_325(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .mode("append")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Savings Ingestion\\AIN Savings\\02. Source Data for the Model\\Phase 2\\Amgen_Hyperion Output Serving as Input for AIN Savings Model.csv")
