from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Amgen_CashSpend_297(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .mode("append")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\Data for Report Builder\\Amgen_Cash Spend Output Serving as Input for AIN Savings Model.csv")
