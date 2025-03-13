from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Amgen_CashSpend_206(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .mode("overwrite")\
        .option("separator", "")\
        .option("header", False)\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Cash Spend - Hyperion Mapping\\04. Error Detection File\\Phase 2\\Amgen_Cash Spend to Hyperion Mapping_Error Detection.xlsx|||LRP Spend$B4:C200")
