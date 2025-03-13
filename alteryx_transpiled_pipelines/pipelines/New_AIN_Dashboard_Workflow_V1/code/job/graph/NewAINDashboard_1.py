from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def NewAINDashboard_1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("# Mapped Roles - On Track", DoubleType(), True), StructField("# Mapped Roles - Complete", DoubleType(), True), StructField("A2 Capability Progress", StringType(), True), StructField("CTS", StringType(), True), StructField("# Mapped Roles - Delayed", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\New AIN Dashboard Data\\New AIN Dashboard Data - Alteryx.xlsx|||`A2 Capability Progress$`")
