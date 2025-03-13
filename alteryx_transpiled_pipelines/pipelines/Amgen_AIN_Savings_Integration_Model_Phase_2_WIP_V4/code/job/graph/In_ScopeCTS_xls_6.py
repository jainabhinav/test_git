from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def In_ScopeCTS_xls_6(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Function as per AIN Business Case Team", StringType(), True), StructField("CTS V3", StringType(), True), StructField("In-Scope?", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Savings Ingestion\\AIN Savings\\02. Source Data for the Model\\In-Scope CTS.xlsx|||`Sheet1$`")
