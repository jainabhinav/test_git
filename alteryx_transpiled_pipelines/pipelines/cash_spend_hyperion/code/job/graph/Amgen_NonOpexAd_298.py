from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Amgen_NonOpexAd_298(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("2026 Non-Opex Adjustment", DoubleType(), True), StructField("2024 Non-Opex Adjustment", DoubleType(), True), StructField("2027 Non-Opex Adjustment", DoubleType(), True), StructField("Mega Category", StringType(), True), StructField("F6", DoubleType(), True), StructField("2025 Non-Opex Adjustment", DoubleType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Cash Spend - Hyperion Mapping\\02. Source Data for the Model\\Amgen_Non Opex Adjustments_Mapping.xlsx|||`Sheet1$`")
