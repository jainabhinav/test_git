from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CopyofLRPPlanni_49(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Name", StringType(), True), StructField("DRM_GrandParent_Descr", StringType(), True), StructField("Description", StringType(), True), StructField("Parent Description", StringType(), True), StructField("DRM_GrandParent", StringType(), True), StructField("DRM_Great_GrandParent", StringType(), True), StructField("Parent Node", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Cash Spend - Hyperion Mapping\\02. Source Data for the Model\\Copy of LRP Planning Acct to Category Mapping 2024 SepFcst 1.xlsx|||`Sheet1$`")
