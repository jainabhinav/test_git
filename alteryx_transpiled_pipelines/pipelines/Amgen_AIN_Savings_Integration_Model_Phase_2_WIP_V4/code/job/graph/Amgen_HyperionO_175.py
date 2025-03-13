from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Amgen_HyperionO_175(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("pa account desc", StringType(), True), StructField("Category", StringType(), True), StructField("scenario name", StringType(), True), StructField("company code", StringType(), True), StructField("sub mc", StringType(), True), StructField("site", StringType(), True), StructField("Sum_Adjusted_Sum_LCL", StringType(), True), StructField("year", StringType(), True), StructField("material group", StringType(), True), StructField("quarter", StringType(), True), StructField("Sum_Adjusted_Sum_LCLCFX", StringType(), True), StructField("hyperion category code", StringType(), True), StructField("company", StringType(), True), StructField("version", StringType(), True), StructField("scenario type", StringType(), True), StructField("planning sku", StringType(), True), StructField("cost center number", StringType(), True), StructField("gl account", StringType(), True), StructField("hyperion category", StringType(), True), StructField("evp", StringType(), True), StructField("pa account", StringType(), True), StructField("material group description", StringType(), True), StructField("mc", StringType(), True), StructField("hlmc", StringType(), True), StructField("regrouped level 4", StringType(), True), StructField("cost center", StringType(), True), StructField("product description", StringType(), True), StructField("Sum_Adjusted_Sum_USDAFX", StringType(), True), StructField("gl account number", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Savings Ingestion\\AIN Savings\\02. Source Data for the Model\\Phase 2\\Amgen_Hyperion Output Serving as Input for AIN Savings Model.csv")
