from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def OPEX_FORECAST_D_5(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("pa account desc", StringType(), True), StructField("sub mc2", StringType(), True), StructField("acct level 5", StringType(), True), StructField("site detail", StringType(), True), StructField("scenario name", StringType(), True), StructField("company code", DoubleType(), True), StructField("sub mc", StringType(), True), StructField("site", StringType(), True), StructField("local currency", StringType(), True), StructField("LCL", DoubleType(), True), StructField("year", StringType(), True), StructField("category code", StringType(), True), StructField("material group", DoubleType(), True), StructField("acct level 3", DoubleType(), True), StructField("quarter", StringType(), True), StructField("company", StringType(), True), StructField("version", StringType(), True), StructField("scenario type", StringType(), True), StructField("planning sku", StringType(), True), StructField("site_nonsite", StringType(), True), StructField("cost center number", StringType(), True), StructField("gl account", DoubleType(), True), StructField("USDAFX", DoubleType(), True), StructField("evp", StringType(), True), StructField("category", StringType(), True), StructField("acct level 4", DoubleType(), True), StructField("LCLCFX", DoubleType(), True), StructField("mc", StringType(), True), StructField("hlmc", StringType(), True), StructField("_material group description", StringType(), True), StructField("regrouped level 4", StringType(), True), StructField("cost center", StringType(), True), StructField("product description", StringType(), True), StructField("PA Account", StringType(), True), StructField("gl account number", DoubleType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Cash Spend - Hyperion Mapping\\02. Source Data for the Model\\OPEX_FORECAST_DATA_MARFCST2024_FINAL (w Product).xlsx|||`Sheet1$`")
