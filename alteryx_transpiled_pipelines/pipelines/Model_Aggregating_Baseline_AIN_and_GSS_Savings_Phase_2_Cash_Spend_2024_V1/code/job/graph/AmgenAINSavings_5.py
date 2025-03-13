from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AmgenAINSavings_5(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Cash Spend", StringType(), True), StructField("Planning Account Description", StringType(), True), StructField("AIN_Savings", StringType(), True), StructField("ASHB", StringType(), True), StructField("Category", StringType(), True), StructField("Non-Opex Adjustments", StringType(), True), StructField("Planning Account Actual", StringType(), True), StructField("Planning Account Description Actual", StringType(), True), StructField("scenario name", StringType(), True), StructField("sub mc", StringType(), True), StructField("site", StringType(), True), StructField("Sum_Adjusted_Sum_LCL", StringType(), True), StructField("AIN_Resource_Cost", StringType(), True), StructField("year", StringType(), True), StructField("material group", StringType(), True), StructField("OSE Labor", StringType(), True), StructField("quarter", StringType(), True), StructField("Company Code", StringType(), True), StructField("Sum_Adjusted_Sum_LCLCFX", StringType(), True), StructField("Hyperion Category", StringType(), True), StructField("version", StringType(), True), StructField("scenario type", StringType(), True), StructField("Hyperion Category Code", StringType(), True), StructField("planning sku", StringType(), True), StructField("Planning Account", StringType(), True), StructField("cost center number", StringType(), True), StructField("gl account", StringType(), True), StructField("evp", StringType(), True), StructField("material group description", StringType(), True), StructField("mc", StringType(), True), StructField("hlmc", StringType(), True), StructField("regrouped level 4", StringType(), True), StructField("CTS v3", StringType(), True), StructField("cost center", StringType(), True), StructField("RecordID", StringType(), True), StructField("product description", StringType(), True), StructField("EW_Reduction", StringType(), True), StructField("Sum_Adjusted_Sum_USDAFX", StringType(), True), StructField("Company Description", StringType(), True), StructField("gl account number", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\Data for Report Builder\\AIN Savings\\Amgen AIN Savings Output.csv")
