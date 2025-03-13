from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CashSpend2024da_98(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Cash Spend", StringType(), True), StructField("Planning Account Description", StringType(), True), StructField("_version", StringType(), True), StructField("OSE Labor V2", StringType(), True), StructField("ASHB", StringType(), True), StructField("_planning sku", StringType(), True), StructField("Non-Opex Adjustments", StringType(), True), StructField("_AIN_Resource_Cost", StringType(), True), StructField("scenario name", StringType(), True), StructField("company code", StringType(), True), StructField("sub mc", StringType(), True), StructField("_scenario type", StringType(), True), StructField("year", StringType(), True), StructField("_GSS BAU Savings", StringType(), True), StructField("material group", StringType(), True), StructField("tableau display category", StringType(), True), StructField("quarter", StringType(), True), StructField("Sum_Adjusted_Sum_LCLCFX", StringType(), True), StructField("_AIN EW_Reduction", StringType(), True), StructField("_Hyperion Category", StringType(), True), StructField("Company Group", StringType(), True), StructField("_GSS Incremental Low Savings", StringType(), True), StructField("Planning Account", StringType(), True), StructField("cost center number", StringType(), True), StructField("_Sum_Adjusted_Sum_LCL", StringType(), True), StructField("gl account", StringType(), True), StructField("_product description", StringType(), True), StructField("evp", StringType(), True), StructField("Regrouped Level 4", StringType(), True), StructField("category", StringType(), True), StructField("material group description", StringType(), True), StructField("_AIN_Savings", StringType(), True), StructField("mc", StringType(), True), StructField("hlmc", StringType(), True), StructField("Site", StringType(), True), StructField("_GSS Incremental High Savings", StringType(), True), StructField("CTS v3", StringType(), True), StructField("cost center", StringType(), True), StructField("In-Scope Final", StringType(), True), StructField("_Hyperion Category Code", StringType(), True), StructField("_Sum_Adjusted_Sum_USDAFX", StringType(), True), StructField("Company Description", StringType(), True), StructField("gl account number", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\Data for Report Builder\\Cash Spend 2024\\Cash Spend 2024 data.csv")
