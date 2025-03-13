from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AmgenNon_Addres_85(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("pa account desc", StringType(), True), StructField("_Planning Account Description Actual", StringType(), True), StructField("_AIN_Resource_Cost (Realised)", StringType(), True), StructField("_Planning Account Actual", StringType(), True), StructField("_GSS Incremental High Savings (Realised)", StringType(), True), StructField("_AIN EW_Reduction (Realised)", StringType(), True), StructField("_ASHB", StringType(), True), StructField("_In-Scope", StringType(), True), StructField("_AIN_Resource_Cost", StringType(), True), StructField("Sum_LCL", StringType(), True), StructField("scenario name", StringType(), True), StructField("company code", StringType(), True), StructField("sub mc", StringType(), True), StructField("site", StringType(), True), StructField("_CTS V3", StringType(), True), StructField("_tableau display category", StringType(), True), StructField("year", StringType(), True), StructField("_GSS BAU Savings", StringType(), True), StructField("category code", StringType(), True), StructField("material group", StringType(), True), StructField("quarter", StringType(), True), StructField("_GSS BAU Savings (Realised)", StringType(), True), StructField("_AIN EW_Reduction", StringType(), True), StructField("company", StringType(), True), StructField("_AIN Ext. Lab. Net Savings Stage", StringType(), True), StructField("version", StringType(), True), StructField("scenario type", StringType(), True), StructField("_OSE Labor V2", StringType(), True), StructField("_AIN_Savings (Realised)", StringType(), True), StructField("planning sku", StringType(), True), StructField("_GSS Incremental Low Savings", StringType(), True), StructField("cost center number", StringType(), True), StructField("gl account", StringType(), True), StructField("_Non-Opex Adjustments", StringType(), True), StructField("evp", StringType(), True), StructField("pa account", StringType(), True), StructField("Sum_LCLCFX", StringType(), True), StructField("category", StringType(), True), StructField("material group description", StringType(), True), StructField("_AIN_Savings", StringType(), True), StructField("mc", StringType(), True), StructField("_GSS Incremental Low Savings (Realised)", StringType(), True), StructField("hlmc", StringType(), True), StructField("regrouped level 4", StringType(), True), StructField("_GSS Incremental High Savings", StringType(), True), StructField("_GSS Enterprise Savings", StringType(), True), StructField("Exclusion Type", StringType(), True), StructField("cost center", StringType(), True), StructField("product description", StringType(), True), StructField("Sum_USDAFX", StringType(), True), StructField("gl account number", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\Data for Report Builder\\Amgen Non-Addressable Data.csv")
