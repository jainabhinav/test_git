from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Amgen_CashSpend_2(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("company code number", StringType(), True), StructField("source_cost center", StringType(), True), StructField("source_accounting date - quarter", StringType(), True), StructField("Category", StringType(), True), StructField("Gl Account", StringType(), True), StructField("Category Old", StringType(), True), StructField("GL", StringType(), True), StructField("Gl Account (# only)", StringType(), True), StructField("tableau display mega category", StringType(), True), StructField("PA#", StringType(), True), StructField("hyperion evp", StringType(), True), StructField("Planning Account", StringType(), True), StructField("Sum_Sum_Spend ($)", StringType(), True), StructField("source_cost center number", StringType(), True), StructField("Regrouped Level 4", StringType(), True), StructField("source_accounting date - year", StringType(), True), StructField("Mega Category Old", StringType(), True), StructField("hyperion hlmc", StringType(), True), StructField("hyperion mc", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\Data for Report Builder\\Amgen_Cash Spend Output Serving as Input for AIN Savings Model.csv")
