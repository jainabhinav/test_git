from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CashSpendOSELab_82(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Legal", StringType(), True), StructField("GL", DoubleType(), True), StructField("Description", StringType(), True), StructField("Supply Chain", StringType(), True), StructField("Regulatory", StringType(), True), StructField("G&A Other", StringType(), True), StructField("Pharmacovigilance", StringType(), True), StructField("Quality", StringType(), True), StructField("Compliance", StringType(), True), StructField("CfOR", StringType(), True), StructField("R&D - Others", StringType(), True), StructField("Human Resources", StringType(), True), StructField("GCO", StringType(), True), StructField("Ops - Process Development", StringType(), True), StructField("Others", StringType(), True), StructField("Procurement", StringType(), True), StructField("Mfg", StringType(), True), StructField("Finance", StringType(), True), StructField("Tech", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Savings Ingestion\\AIN Savings\\02. Source Data for the Model\\Cash Spend OSE Labor Lookup.xlsx|||`Sheet1$`")
