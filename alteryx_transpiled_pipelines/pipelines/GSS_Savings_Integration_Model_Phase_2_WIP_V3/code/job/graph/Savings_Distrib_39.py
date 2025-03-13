from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Savings_Distrib_39(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Savings Type", StringType(), True), StructField("Operations-2027", DoubleType(), True), StructField("R&D/MA-2025", DoubleType(), True), StructField("G&A-2025", DoubleType(), True), StructField("R&D/MA-2026", DoubleType(), True), StructField("Mega Category", StringType(), True), StructField("Other-2025", DoubleType(), True), StructField("G&A-2026", DoubleType(), True), StructField("S&M/Int'l-2027", DoubleType(), True), StructField("Other-2026", DoubleType(), True), StructField("Operations-2025", DoubleType(), True), StructField("S&M/Int'l-2026", DoubleType(), True), StructField("R&D/MA-2027", DoubleType(), True), StructField("F18", DoubleType(), True), StructField("G&A-2027", DoubleType(), True), StructField("Operations-2026", DoubleType(), True), StructField("S&M/Int'l-2025", DoubleType(), True), StructField("Other-2027", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Savings Ingestion\\GSS Enterprise Savings\\02. Source Data for the Model\\Savings % Distribution by EVP.xlsx|||`Incremental$`")
