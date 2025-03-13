from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SavingsbyMegaCa_1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Incremental Savings High", DoubleType(), True), StructField("Mega Category", StringType(), True), StructField("Year", DoubleType(), True), StructField("F7", DoubleType(), True), StructField("F6", DoubleType(), True), StructField("BAU Savings", DoubleType(), True), StructField("F8", DoubleType(), True), StructField("Incremental Savings Low", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Savings Ingestion\\GSS Enterprise Savings\\02. Source Data for the Model\\Savings by Mega Category across year.xlsx|||`Sheet1$`")
