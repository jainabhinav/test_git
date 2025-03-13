from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CashSpendCTSV3L_12(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("GL Number", StringType(), True), StructField("Category", StringType(), True), StructField("Gl Account", StringType(), True), StructField("F7", DoubleType(), True), StructField("CC Lookup", DoubleType(), True), StructField("Planning Account", StringType(), True), StructField("F6", DoubleType(), True), StructField("CTS Category", StringType(), True), StructField("CTS Mapping", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Savings Ingestion\\AIN Savings\\02. Source Data for the Model\\Cash Spend CTS V3 Lookup.xlsx|||`Sheet1$`")
