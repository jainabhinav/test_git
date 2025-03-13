from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CashSpendASHBLo_9(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("CC Lookup", DoubleType(), True), StructField("Department Code", StringType(), True), StructField("Shb", StringType(), True), StructField("EPM_ESS_Function_ASHB_Attr", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Savings Ingestion\\AIN Savings\\02. Source Data for the Model\\Cash Spend ASHB Lookup.xlsx|||`Sheet1$`")
