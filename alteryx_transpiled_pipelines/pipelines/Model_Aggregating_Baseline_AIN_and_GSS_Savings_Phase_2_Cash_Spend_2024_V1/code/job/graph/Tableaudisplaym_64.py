from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Tableaudisplaym_64(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("tableau display mega category", StringType(), True), StructField("Sum_Sum_Spend ($)", DoubleType(), True), StructField("tableau display category", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - 08.15 Baseline Analytics Engine and Dashboards\\Data Shared by ACN\\Model Aggregating Baseline, AIN and GSS Savings\\02. Source Data for the Model\\Phase 2\\Tableau display mega - Tableau display category Mapping.xlsx|||`Sheet1$`")
