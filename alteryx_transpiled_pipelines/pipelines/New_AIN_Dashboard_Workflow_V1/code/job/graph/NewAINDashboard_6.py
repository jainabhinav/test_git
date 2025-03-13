from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def NewAINDashboard_6(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Hired Mapped Roles Filled", DoubleType(), True), StructField("Function", StringType(), True), StructField("Roles Mapped to EW / OrgVue Data", DoubleType(), True), StructField("Roles Posted", DoubleType(), True), StructField("QA Check: Map. Rol. Neg. by Function = A2", BooleanType(), True), StructField("Asset Status and Impact - Complete", DoubleType(), True), StructField("QA Check: Negotiate Total = Mapped", BooleanType(), True), StructField("Asset Status and Impact - Not Started", DoubleType(), True), StructField("Group", DoubleType(), True), StructField("Hired Mapped Roles Unfilled", DoubleType(), True), StructField("Mapped Roles to Negotiate - Delayed", DoubleType(), True), StructField("Roles Unmapped to EW / OrgVue Data", DoubleType(), True), StructField("Impacted Suppliers - Core", DoubleType(), True), StructField("Mapped Roles to Negotiate - Complete", DoubleType(), True), StructField("Roles Posted as of Date", TimestampType(), True), StructField("Impacted Suppliers - Tail", DoubleType(), True), StructField("QA Check: Mapped + Unmapped = Posted?", BooleanType(), True), StructField("Mapped Roles to Negotiate - On Track", DoubleType(), True), StructField("QA Check: Filled + Unfilled = Mapped", BooleanType(), True), StructField("Asset Status and Impact - In Progress", DoubleType(), True), StructField("Impacted Suppliers - Essential", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("C:\\Users\\nikhil.f.bansal\\Amgen\\Amgen India - Baseline Analytics - Baseline Analytics\\Working Files\\Nikhil's WIP Files\\New AIN Dashboard Data\\New AIN Dashboard Data - Alteryx.xlsx|||`Function Summary$`")
