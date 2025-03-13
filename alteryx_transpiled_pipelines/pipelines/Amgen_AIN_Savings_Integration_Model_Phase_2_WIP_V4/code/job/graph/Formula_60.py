from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_60(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Function",
          when((col("Function") == lit("PV")).cast(BooleanType()), lit("Pharmacovigilance"))\
            .when((col("Function") == lit("HR")).cast(BooleanType()), lit("Human Resources"))\
            .otherwise(col("Function"))\
            .cast(StringType())
        )\
        .withColumn("Cost Center", call_spark_fcn("string_substring", col("`Cost Center`"), lit(1), lit(5)).cast(StringType()))\
        .withColumn("Key", (((col("Function") + col("EVP")) + col("HLMC")) + col("`Cost Center`")).cast(StringType()))\
        .withColumn(
          "AIN_Resource_Cost",
          when(col("AIN_Resource_Cost").isNull().cast(BooleanType()), lit(0))\
            .otherwise(col("AIN_Resource_Cost"))\
            .cast(DoubleType())
        )\
        .withColumn(
          "AIN_Savings",
          when(col("AIN_Savings").isNull().cast(BooleanType()), lit(0)).otherwise(col("AIN_Savings")).cast(DoubleType())
        )\
        .withColumn("EW_Reduction", when(col("EW_Reduction").isNull().cast(BooleanType()), lit(0))\
        .otherwise(col("EW_Reduction"))\
        .cast(DoubleType()))
