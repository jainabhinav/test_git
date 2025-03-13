from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_322(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Cost Center Exists in Baseline?",
          when(col("`cost center number`").isNull().cast(BooleanType()), lit("No"))\
            .otherwise(lit("Yes"))\
            .cast(StringType())
        )\
        .withColumn(
          "Cost Center Missing in Baseline?",
          when((col("`Cost Center Exists in Baseline?`") == lit("Yes")).cast(BooleanType()), lit("No"))\
            .otherwise(lit("Yes"))\
            .cast(StringType())
        )\
        .withColumn(
          "Hyperion CTS <> AIN Input CTS",
          when((col("`CTS Mapped in Data`") == col("Function")).cast(BooleanType()), lit("No"))\
            .otherwise(lit("Yes"))\
            .cast(StringType())
        )\
        .withColumn(
          "OSE Labor V2 <> Labor?",
          when((col("`OSE Labor V2 Mapped in Data`") == lit("Labor OSE")).cast(BooleanType()), lit("No"))\
            .otherwise(lit("Yes"))\
            .cast(StringType())
        )\
        .withColumn(
          "ASHB = 0 or 'Not Found'?",
          when(
              ((col("`ASHB Mapped in Data`") == lit("0")) | col("`ASHB Mapped in Data`").isNull()).cast(BooleanType()), 
              lit("Yes")
            )\
            .otherwise(lit("No"))\
            .cast(StringType())
        )\
        .withColumn("Unique Key", ((((col("Function") + col("EVP")) + col("HLMC")) + col("`Cost Center`")) + col("Year")).cast(StringType()))
