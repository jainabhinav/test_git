from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_305(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "AIN_Resource_Cost",
          when(
              ((col("Sum_AIN_Savings") <= lit(2000)) | (col("Sum_EW_Reduction") <= lit(2000))).cast(BooleanType()), 
              lit(0)
            )\
            .otherwise(col("AIN_Resource_Cost"))\
            .cast(DoubleType())
        )\
        .withColumn(
          "AIN_Savings",
          when(
              ((col("Sum_AIN_Savings") <= lit(2000)) | (col("Sum_EW_Reduction") <= lit(2000))).cast(BooleanType()), 
              lit(0)
            )\
            .otherwise(col("AIN_Savings"))\
            .cast(DoubleType())
        )\
        .withColumn("EW_Reduction", when(((col("Sum_AIN_Savings") <= lit(2000)) | (col("Sum_EW_Reduction") <= lit(2000))).cast(BooleanType()), lit(0))\
        .otherwise(col("EW_Reduction"))\
        .cast(DoubleType()))
