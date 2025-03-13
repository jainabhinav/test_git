from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_65(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("AIN_Savings"), 
        col("AIN_Resource_Cost"), 
        col("Year"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("EVP"), 
        col("Cumulative/Incremental"), 
        col("HLMC"), 
        col("EW_Reduction"), 
        col("Key"), 
        (col("Function") + col("HLMC")).cast(StringType()).alias("ADJ 1"), 
        (col("Function") + col("EVP")).cast(StringType()).alias("ADJ 2")
    )
