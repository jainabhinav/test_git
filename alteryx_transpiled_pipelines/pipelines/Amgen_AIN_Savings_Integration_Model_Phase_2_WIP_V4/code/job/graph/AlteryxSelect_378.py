from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_378(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`AIN Savings Removed`").cast(DoubleType()).alias("AIN Savings Removed"), 
        col("year"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("Category"), 
        col("hlmc"), 
        col("`EW Savings Removed`").cast(DoubleType()).alias("EW Savings Removed")
    )
