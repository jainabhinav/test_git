from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_58(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("EVP"), 
        col("HLMC"), 
        col("Value").alias("Savings"), 
        col("Name1").alias("Saving Type"), 
        col("Name2").alias("Year"), 
        col("Name3").alias("Cumulative/Incremental")
    )
