from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_370_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("_Cumulative/Incremental").alias("Cumulative/Incremental"), 
        col("_AIN_Savings").alias("AIN_Savings"), 
        col("`_ADJ 2`").alias("ADJ 2"), 
        col("_Year").alias("Year"), 
        col("_Key").alias("Key"), 
        col("`_ADJ 1`").alias("ADJ 1"), 
        col("_EW_Reduction").alias("EW_Reduction"), 
        col("_Function").alias("Function"), 
        col("_EVP").alias("EVP"), 
        col("_HLMC").alias("HLMC"), 
        col("_AIN_Resource_Cost").alias("AIN_Resource_Cost"), 
        col("`_Cost Center`").alias("Cost Center")
    )
