from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_342(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Cumulative/Incremental"), 
        col("AIN_Savings"), 
        col("`ADJ 2`").alias("ADJ 2"), 
        col("Year"), 
        col("Key"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`ADJ 1`").alias("ADJ 1"), 
        col("EW_Reduction"), 
        col("Function"), 
        col("EVP"), 
        col("HLMC"), 
        col("AIN_Resource_Cost"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("`ADJ 3`").alias("ADJ 3")
    )
