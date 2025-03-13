from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_217(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`ADJ 1`").alias("ADJ 1"), 
        col("`Multiplying Factor`").alias("Multiplying Factor"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("Cumulative/Incremental"), 
        col("Year"), 
        col("EVP"), 
        col("AIN_Resource_Cost"), 
        col("`% Split`").alias("% Split"), 
        col("`ADJ 2`").alias("ADJ 2"), 
        col("EW_Reduction"), 
        col("AIN_Savings"), 
        col("`Sum_% Split`").alias("Sum_% Split"), 
        col("Function"), 
        col("Key"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("HLMC")
    )
