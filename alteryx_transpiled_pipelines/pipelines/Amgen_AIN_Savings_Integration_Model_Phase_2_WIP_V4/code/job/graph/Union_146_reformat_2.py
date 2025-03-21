from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_146_reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`% Split`").alias("% Split"), 
        col("`ADJ 1`").alias("ADJ 1"), 
        col("`ADJ 2`").alias("ADJ 2"), 
        col("AIN_Resource_Cost"), 
        col("AIN_Savings"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("Cumulative/Incremental"), 
        col("EVP"), 
        col("EW_Reduction"), 
        col("Flag"), 
        col("Function"), 
        col("HLMC"), 
        col("Key"), 
        col("Right_Right_Key").cast(StringType()).alias("Right_Right_Key"), 
        col("`Right_Right_tableau display mega category`")\
          .cast(StringType())\
          .alias("Right_Right_tableau display mega category"), 
        col("Year"), 
        col("`tableau display mega category`").alias("tableau display mega category")
    )
