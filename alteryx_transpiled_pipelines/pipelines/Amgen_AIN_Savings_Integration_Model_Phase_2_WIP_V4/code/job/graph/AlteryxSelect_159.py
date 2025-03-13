from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_159(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`ADJ 1`").alias("ADJ 1"), 
        col("`Right_Right_tableau display mega category`").alias("Right_Right_tableau display mega category"), 
        col("Flag"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("Cumulative/Incremental"), 
        col("Year"), 
        col("`Right_ADJ 2`").alias("Right_ADJ 2"), 
        col("EVP"), 
        col("AIN_Resource_Cost"), 
        col("`ADJ 2`").alias("ADJ 2"), 
        col("EW_Reduction"), 
        col("AIN_Savings"), 
        col("Function"), 
        col("Key"), 
        col("`Right_ADJ 1`").alias("Right_ADJ 1"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("HLMC"), 
        col("`Right_Right_Right_tableau display mega category`")\
          .alias("Right_Right_Right_tableau display mega category"), 
        col("`Avg_% Split`").alias("% Split")
    )
