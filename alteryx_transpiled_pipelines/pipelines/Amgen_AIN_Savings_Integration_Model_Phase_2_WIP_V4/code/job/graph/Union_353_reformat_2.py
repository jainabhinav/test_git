from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_353_reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`ADJ 1`").alias("ADJ 1"), 
        col("`ADJ 2`").alias("ADJ 2"), 
        col("`ADJ 3`").alias("ADJ 3"), 
        col("AIN_Resource_Cost"), 
        col("AIN_Savings"), 
        col("`Adjustment Required?`").cast(StringType()).alias("Adjustment Required?"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("Cumulative/Incremental"), 
        col("EVP"), 
        col("EW_Reduction"), 
        col("Function"), 
        col("HLMC"), 
        col("Key"), 
        col("`Right_ADJ 1`").cast(StringType()).alias("Right_ADJ 1"), 
        col("`Right_ADJ 2`").cast(StringType()).alias("Right_ADJ 2"), 
        col("`Right_ADJ 3`").cast(StringType()).alias("Right_ADJ 3"), 
        lit(None).cast(StringType()).alias("Right_Key"), 
        col("`Right_tableau display mega category`").cast(StringType()).alias("Right_tableau display mega category"), 
        col("Year"), 
        col("`tableau display mega category`").alias("tableau display mega category")
    )
