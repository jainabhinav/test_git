from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_350(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("AIN_Savings"), 
        col("`Right_tableau display mega category`").alias("Right_tableau display mega category"), 
        col("`ADJ 3`").alias("ADJ 3"), 
        col("AIN_Resource_Cost"), 
        col("`Right_ADJ 1`").alias("Right_ADJ 1"), 
        col("Year"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`ADJ 2`").alias("ADJ 2"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("`Right_ADJ 2`").alias("Right_ADJ 2"), 
        col("EVP"), 
        col("`ADJ 1`").alias("ADJ 1"), 
        col("Cumulative/Incremental"), 
        col("HLMC"), 
        col("EW_Reduction"), 
        col("`Right_ADJ 3`").alias("Right_ADJ 3"), 
        col("Key"), 
        lit("Function + Mega Category").cast(StringType()).alias("Adjustment Required?")
    )
