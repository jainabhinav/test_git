from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_370_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(None).cast(StringType()).alias("_ADJ 1"), 
        lit(None).cast(StringType()).alias("_ADJ 2"), 
        col("_AIN_Resource_Cost").cast(StringType()).alias("_AIN_Resource_Cost"), 
        col("_AIN_Savings").cast(StringType()).alias("_AIN_Savings"), 
        lit(None).cast(StringType()).alias("_Cost Center"), 
        col("_Cumulative/Incremental"), 
        lit(None).cast(StringType()).alias("_EVP"), 
        col("_EW_Reduction").cast(StringType()).alias("_EW_Reduction"), 
        col("_Function"), 
        lit(None).cast(StringType()).alias("_HLMC"), 
        lit(None).cast(StringType()).alias("_Key"), 
        col("_Year")
    )
