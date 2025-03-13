from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_139(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("Sum_AIN_Savings"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("EVP"), 
        col("HLMC"), 
        lit("No Savings").cast(StringType()).alias("Flag")
    )
