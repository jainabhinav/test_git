from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_388_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_Adjustment Required?`").alias("_Adjustment Required?"), 
        col("`_Cost Center`").alias("_Cost Center"), 
        col("_EVP"), 
        col("_Function"), 
        col("_HLMC"), 
        col("_Sum_AIN_Savings"), 
        col("_Year"), 
        col("`_tableau display mega category`").alias("_tableau display mega category")
    )
