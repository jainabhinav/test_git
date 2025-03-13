from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_388_variable2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function").alias("_Function"), 
        col("`Adjustment Required?`").alias("_Adjustment Required?"), 
        col("`tableau display mega category`").alias("_tableau display mega category"), 
        col("HLMC").alias("_HLMC"), 
        col("Year").alias("_Year"), 
        col("`Cost Center`").alias("_Cost Center"), 
        col("Sum_AIN_Savings").alias("_Sum_AIN_Savings"), 
        col("EVP").alias("_EVP")
    )
