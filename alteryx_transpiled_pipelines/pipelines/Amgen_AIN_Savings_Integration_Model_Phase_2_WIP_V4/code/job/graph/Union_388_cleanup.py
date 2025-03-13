from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_388_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_Adjustment Required?`").alias("Adjustment Required?"), 
        col("_Year").alias("Year"), 
        col("_Sum_AIN_Savings").alias("Sum_AIN_Savings"), 
        col("`_tableau display mega category`").alias("tableau display mega category"), 
        col("_Function").alias("Function"), 
        col("_EVP").alias("EVP"), 
        col("_HLMC").alias("HLMC"), 
        col("`_Cost Center`").alias("Cost Center")
    )
