from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_230_rename(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Cumulative/Incremental"), 
        col("AIN_Savings_Sum").alias("AIN_Savings"), 
        col("Year"), 
        col("EW_Reduction_Sum").alias("EW_Reduction"), 
        col("Function"), 
        col("AIN_Resource_Cost_Sum").alias("AIN_Resource_Cost")
    )
