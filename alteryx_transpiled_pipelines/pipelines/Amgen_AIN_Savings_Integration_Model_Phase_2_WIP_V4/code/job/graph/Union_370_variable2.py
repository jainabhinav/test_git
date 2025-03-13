from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_370_variable2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function").alias("_Function"), 
        col("Year").alias("_Year"), 
        col("AIN_Savings").alias("_AIN_Savings"), 
        col("Cumulative/Incremental").alias("_Cumulative/Incremental"), 
        col("EW_Reduction").alias("_EW_Reduction"), 
        col("AIN_Resource_Cost").alias("_AIN_Resource_Cost")
    )
