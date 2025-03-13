from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_249(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("AIN_Savings"), 
        col("AIN_Resource_Cost"), 
        col("Year"), 
        col("Cumulative/Incremental"), 
        col("EW_Reduction"), 
        lit("Tech").cast(StringType()).alias("Function")
    )
