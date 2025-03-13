from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_42_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Savings Type`").alias("Savings Type"), 
        col("Name"), 
        col("`Mega Category`").alias("Mega Category"), 
        col("Name1"), 
        col("Value"), 
        col("Name2")
    )
