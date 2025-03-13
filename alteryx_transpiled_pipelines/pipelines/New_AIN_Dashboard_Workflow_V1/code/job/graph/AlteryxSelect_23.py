from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_23(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("`Asset Status and Impact - Complete`").alias("Complete"), 
        col("`Asset Status and Impact - In Progress`").alias("In Progress"), 
        col("`Asset Status and Impact - Not Started`").alias("Not Started")
    )
