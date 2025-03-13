from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_57_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("Name"), 
        col("Name1"), 
        col("Value"), 
        col("Name3"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("EVP"), 
        col("Name2"), 
        col("HLMC")
    )
