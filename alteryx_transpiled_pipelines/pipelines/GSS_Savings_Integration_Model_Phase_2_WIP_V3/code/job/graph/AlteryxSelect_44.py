from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_44(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Mega Category`").alias("Mega Category"), 
        col("Value").alias("% Allocation - BAU"), 
        col("Name1").alias("EVP"), 
        col("Name2").alias("Year")
    )
