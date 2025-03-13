from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_140(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("`Hyp Cat`").alias("Hyp Cat"), 
        col("`PA Account`").alias("pa account"), 
        col("Sum_LCL").alias("Avg_Sum_LCL")
    )
