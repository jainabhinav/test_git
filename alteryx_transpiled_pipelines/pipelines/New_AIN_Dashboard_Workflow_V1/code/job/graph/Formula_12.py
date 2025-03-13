from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_12(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col(
            "`# Job Roles`"
          )\
          .alias(
          "# Job Roles"
        ), 
        col("Group"), 
        col("Filled/Unfilled"), 
        col("`Roles Posted as of Date`").alias("Roles Posted as of Date"), 
        lit("Mapped").cast(StringType()).alias("Unmapped/Mapped")
    )
