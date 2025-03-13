from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_15_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col(
            "`# Job Roles`"
          )\
          .alias(
          "# Job Roles"
        ), 
        lit(None).cast(StringType()).alias("Filled/Unfilled"), 
        col("Function"), 
        col("Group"), 
        col("`Roles Posted as of Date`").alias("Roles Posted as of Date"), 
        col("Unmapped/Mapped")
    )
