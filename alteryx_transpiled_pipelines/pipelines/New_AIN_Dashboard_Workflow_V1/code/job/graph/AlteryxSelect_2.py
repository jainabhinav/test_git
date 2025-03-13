from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`A2 Capability Progress`").alias("A2 Capability Progress"), 
        col(
            "`# Mapped Roles - Delayed`"
          )\
          .alias("Delayed"), 
        col("CTS"), 
        col(
            "`# Mapped Roles - On Track`"
          )\
          .alias("On Track"), 
        col(
            "`# Mapped Roles - Complete`"
          )\
          .alias("Complete")
    )
