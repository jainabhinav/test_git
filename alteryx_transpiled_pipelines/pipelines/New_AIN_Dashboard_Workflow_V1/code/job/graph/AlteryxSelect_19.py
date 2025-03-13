from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_19(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("`Mapped Roles to Negotiate - Complete`").alias("Complete"), 
        col("`Mapped Roles to Negotiate - On Track`").alias("On Track"), 
        col("`Mapped Roles to Negotiate - Delayed`").alias("Delayed")
    )
