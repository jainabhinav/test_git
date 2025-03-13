from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_9(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("Group"), 
        col("`Roles Posted as of Date`").alias("Roles Posted as of Date"), 
        col("`Roles Unmapped to EW / OrgVue Data`").alias("Unmapped")
    )
