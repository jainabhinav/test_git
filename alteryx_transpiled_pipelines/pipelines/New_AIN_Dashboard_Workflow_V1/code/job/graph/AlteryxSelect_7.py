from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_7(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Hired Mapped Roles Unfilled`").alias("Unfilled"), 
        col("`Roles Posted as of Date`").alias("Roles Posted as of Date"), 
        col("`Roles Mapped to EW / OrgVue Data`").alias("Roles Mapped to EW / OrgVue Data"), 
        col("Function"), 
        col("Group"), 
        col("`Hired Mapped Roles Filled`").alias("Filled")
    )
