from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_20(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Mega Category`").alias("Mega Category"), 
        col("`Incremental Savings High`").alias("Incremental Savings High"), 
        col("Year").cast(StringType()).alias("Year"), 
        col("`BAU Savings`").alias("BAU Savings"), 
        col("`Incremental Savings Low`").alias("Incremental Savings Low")
    )
