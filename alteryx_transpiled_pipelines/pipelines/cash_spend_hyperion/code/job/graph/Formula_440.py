from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_440(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Hyp Cat`").alias("Hyp Cat"), 
        col("`pa account`").alias("pa account"), 
        col("`pa account desc`").alias("pa account desc"), 
        col("percent_of_spend"), 
        lit("PA from Actual Data").cast(StringType()).alias("Data Source")
    )
