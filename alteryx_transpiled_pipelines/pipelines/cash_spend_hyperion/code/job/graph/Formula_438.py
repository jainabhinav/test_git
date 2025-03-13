from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_438(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Hyp Cat`").alias("Hyp Cat"), 
        col("`pa account`").alias("pa account"), 
        col("`PA Account Description`").alias("PA Account Description"), 
        col("percent_of_spend"), 
        lit("PA Mapping Table").cast(StringType()).alias("Data Source")
    )
