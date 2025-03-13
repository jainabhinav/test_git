from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_16(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("`Impacted Suppliers - Core`").alias("Core"), 
        col("`Impacted Suppliers - Essential`").alias("Essential"), 
        col("`Impacted Suppliers - Tail`").alias("Tail")
    )
