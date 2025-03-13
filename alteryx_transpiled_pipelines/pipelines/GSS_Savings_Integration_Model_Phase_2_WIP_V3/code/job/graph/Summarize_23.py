from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_23(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("Category"))

    return df1.agg(
        sum(col("`GSS BAU Savings`")).alias("Sum_GSS BAU Savings"), 
        sum(col("`GSS Incremental Low Savings`")).alias("Sum_GSS Incremental Low Savings"), 
        sum(col("`GSS Incremental High Savings`")).alias("Sum_GSS Incremental High Savings")
    )
