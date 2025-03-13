from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_379(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("AIN_Savings"), 
        col("`scenario name`").alias("scenario name"), 
        col("site"), 
        col("year"), 
        col("quarter"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("Category"), 
        col("mc"), 
        col("hlmc"), 
        col("`cost center`").alias("cost center"), 
        col("Sum_Adjusted_Sum_LCLCFX")
    )
