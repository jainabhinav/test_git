from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_18(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("`scenario name`").alias("scenario name"))

    return df1.agg(
        sum(col("Sum_Adjusted_Sum_LCLCFX")).alias("Sum_Adjusted_Sum_LCLCFX"), 
        sum(col("`Non-Opex Adjustments`")).alias("Non-Opex Adjustments"), 
        sum(col("`Cash Spend`")).alias("Cash Spend"), 
        sum(col("`GSS BAU Savings`")).alias("GSS BAU Savings"), 
        sum(col("`GSS Incremental Low Savings`")).alias("GSS Incremental Low Savings"), 
        sum(col("`GSS Incremental High Savings`")).alias("GSS Incremental High Savings"), 
        sum(col("AIN_Resource_Cost")).alias("AIN_Resource_Cost"), 
        sum(col("AIN_Savings")).alias("AIN_Savings"), 
        sum(col("`AIN EW_Reduction`")).alias("AIN EW_Reduction")
    )
