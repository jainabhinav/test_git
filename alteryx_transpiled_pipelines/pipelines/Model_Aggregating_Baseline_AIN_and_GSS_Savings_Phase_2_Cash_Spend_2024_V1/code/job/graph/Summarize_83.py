from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_83(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("Category"))

    return df1.agg(
        sum(col("Sum_Adjusted_Sum_USDAFX")).alias("Sum_Sum_Adjusted_Sum_USDAFX"), 
        sum(col("Sum_Adjusted_Sum_LCLCFX")).alias("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        sum(col("Sum_Adjusted_Sum_LCL")).alias("Sum_Sum_Adjusted_Sum_LCL"), 
        sum(col("`Non-Opex Adjustments`")).alias("Sum_Non-Opex Adjustments"), 
        sum(col("`Cash Spend`")).alias("Sum_Cash Spend"), 
        sum(col("`GSS BAU Savings`")).alias("Sum_GSS BAU Savings"), 
        sum(col("`GSS Incremental Low Savings`")).alias("Sum_GSS Incremental Low Savings"), 
        sum(col("`GSS Incremental High Savings`")).alias("Sum_GSS Incremental High Savings"), 
        sum(col("AIN_Resource_Cost")).alias("Sum_AIN_Resource_Cost"), 
        sum(col("AIN_Savings")).alias("Sum_AIN_Savings"), 
        sum(col("`AIN EW_Reduction`")).alias("Sum_AIN EW_Reduction")
    )
