from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_91(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("Category"), 
        col("`scenario name`").alias("scenario name"), 
        col("year"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("evp"), 
        col("`In-Scope Final`").alias("In-Scope Final")
    )

    return df1.agg(
        sum(col("Sum_Adjusted_Sum_LCLCFX")).alias("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        sum(col("`Non-Opex Adjustments`")).alias("Sum_Non-Opex Adjustments"), 
        sum(col("`Cash Spend`")).alias("Sum_Cash Spend"), 
        sum(col("AIN_Resource_Cost")).alias("Sum_AIN_Resource_Cost"), 
        sum(col("AIN_Savings")).alias("Sum_AIN_Savings"), 
        sum(col("`AIN EW_Reduction`")).alias("Sum_AIN EW_Reduction"), 
        sum(col("`GSS BAU Savings`")).alias("Sum_GSS BAU Savings")
    )
