from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_293(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("`scenario name`").alias("scenario name"), col("year"), col("Category"), col("evp"))

    return df1.agg(
        sum(col("Sum_Adjusted_Sum_USDAFX")).alias("Sum_Sum_Adjusted_Sum_USDAFX"), 
        sum(col("Sum_Adjusted_Sum_LCLCFX")).alias("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        sum(col("Sum_Adjusted_Sum_LCL")).alias("Sum_Sum_Adjusted_Sum_LCL"), 
        sum(col("`Non-Opex Adjustments`")).alias("Sum_Non-Opex Adjustments"), 
        sum(col("`Cash Spend`")).alias("Sum_Cash Spend")
    )
