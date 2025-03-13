from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_224(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("Category"), 
        col("`scenario name`").alias("scenario name"), 
        col("year"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("hlmc"), 
        col("`CTS v3`").alias("CTS v3")
    )

    return df1.agg(sum(col("Sum_Sum_Adjusted_Sum_LCLCFX")).alias("Sum_Sum_Sum_Adjusted_Sum_LCLCFX"))
