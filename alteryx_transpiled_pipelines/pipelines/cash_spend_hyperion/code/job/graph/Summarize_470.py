from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_470(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`scenario name`").alias("scenario name"), 
        col("site"), 
        col("year"), 
        col("quarter"), 
        col("version"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("`pa account`").alias("pa account"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`cost center`").alias("cost center")
    )

    return df1.agg(
        sum(col("Sum_USDAFX")).alias("Sum_USDAFX"), 
        sum(col("Sum_LCLCFX")).alias("Sum_LCLCFX"), 
        sum(col("Sum_LCL")).alias("Sum_LCL")
    )
