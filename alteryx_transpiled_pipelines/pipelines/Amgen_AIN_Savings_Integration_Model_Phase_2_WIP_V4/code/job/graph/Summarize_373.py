from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_373(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("Category"), 
        col("year"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("hlmc")
    )

    return df1.agg(sum(col("AIN_Savings")).alias("Sum_AIN_Savings"), sum(col("EW_Reduction")).alias("Sum_EW_Reduction"))
