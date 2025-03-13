from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_387(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("Function"), 
        col("Category").alias("tableau display mega category"), 
        col("Year"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("EVP"), 
        col("HLMC"), 
        col("`Adjustment Required?`").alias("Adjustment Required?")
    )

    return df1.agg(sum(col("AIN_Savings")).alias("Sum_AIN_Savings"))
