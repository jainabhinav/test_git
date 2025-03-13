from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_317(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("Function"), 
        col("Year"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("EVP"), 
        col("Cumulative/Incremental"), 
        col("HLMC")
    )

    return df1.agg(
        sum(col("AIN_Resource_Cost")).alias("Sum_AIN_Resource_Cost"), 
        sum(col("AIN_Savings")).alias("Sum_AIN_Savings"), 
        sum(col("EW_Reduction")).alias("Sum_EW_Reduction")
    )
