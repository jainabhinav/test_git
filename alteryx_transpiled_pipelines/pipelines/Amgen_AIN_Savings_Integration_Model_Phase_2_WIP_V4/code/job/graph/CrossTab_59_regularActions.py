from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_59_regularActions(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("Function"), 
        col("EVP"), 
        col("HLMC"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("Year"), 
        col("Cumulative/Incremental")
    )
    df2 = df1.pivot("`Saving Type`", ["AIN_Savings", "AIN_Resource_Cost", "EW_Reduction"])

    return df2.agg(sum(col("Savings")).alias("Sum"), lit(1).alias("_dummy_"))
