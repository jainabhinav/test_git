from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_104(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`scenario name`").alias("scenario name"), 
        col("year"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("hlmc"), 
        col("`cost center`").alias("cost center")
    )

    return df1.agg(
        sum(col("AIN_Resource_Cost")).alias("Sum_AIN_Resource_Cost"), 
        sum(col("AIN_Savings")).alias("Sum_AIN_Savings"), 
        sum(col("`AIN EW_Reduction`")).alias("Sum_AIN EW_Reduction")
    )
