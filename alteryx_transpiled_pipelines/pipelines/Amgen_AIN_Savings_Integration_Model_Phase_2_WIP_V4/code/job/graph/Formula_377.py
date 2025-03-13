from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_377(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Sum_EW_Reduction"), 
        col("`Right_cost center number`").alias("Right_cost center number"), 
        col("Right_Category"), 
        col("Right_Sum_EW_Reduction"), 
        col("Category"), 
        col("Right_Sum_AIN_Savings"), 
        col("Right_hlmc"), 
        col("year"), 
        col("Sum_AIN_Savings"), 
        col("`cost center number`").alias("cost center number"), 
        col("Right_year"), 
        col("evp"), 
        col("hlmc"), 
        col("Right_evp"), 
        (col("Sum_AIN_Savings") - col("Right_Sum_AIN_Savings")).cast(StringType()).alias("AIN Savings Removed"), 
        (col("Sum_EW_Reduction") - col("Right_Sum_EW_Reduction")).cast(StringType()).alias("EW Savings Removed")
    )
