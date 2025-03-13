from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_54(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`Sum_Sum_Sum_Sum_Spend ($)`").alias("Sum_Sum_Sum_Sum_Spend ($)"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`Sum_Sum_Sum_Spend ($)`").alias("Sum_Sum_Sum_Spend ($)"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("Key"), 
        (col("`Sum_Sum_Sum_Spend ($)`") / col("`Sum_Sum_Sum_Sum_Spend ($)`")).cast(DoubleType()).alias("% Split")
    )
