from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_306(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("Group"), 
        col("`Split Divide`").alias("Split Divide"), 
        col("Sum_Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`Right_Split Divide`").alias("Right_Split Divide"), 
        (col("Sum_Sum_Adjusted_Sum_LCLCFX") / col("Sum_Sum_Sum_Adjusted_Sum_LCLCFX"))\
          .cast(DoubleType())\
          .alias("% Split")
    )
