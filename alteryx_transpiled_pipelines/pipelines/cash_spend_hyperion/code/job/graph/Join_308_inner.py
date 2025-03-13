from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_308_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`Split Divide`") == col("in1.`Split Divide`")), "inner")\
        .select(col("in0.`Split Divide`").alias("Split Divide"), col("in1.Sum_Sum_Sum_Adjusted_Sum_LCLCFX").alias("Sum_Sum_Sum_Adjusted_Sum_LCLCFX"), col("in1.`Split Divide`").alias("Right_Split Divide"), col("in0.Group").alias("Group"), col("in0.Sum_Sum_Adjusted_Sum_LCLCFX").alias("Sum_Sum_Adjusted_Sum_LCLCFX"))
