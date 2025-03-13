from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_68_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (col("in0.`tableau display mega category`") == col("in1.`tableau display mega category`")),
          "inner"
        )\
        .select(col("in0.`Sum_Sum_Sum_Spend ($)`").alias("Sum_Sum_Sum_Spend ($)"), col("in1.`Sum_Sum_Sum_Spend ($)`").alias("Right_Sum_Sum_Sum_Spend ($)"), col("in0.`tableau display category`").alias("tableau display category"), col("in1.`tableau display mega category`").alias("Right_tableau display mega category"), col("in0.`tableau display mega category`").alias("tableau display mega category"))
