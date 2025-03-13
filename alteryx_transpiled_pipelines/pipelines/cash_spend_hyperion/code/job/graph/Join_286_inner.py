from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_286_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`pa account`") == col("in1.`pa account`")), "inner")\
        .select(col("in0.`pa account`").alias("pa account"), col("in1.Sum_Sum_LCLCFX").alias("Sum_Sum_LCLCFX"))
