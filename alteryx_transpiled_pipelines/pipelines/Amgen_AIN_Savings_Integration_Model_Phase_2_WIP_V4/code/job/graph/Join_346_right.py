from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_346_right(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            (col("in1.`ADJ 2`") == col("in0.`ADJ 2`"))
            & (col("in1.`tableau display mega category`") == col("in0.`tableau display mega category`"))
          ),
          "leftanti"
        )\
        .select(col("in0.`ADJ 1`").alias("ADJ 1"), col("in0.`ADJ 2`").alias("ADJ 2"), col("in0.`ADJ 3`").alias("ADJ 3"), col("in0.`tableau display mega category`").alias("tableau display mega category"))
