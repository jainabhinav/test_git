from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_154_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`Hyp Cat`") == col("in1.category")), "inner")\
        .select(col("in1.`pa account desc`").alias("pa account desc"), col("in0.`Hyp Cat`").alias("Hyp Cat"), col("in1.Sum_LCL").alias("Sum_LCL"), col("in1.`pa account`").alias("pa account"), col("in1.category").alias("category"))
