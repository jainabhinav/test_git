from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_138_left(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`Hyp Cat`") == col("in1.category")), "leftanti")\
        .select(col("in0.`Hyp Cat`").alias("Hyp Cat"), col("in0.`PA Account`").alias("PA Account"), col("in0.`PA Account Description`").alias("PA Account Description"))
