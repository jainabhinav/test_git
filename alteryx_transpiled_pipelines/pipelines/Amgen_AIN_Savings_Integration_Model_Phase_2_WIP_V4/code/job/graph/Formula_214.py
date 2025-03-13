from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_214(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Multiplying Factor", (lit(1) / col("`Sum_% Split`")).cast(DoubleType()))\
        .withColumn("Multiplying Factor", when(col("`Multiplying Factor`").isNull().cast(BooleanType()), lit(0))\
        .otherwise(col("`Multiplying Factor`"))\
        .cast(DoubleType()))
