from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_218(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("New % Split", (col("`% Split`") * col("`Multiplying Factor`")).cast(DoubleType()))\
        .withColumn("AIN_Resource_Cost", (col("`New % Split`") * col("AIN_Resource_Cost")).cast(DoubleType()))\
        .withColumn("AIN_Savings", (col("`New % Split`") * col("AIN_Savings")).cast(DoubleType()))\
        .withColumn("EW_Reduction", (col("`New % Split`") * col("EW_Reduction")).cast(DoubleType()))\
        .withColumn("ADJ 3", (col("Function") + col("`tableau display mega category`")).cast(StringType()))
