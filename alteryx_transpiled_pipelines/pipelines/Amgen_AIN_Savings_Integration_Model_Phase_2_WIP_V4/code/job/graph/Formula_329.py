from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_329(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Sum_AIN_Savings", (col("Sum_AIN_Savings") / col("Count")).cast(DoubleType()))\
        .withColumn("Sum_EW_Reduction", (col("Sum_EW_Reduction") / col("Count")).cast(DoubleType()))
