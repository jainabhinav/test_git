from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_239(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("AIN_Savings", (col("`Split Divide`") * col("AIN_Savings")).cast(DoubleType()))\
        .withColumn("AIN_Resource_Cost", (col("`Split Divide`") * col("AIN_Resource_Cost")).cast(DoubleType()))\
        .withColumn("EW_Reduction", (col("`Split Divide`") * col("EW_Reduction")).cast(DoubleType()))
