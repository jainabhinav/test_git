from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_390(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "Sum_AIN_Savings",
        when((col("Year") == lit("2024")).cast(BooleanType()), (col("Sum_AIN_Savings") * lit(2)))\
          .otherwise(col("Sum_AIN_Savings"))\
          .cast(DoubleType())
    )
