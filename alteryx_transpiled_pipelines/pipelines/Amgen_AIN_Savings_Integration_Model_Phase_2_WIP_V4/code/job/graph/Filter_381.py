from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_381(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        ~ ((col("AIN_Savings") == lit(0)).cast(BooleanType()) | col("AIN_Savings").isNull().cast(BooleanType())).cast(
          BooleanType()
        )
    )
