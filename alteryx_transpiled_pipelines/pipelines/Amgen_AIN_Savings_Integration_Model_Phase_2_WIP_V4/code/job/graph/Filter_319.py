from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_319(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (((col("Year") == lit("2024")).cast(BooleanType()) | (col("Year") == lit("2025")).cast(BooleanType())).cast(BooleanType()) | (col("Year") == lit("2026")).cast(BooleanType())).cast(BooleanType())
          | (col("Year") == lit("2027")).cast(BooleanType())
        )
    )
