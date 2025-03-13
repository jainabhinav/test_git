from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_323(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "scenario name",
        when((col("`scenario name`") == lit("ACTUAL 2023")).cast(BooleanType()), lit("HYPERION ACTUAL 2023"))\
          .otherwise(col("`scenario name`"))\
          .cast(StringType())
    )
